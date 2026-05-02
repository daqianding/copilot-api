import type { Context } from "hono"

import type { Model } from "~/services/copilot/get-models"

import { awaitApproval } from "~/lib/approval"
import { COMPACT_REQUEST } from "~/lib/compact"
import { getSmallModel, isMessagesApiEnabled } from "~/lib/config"
import { createHandlerLogger, debugJson } from "~/lib/logger"
import { resolveToUpstream } from "~/lib/model-alias"
import { findEndpointModel } from "~/lib/models"
import { checkRateLimit } from "~/lib/rate-limit"
import { state } from "~/lib/state"
import { generateRequestIdFromPayload, getRootSessionId } from "~/lib/utils"
import { createMessages } from "~/services/copilot/create-messages"

import {
  type AnthropicMessagesPayload,
  type AnthropicResponse,
} from "./anthropic-types"
import {
  handleWithChatCompletions,
  handleWithMessagesApi,
  handleWithResponsesApi,
} from "./api-flows"
import {
  getCompactType,
  mergeToolResultForClaude,
  sanitizeIdeTools,
  stripToolReferenceTurnBoundary,
} from "./preprocess"
import { parseSubagentMarkerFromFirstUser } from "./subagent-marker"
import {
  appendToolRoundTrip,
  buildToolResult,
  extractWebSearchCalls,
  hasAnthropicWebSearch,
  injectWebSearchHint,
  logShim,
  MAX_TOOL_LOOP_ITERATIONS,
  performWebSearchViaResponses,
  rewriteWebSearchToolsToFunction,
} from "./web-search-shim"

const logger = createHandlerLogger("messages-handler")

export async function handleCompletion(c: Context) {
  await checkRateLimit(state)

  const anthropicPayload = await c.req.json<AnthropicMessagesPayload>()
  // Resolve client-facing aliases (e.g. CC's 1M-context naming) to the real
  // upstream Copilot model id before any downstream processing.
  anthropicPayload.model = resolveToUpstream(anthropicPayload.model)
  debugJson(logger, "Anthropic request payload:", anthropicPayload)

  sanitizeIdeTools(anthropicPayload)

  const subagentMarker = parseSubagentMarkerFromFirstUser(anthropicPayload)
  if (subagentMarker) {
    debugJson(logger, "Detected Subagent marker:", subagentMarker)
  }

  const sessionId = getRootSessionId(anthropicPayload, c)
  logger.debug("Extracted session ID:", sessionId)

  // claude code and opencode compact / auto-continue detection
  const compactType = getCompactType(anthropicPayload)

  // fix claude code 2.0.28+ warmup request consume premium request, forcing small model if no tools are used
  // set "CLAUDE_CODE_SUBAGENT_MODEL": "you small model" also can avoid this
  const anthropicBeta = c.req.header("anthropic-beta")
  logger.debug("Anthropic Beta header:", anthropicBeta)
  const noTools = !anthropicPayload.tools || anthropicPayload.tools.length === 0
  if (anthropicBeta && noTools && compactType === 0) {
    anthropicPayload.model = getSmallModel()
  }

  if (compactType) {
    logger.debug("Compact request type:", compactType)
  }

  stripToolReferenceTurnBoundary(anthropicPayload)

  // Merge tool_result and text blocks into tool_result to avoid consuming premium requests
  // (caused by skill invocations, edit hooks, plan or to do reminders)
  // e.g. {"role":"user","content":[{"type":"tool_result","content":"Launching skill: xxx"},{"type":"text","text":"xxx"}]}
  // not only for claude, but also for opencode
  // compact requests still run this processing, except for the final compact message itself
  mergeToolResultForClaude(anthropicPayload, {
    skipLastMessage: compactType === COMPACT_REQUEST,
  })

  const requestId = generateRequestIdFromPayload(anthropicPayload, sessionId)
  logger.debug("Generated request ID:", requestId)

  if (state.manualApprove) {
    await awaitApproval()
  }

  const selectedModel = findEndpointModel(anthropicPayload.model)
  anthropicPayload.model = selectedModel?.id ?? anthropicPayload.model

  // Copilot's Messages backend rejects Anthropic server-side tools like
  // `web_search_*`, but its Responses backend natively supports `web_search`.
  // If the request carries any web_search tool, force the Responses path
  // (when the model supports it) so the call actually works end-to-end.
  const wantsWebSearch = hasAnthropicWebSearch(anthropicPayload)
  if (wantsWebSearch && shouldUseResponsesApi(selectedModel)) {
    return await handleWithResponsesApi(c, anthropicPayload, {
      subagentMarker,
      selectedModel,
      requestId,
      sessionId,
      compactType,
      logger,
    })
  }

  // Claude on Copilot only has `/v1/messages` and Copilot blocks
  // Anthropic's server-side web_search there. Run an internal shim:
  // present `web_search` as a normal function tool to Claude, and
  // when Claude calls it, the proxy executes the actual search by
  // delegating to a Responses-API model (gpt-5.x) which supports
  // `{type:"web_search"}`. Loop until Claude finishes.
  if (wantsWebSearch && shouldUseMessagesApi(selectedModel)) {
    return c.json(
      await runWebSearchShimLoop(anthropicPayload, {
        anthropicBetaHeader: anthropicBeta,
        subagentMarker,
        requestId,
        sessionId,
        compactType,
      }),
    )
  }

  if (shouldUseMessagesApi(selectedModel)) {
    return await handleWithMessagesApi(c, anthropicPayload, {
      anthropicBetaHeader: anthropicBeta,
      subagentMarker,
      selectedModel,
      requestId,
      sessionId,
      compactType,
      logger,
    })
  }

  if (shouldUseResponsesApi(selectedModel)) {
    return await handleWithResponsesApi(c, anthropicPayload, {
      subagentMarker,
      selectedModel,
      requestId,
      sessionId,
      compactType,
      logger,
    })
  }

  return await handleWithChatCompletions(c, anthropicPayload, {
    subagentMarker,
    requestId,
    sessionId,
    compactType,
    logger,
  })
}

const hasWebSearchTool = hasAnthropicWebSearch
void hasWebSearchTool

interface ShimLoopOptions {
  anthropicBetaHeader: string | undefined
  subagentMarker: ReturnType<typeof parseSubagentMarkerFromFirstUser>
  requestId: string
  sessionId?: string
  compactType: ReturnType<typeof getCompactType>
}

/**
 * Drives the Claude <-> proxy <-> (gpt-5 web_search) loop.
 *
 * Pre: payload uses a Messages-only model (e.g. Claude on Copilot) and
 * carries an Anthropic server-side web_search tool.
 *
 * The function rewrites the tool to a function tool, calls the Messages
 * API, and whenever the response asks to call `web_search`, the proxy
 * executes the search via Copilot's Responses API and feeds the result
 * back. Loops up to MAX_TOOL_LOOP_ITERATIONS times.
 */
async function runWebSearchShimLoop(
  payload: AnthropicMessagesPayload,
  opts: ShimLoopOptions,
): Promise<AnthropicResponse> {
  rewriteWebSearchToolsToFunction(payload)
  injectWebSearchHint(payload)

  // We are running an internal multi-turn loop. The downstream call to
  // Claude must be non-streaming so we can inspect tool_use blocks.
  payload.stream = false

  // Pick a Responses-capable model from the live catalog to perform the
  // actual web searches.
  const searchModel = pickSearchModel()
  logShim("Entering shim loop", {
    primaryModel: payload.model,
    searchModel,
  })

  let iterations = 0
  let lastResponse: AnthropicResponse | undefined

  while (iterations < MAX_TOOL_LOOP_ITERATIONS) {
    iterations += 1
    const response = (await createMessages(payload, opts.anthropicBetaHeader, {
      subagentMarker: opts.subagentMarker,
      requestId: `${opts.requestId}-shim-${iterations}`,
      sessionId: opts.sessionId,
      compactType: opts.compactType,
    })) as AnthropicResponse
    lastResponse = response

    const calls = extractWebSearchCalls(response)
    if (calls.length === 0) {
      logShim(`Loop done after ${iterations} iteration(s)`)
      break
    }

    logShim(`Iteration ${iterations}: ${calls.length} web_search call(s)`)

    // Execute every web_search call from this assistant turn (in parallel)
    // and produce a tool_result for each.
    const results = await Promise.all(
      calls.map(async (call) => {
        const query =
          typeof call.input.query === "string" ?
            call.input.query
          : JSON.stringify(call.input)
        try {
          const text = await performWebSearchViaResponses(query, {
            searchModel,
            requestId: opts.requestId,
            sessionId: opts.sessionId,
            subagentMarker: opts.subagentMarker,
            compactType: opts.compactType,
          })
          return buildToolResult(call.id, text, false)
        } catch (err) {
          const msg =
            err instanceof Error ? err.message : "unknown web_search error"
          logShim(`web_search failed for query "${query}": ${msg}`)
          return buildToolResult(call.id, `Web search failed: ${msg}`, true)
        }
      }),
    )

    appendToolRoundTrip(payload, response.content, results)
  }

  if (!lastResponse) {
    throw new Error("Web search shim produced no response")
  }
  return lastResponse
}

const pickSearchModel = (): string => {
  const models = state.models?.data ?? []
  // Prefer a small / cheap Responses-capable model.
  const preferred = ["gpt-5-mini", "gpt-5.4-mini", "gpt-5.4", "gpt-5.3-codex"]
  for (const id of preferred) {
    const m = models.find((x) => x.id === id)
    if (m?.supported_endpoints?.includes("/responses")) return id
  }
  // Fall back to the first model that supports /responses.
  const any = models.find((m) => m.supported_endpoints?.includes("/responses"))
  if (any) return any.id
  // Last resort.
  return "gpt-5-mini"
}

const RESPONSES_ENDPOINT = "/responses"
const MESSAGES_ENDPOINT = "/v1/messages"

const shouldUseResponsesApi = (selectedModel: Model | undefined): boolean => {
  return (
    selectedModel?.supported_endpoints?.includes(RESPONSES_ENDPOINT) ?? false
  )
}

const shouldUseMessagesApi = (selectedModel: Model | undefined): boolean => {
  const useMessagesApi = isMessagesApiEnabled()
  if (!useMessagesApi) {
    return false
  }
  return (
    selectedModel?.supported_endpoints?.includes(MESSAGES_ENDPOINT) ?? false
  )
}
