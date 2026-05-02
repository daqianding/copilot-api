// web-search-shim.ts
//
// Anthropic exposes a server-side `web_search_20250305` tool that is supposed
// to be executed by Anthropic's backend. Copilot's Anthropic transport refuses
// that tool type. To still let Claude (running on Copilot) use web search,
// this module rewrites the request so Claude sees `web_search` as a regular
// function tool, then — when Claude actually calls it — the proxy itself
// performs the search by delegating to a Copilot Responses-API model
// (e.g. gpt-5.4) which natively supports `{type:"web_search"}`.
//
// The whole loop happens inside the proxy. The Claude Code client only sees
// a Claude response that used web_search, exactly like Anthropic's
// server-side experience.

import consola from "consola"

import type { CompactType } from "~/lib/compact"
import type { SubagentMarker } from "~/lib/subagent"

import { state } from "~/lib/state"
import {
  createResponses,
  type ResponsesPayload,
} from "~/services/copilot/create-responses"

import {
  type AnthropicAssistantContentBlock,
  type AnthropicAssistantMessage,
  type AnthropicMessagesPayload,
  type AnthropicResponse,
  type AnthropicTool,
  type AnthropicToolResultBlock,
  type AnthropicToolUseBlock,
  type AnthropicUserContentBlock,
} from "./anthropic-types"

const WEB_SEARCH_TOOL_NAME = "web_search"
const WEB_SEARCH_DESCRIPTION =
  "Search the web for up-to-date information. "
  + "Provide a focused query string. "
  + "Returns a textual summary of search results with source URLs."
const WEB_SEARCH_INPUT_SCHEMA: Record<string, unknown> = {
  type: "object",
  properties: {
    query: {
      type: "string",
      description: "The search query.",
    },
  },
  required: ["query"],
}
const MAX_TOOL_LOOP_ITERATIONS = 5

export interface WebSearchShimContext {
  searchModel: string
  requestId: string
  sessionId?: string
  subagentMarker?: SubagentMarker | null
  compactType?: CompactType
}

/**
 * Returns true if any tool in the payload is an Anthropic server-side
 * web_search tool (e.g. `web_search_20250305`).
 */
export const hasAnthropicWebSearch = (
  payload: AnthropicMessagesPayload,
): boolean => {
  if (!payload.tools || payload.tools.length === 0) return false
  return payload.tools.some((t) => isAnthropicWebSearchTool(t))
}

const isAnthropicWebSearchTool = (tool: AnthropicTool): boolean => {
  const t = tool.type ?? ""
  return t.startsWith("web_search")
}

/**
 * Rewrites Anthropic server-side web_search tool entries into a normal
 * function tool that Copilot's Anthropic transport accepts. Mutates payload.
 */
export const rewriteWebSearchToolsToFunction = (
  payload: AnthropicMessagesPayload,
): void => {
  if (!payload.tools) return
  payload.tools = payload.tools.map((tool) => {
    if (!isAnthropicWebSearchTool(tool)) return tool
    return {
      name: WEB_SEARCH_TOOL_NAME,
      description: WEB_SEARCH_DESCRIPTION,
      input_schema: WEB_SEARCH_INPUT_SCHEMA,
    }
  })
}

/**
 * Inspects an assistant response for tool_use blocks targeting our
 * shimmed web_search tool.
 */
export const extractWebSearchCalls = (
  response: AnthropicResponse,
): Array<AnthropicToolUseBlock> => {
  const calls: Array<AnthropicToolUseBlock> = []
  for (const block of response.content) {
    if (block.type === "tool_use" && block.name === WEB_SEARCH_TOOL_NAME) {
      calls.push(block)
    }
  }
  return calls
}

/**
 * Run a single web search by delegating to a Copilot Responses-API model
 * with the native `{type: "web_search"}` server tool. Returns a plain-text
 * summary suitable for handing back to Claude as a tool_result.
 */
export const performWebSearchViaResponses = async (
  query: string,
  ctx: WebSearchShimContext,
): Promise<string> => {
  const searchModel = ctx.searchModel
  if (!state.copilotToken) throw new Error("Copilot token not found")

  const instructions =
    "You are a web search assistant. Use the web_search tool to find "
    + "current information for the user's query. Then write a concise "
    + "answer (a few short paragraphs) that synthesises the results, "
    + "and include the most relevant source URLs inline."

  const responsesPayload: ResponsesPayload = {
    model: searchModel,
    input: [
      {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: query }],
      },
    ] as unknown as ResponsesPayload["input"],
    instructions,
    tools: [{ type: "web_search" }],
    tool_choice: "auto",
    temperature: 1,
    max_output_tokens: 4096,
    stream: false,
    store: false,
    parallel_tool_calls: true,
    reasoning: {
      effort: "low",
      summary: "auto",
    } as ResponsesPayload["reasoning"],
  }

  const result = await createResponses(responsesPayload, {
    vision: false,
    initiator: "agent",
    requestId: `${ctx.requestId}-websearch`,
    sessionId: ctx.sessionId,
  })

  // Non-streaming: createResponses returns a ResponsesResult.
  // Extract the final message text from `output`.
  const summary = extractResponsesText(result)
  if (summary && summary.trim().length > 0) {
    return summary
  }
  return `(Web search returned no synthesizable text for query: ${query})`
}

interface ResponsesContentItem {
  type?: string
  text?: string
}
interface ResponsesOutputItem {
  type?: string
  role?: string
  content?: Array<ResponsesContentItem>
}
interface ResponsesShape {
  output_text?: string
  output?: Array<ResponsesOutputItem>
}

const extractResponsesText = (result: unknown): string => {
  if (!result || typeof result !== "object") return ""
  const r = result as ResponsesShape
  if (typeof r.output_text === "string" && r.output_text.length > 0) {
    return r.output_text
  }
  if (!Array.isArray(r.output)) return ""
  return r.output
    .filter((item) => isAssistantMessageItem(item))
    .flatMap((item) => getMessageTextParts(item))
    .join("\n")
    .trim()
}

const isAssistantMessageItem = (item: ResponsesOutputItem): boolean => {
  if (item.type !== "message") return false
  if (item.role !== undefined && item.role !== "assistant") return false
  return Array.isArray(item.content)
}

const getMessageTextParts = (item: ResponsesOutputItem): Array<string> => {
  if (!Array.isArray(item.content)) return []
  const out: Array<string> = []
  for (const c of item.content) {
    const isText = c.type === "output_text" || c.type === "text"
    if (isText && typeof c.text === "string") out.push(c.text)
  }
  return out
}

/**
 * Append the assistant turn (containing tool_use blocks) and the
 * corresponding tool_result blocks to the conversation, so the next
 * createMessages call sees them as prior turns.
 */
export const appendToolRoundTrip = (
  payload: AnthropicMessagesPayload,
  assistantContent: Array<AnthropicAssistantContentBlock>,
  toolResults: Array<AnthropicToolResultBlock>,
): void => {
  const assistantMsg: AnthropicAssistantMessage = {
    role: "assistant",
    content: assistantContent,
  }
  const userContent: Array<AnthropicUserContentBlock> = toolResults
  payload.messages.push(assistantMsg, { role: "user", content: userContent })
}

export const buildToolResult = (
  toolUseId: string,
  text: string,
  isError = false,
): AnthropicToolResultBlock => ({
  type: "tool_result",
  tool_use_id: toolUseId,
  content: [{ type: "text", text }],
  is_error: isError,
})

export { MAX_TOOL_LOOP_ITERATIONS, WEB_SEARCH_TOOL_NAME }

// Hint to Claude (added once at the top of `system`) that web_search exists
// and how to call it. This nudges the model to actually invoke the tool
// instead of refusing or guessing.
export const WEB_SEARCH_SYSTEM_HINT =
  "You have access to a `web_search` tool. "
  + "When the user asks about current events, recent data, or anything "
  + "that may have changed, call `web_search` with a focused query. "
  + "You may call it multiple times. After you have enough information, "
  + "answer the user with citations to the source URLs returned."

export const injectWebSearchHint = (
  payload: AnthropicMessagesPayload,
): void => {
  const hint = { type: "text" as const, text: WEB_SEARCH_SYSTEM_HINT }
  if (!payload.system) {
    payload.system = [hint]
    return
  }
  if (typeof payload.system === "string") {
    payload.system = [{ type: "text", text: payload.system }, hint]
    return
  }
  payload.system = [hint, ...payload.system]
}

// Logging helper so we can see the shim activity in the proxy console.
export const logShim = (msg: string, extra?: Record<string, unknown>): void => {
  if (extra) consola.info(`[web-search-shim] ${msg}`, extra)
  else consola.info(`[web-search-shim] ${msg}`)
}
