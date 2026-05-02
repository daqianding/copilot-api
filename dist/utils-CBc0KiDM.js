import consola from "consola";
import { readFile } from "node:fs/promises";
import { networkInterfaces } from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { exec } from "node:child_process";
import { AsyncLocalStorage } from "node:async_hooks";

//#region src/lib/state.ts
const state = {
	accountType: "individual",
	manualApprove: false,
	rateLimitWait: false,
	showToken: false,
	verbose: false,
	vsCodeDeviceId: randomUUID()
};

//#endregion
//#region src/lib/compact.ts
const COMPACT_REQUEST = 1;
const COMPACT_AUTO_CONTINUE = 2;
const compactSystemPromptStart = "You are a helpful AI assistant tasked with summarizing conversations";
const compactOpenCodeSystemPromptStart = "You are an anchored context summarization assistant for coding sessions.";
const compactSystemPromptStarts = [compactSystemPromptStart, compactOpenCodeSystemPromptStart];
const compactTextOnlyGuard = "CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.";
const compactSummaryPromptStart = "Your task is to create a detailed summary of the conversation so far";
const compactAutoContinueClaudeCodePromptStart = "This session is being continued from a previous conversation that ran out of context. The summary below covers the earlier portion of the conversation.";
const compactAutoContinueOpenCodePromptStart = "Continue if you have next steps, or stop and ask for clarification if you are unsure how to proceed.";
const compactAutoContinueOpenCodePromptStart2 = "The previous request exceeded the provider's size limit due to large media attachments. The conversation was compacted and media files were removed from context.";
const compactAutoContinuePromptStarts = [
	compactAutoContinueClaudeCodePromptStart,
	compactAutoContinueOpenCodePromptStart,
	compactAutoContinueOpenCodePromptStart2
];
const compactMessageSections = ["Pending Tasks:", "Current Work:"];

//#endregion
//#region src/lib/opencode.ts
const execAsync = (command) => {
	return new Promise((resolve, reject) => {
		exec(command, (error, stdout) => {
			if (error) {
				reject(error);
				return;
			}
			resolve(stdout);
		});
	});
};
let opencodeVersionCache;
const getGlobalNpmRoot = async () => {
	return (await execAsync("npm root -g")).trim();
};
async function resolveOpencodeVersion() {
	try {
		const npmRootPath = await getGlobalNpmRoot();
		const packageJson = await readFile(path.join(npmRootPath, "opencode-ai", "package.json"), "utf8");
		const { version } = JSON.parse(packageJson);
		opencodeVersionCache = version;
	} catch (error) {
		consola.warn(`Failed to resolve opencode version`, error);
	}
}
const initOpencodeVersion = () => {
	if (process.env.COPILOT_API_OAUTH_APP?.trim() !== "opencode") return Promise.resolve();
	return resolveOpencodeVersion();
};
const getCachedOpencodeVersion = () => {
	return opencodeVersionCache;
};

//#endregion
//#region src/lib/request-context.ts
const TRACE_ID_MAX_LENGTH = 64;
const TRACE_ID_PATTERN = /^\w[\w.-]*$/;
const asyncLocalStorage = new AsyncLocalStorage();
const requestContext = {
	getStore: () => asyncLocalStorage.getStore(),
	run: (context, callback) => asyncLocalStorage.run(context, callback)
};
function generateTraceId() {
	return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}
function resolveTraceId(traceId) {
	const candidate = traceId?.trim();
	if (!candidate || candidate.length > TRACE_ID_MAX_LENGTH || !TRACE_ID_PATTERN.test(candidate)) return generateTraceId();
	return candidate;
}

//#endregion
//#region src/lib/api-config.ts
const isOpencodeOauthApp = () => {
	return process.env.COPILOT_API_OAUTH_APP?.trim() === "opencode";
};
const normalizeDomain = (input) => {
	return input.trim().replace(/^https?:\/\//u, "").replace(/\/+$/u, "");
};
const getEnterpriseDomain = () => {
	const raw = (process.env.COPILOT_API_ENTERPRISE_URL ?? "").trim();
	if (!raw) return null;
	return normalizeDomain(raw) || null;
};
const getGitHubBaseUrl = () => {
	const resolvedDomain = getEnterpriseDomain();
	return resolvedDomain ? `https://${resolvedDomain}` : GITHUB_BASE_URL;
};
const getGitHubApiBaseUrl = () => {
	const resolvedDomain = getEnterpriseDomain();
	return resolvedDomain ? `https://api.${resolvedDomain}` : GITHUB_API_BASE_URL;
};
const getOpencodeOauthHeaders = () => {
	return {
		Accept: "application/json",
		"Content-Type": "application/json",
		"User-Agent": getOpencodeVersion()
	};
};
const getOpencodeLLMHeaders = () => {
	return {
		Accept: "application/json",
		"Content-Type": "application/json",
		"User-Agent": OPENCODE_LLM_USER_AGENT
	};
};
const normalizeOpencodeUserAgent = (userAgent) => {
	const candidate = userAgent.trim();
	const opencodeProduct = candidate.match(/^opencode\/[^\s,]+/u)?.[0];
	if (!opencodeProduct || candidate.includes(`, ${opencodeProduct}`)) return candidate;
	return `${candidate}, ${opencodeProduct}`;
};
const getOauthUrls = () => {
	const githubBaseUrl = getGitHubBaseUrl();
	return {
		deviceCodeUrl: `${githubBaseUrl}/login/device/code`,
		accessTokenUrl: `${githubBaseUrl}/login/oauth/access_token`
	};
};
const getOauthAppConfig = () => {
	if (isOpencodeOauthApp()) return {
		clientId: OPENCODE_GITHUB_CLIENT_ID,
		headers: getOpencodeOauthHeaders(),
		scope: GITHUB_APP_SCOPES
	};
	return {
		clientId: GITHUB_CLIENT_ID,
		headers: standardHeaders(),
		scope: GITHUB_APP_SCOPES
	};
};
const prepareForCompact = (headers, compactType) => {
	if (compactType) {
		headers["x-initiator"] = "agent";
		if (!isOpencodeOauthApp() && compactType === COMPACT_REQUEST) {
			headers["x-interaction-type"] = "conversation-other";
			headers["openai-intent"] = "conversation-other";
		}
	}
};
const prepareInteractionHeaders = (sessionId, isSubagent, headers) => {
	const sendInteractionHeaders = !isOpencodeOauthApp();
	if (isSubagent) {
		headers["x-initiator"] = "agent";
		if (sendInteractionHeaders) headers["x-interaction-type"] = "conversation-subagent";
	}
	if (sessionId && sendInteractionHeaders) headers["x-interaction-id"] = sessionId;
};
const standardHeaders = () => ({
	"content-type": "application/json",
	accept: "application/json"
});
const getOpencodeVersion = () => {
	const version = getCachedOpencodeVersion();
	if (version) return "opencode/" + version;
	return OPENCODE_VERSION;
};
const OPENCODE_VERSION = "opencode/1.14.29";
const OPENCODE_LLM_USER_AGENT = "opencode/1.14.29 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.13, opencode/1.14.29";
const COPILOT_VERSION = "0.46.0";
const EDITOR_PLUGIN_VERSION = `copilot-chat/${COPILOT_VERSION}`;
const USER_AGENT = `GitHubCopilotChat/${COPILOT_VERSION}`;
const CLAUDE_AGENT_USER_AGENT = "vscode_claude_code/2.1.112 (external, sdk-ts, agent-sdk/0.2.112)";
const API_VERSION = "2025-10-01";
const copilotBaseUrl = (state$1) => {
	const enterpriseDomain = getEnterpriseDomain();
	if (enterpriseDomain) return `https://copilot-api.${enterpriseDomain}`;
	if (isOpencodeOauthApp()) return "https://api.githubcopilot.com";
	if (state$1.copilotApiUrl) return state$1.copilotApiUrl;
	return state$1.accountType === "individual" ? "https://api.githubcopilot.com" : `https://api.${state$1.accountType}.githubcopilot.com`;
};
const prepareMessageProxyHeaders = (headers) => {
	if (isOpencodeOauthApp()) return;
	const requestIdValue = randomUUID();
	headers["x-agent-task-id"] = requestIdValue;
	headers["x-request-id"] = requestIdValue;
	headers["x-interaction-type"] = "messages-proxy";
	headers["openai-intent"] = "messages-proxy";
	headers["user-agent"] = CLAUDE_AGENT_USER_AGENT;
	delete headers["copilot-integration-id"];
};
const githubUserHeaders = (state$1) => {
	if (isOpencodeOauthApp()) return {
		Authorization: `Bearer ${state$1.githubToken}`,
		"User-Agent": getOpencodeVersion()
	};
	return {
		accept: "application/vnd.github+json",
		authorization: `token ${state$1.githubToken}`,
		"user-agent": USER_AGENT,
		"x-github-api-version": "2022-11-28",
		"x-vscode-user-agent-library-version": "electron-fetch"
	};
};
const copilotModelsHeaders = (state$1) => {
	if (isOpencodeOauthApp()) return {
		Authorization: `Bearer ${state$1.copilotToken}`,
		"User-Agent": getOpencodeVersion()
	};
	const headers = githubCopilotHeaders(state$1);
	headers["x-interaction-type"] = "model-access";
	headers["openai-intent"] = "model-access";
	delete headers["x-interaction-id"];
	delete headers["content-type"];
	return headers;
};
const copilotHeaders = (state$1, requestId, vision = false) => {
	if (isOpencodeOauthApp()) {
		const headers = {
			Authorization: `Bearer ${state$1.copilotToken}`,
			...getOpencodeLLMHeaders(),
			"Openai-Intent": "conversation-edits"
		};
		const store = requestContext.getStore();
		const userAgent = store?.userAgent.trim();
		if (userAgent?.startsWith("opencode/")) headers["User-Agent"] = normalizeOpencodeUserAgent(userAgent);
		if (store?.sessionAffinity) headers["x-session-affinity"] = store.sessionAffinity;
		if (store?.parentSessionId) headers["x-parent-session-id"] = store.parentSessionId;
		if (vision) headers["Copilot-Vision-Request"] = "true";
		return headers;
	}
	return githubCopilotHeaders(state$1, requestId, vision);
};
const githubCopilotHeaders = (state$1, requestId, vision = false) => {
	const requestIdValue = requestId ?? randomUUID();
	const headers = {
		Authorization: `Bearer ${state$1.copilotToken}`,
		"content-type": standardHeaders()["content-type"],
		"copilot-integration-id": "vscode-chat",
		"editor-device-id": state$1.vsCodeDeviceId,
		"editor-version": `vscode/${state$1.vsCodeVersion}`,
		"editor-plugin-version": EDITOR_PLUGIN_VERSION,
		"user-agent": USER_AGENT,
		"openai-intent": "conversation-agent",
		"x-github-api-version": API_VERSION,
		"x-request-id": requestIdValue,
		"x-vscode-user-agent-library-version": "electron-fetch",
		"x-agent-task-id": requestIdValue,
		"x-interaction-type": "conversation-agent"
	};
	if (vision) headers["copilot-vision-request"] = "true";
	if (state$1.macMachineId) headers["vscode-machineid"] = state$1.macMachineId;
	if (state$1.vsCodeSessionId) headers["vscode-sessionid"] = state$1.vsCodeSessionId;
	return headers;
};
const GITHUB_API_BASE_URL = "https://api.github.com";
const githubHeaders = (state$1) => {
	if (isOpencodeOauthApp()) return {
		Authorization: `Bearer ${state$1.githubToken}`,
		...getOpencodeOauthHeaders()
	};
	return {
		authorization: `token ${state$1.githubToken}`,
		"user-agent": USER_AGENT,
		"x-github-api-version": "2025-04-01",
		"x-vscode-user-agent-library-version": "electron-fetch"
	};
};
const GITHUB_BASE_URL = "https://github.com";
const GITHUB_CLIENT_ID = "Iv1.b507a08c87ecfe98";
const GITHUB_APP_SCOPES = ["read:user"].join(" ");
const OPENCODE_GITHUB_CLIENT_ID = "Ov23li8tweQw6odWQebz";

//#endregion
//#region src/lib/error.ts
var HTTPError = class extends Error {
	response;
	constructor(message, response) {
		super(message);
		this.response = response;
	}
};
async function forwardError(c, error) {
	consola.error("Error occurred:", error);
	if (error instanceof HTTPError) {
		if (error.response.status === 429) for (const [name, value] of error.response.headers) {
			const lowerName = name.toLowerCase();
			if (lowerName === "retry-after" || lowerName.startsWith("x-")) c.header(name, value);
		}
		const errorText = await error.response.text();
		let errorJson;
		try {
			errorJson = JSON.parse(errorText);
		} catch {
			errorJson = errorText;
		}
		consola.error("HTTP error:", errorJson);
		return c.json({ error: {
			message: errorText,
			type: "error"
		} }, error.response.status);
	}
	return c.json({ error: {
		message: error.message,
		type: "error"
	} }, 500);
}

//#endregion
//#region src/services/github/get-copilot-usage.ts
const getCopilotUsage = async (githubToken) => {
	const resolvedGithubToken = githubToken ?? state.githubToken;
	if (!resolvedGithubToken) throw new Error("GitHub token not found");
	const authState = {
		...state,
		githubToken: resolvedGithubToken
	};
	const response = await fetch(`${getGitHubApiBaseUrl()}/copilot_internal/user`, { headers: githubHeaders(authState) });
	if (!response.ok) throw new HTTPError("Failed to get Copilot usage", response);
	return await response.json();
};

//#endregion
//#region src/services/copilot/get-models.ts
const getModels = async () => {
	consola.info(`Fetching models from ${copilotBaseUrl(state)}/models`);
	const response = await fetch(`${copilotBaseUrl(state)}/models`, { headers: copilotModelsHeaders(state) });
	if (!response.ok) {
		const errorText = await response.clone().text();
		consola.error("Failed to get models response body", errorText);
		throw new HTTPError("Failed to get models", response);
	}
	return await response.json();
};

//#endregion
//#region src/services/get-vscode-version.ts
const FALLBACK = "1.118.0";
async function getVSCodeVersion() {
	await Promise.resolve();
	return FALLBACK;
}

//#endregion
//#region src/lib/deviceid.ts
const WINDOWS_DEVICE_ID_KEY = String.raw`\SOFTWARE\Microsoft\DeveloperTools`;
const WINDOWS_DEVICE_ID_NAME = "deviceid";
const windows64Architectures = new Set([
	"AMD64",
	"ARM64",
	"IA64"
]);
const getPosixHomeDir = () => {
	if (!process.env.HOME) throw new Error("Home directory not found");
	return process.env.HOME;
};
const getDeviceIdFilePath = () => {
	let folder;
	switch (process.platform) {
		case "darwin":
			folder = path.posix.join(getPosixHomeDir(), "Library", "Application Support");
			break;
		case "linux":
			folder = process.env.XDG_CACHE_HOME ?? path.posix.join(getPosixHomeDir(), ".cache");
			break;
		default: throw new Error("Unsupported platform");
	}
	return path.posix.join(folder, "Microsoft", "DeveloperTools", "deviceid");
};
const isMissingFileError = (error) => {
	return error instanceof Error && "code" in error && error.code === "ENOENT";
};
const readStoredDeviceIdFile = async (filePath) => {
	const { readFile: readFile$1 } = await import("node:fs/promises");
	try {
		return await readFile$1(filePath, "utf8");
	} catch (error) {
		if (isMissingFileError(error)) return;
		throw error;
	}
};
const writeStoredDeviceIdFile = async (filePath, deviceId) => {
	const { mkdir, writeFile } = await import("node:fs/promises");
	await mkdir(path.posix.dirname(filePath), { recursive: true });
	await writeFile(filePath, deviceId, "utf8");
};
const getWindowsRegistryArch = () => {
	const architecture = (process.env.PROCESSOR_ARCHITEW6432 ?? process.env.PROCESSOR_ARCHITECTURE)?.toUpperCase();
	return architecture && windows64Architectures.has(architecture) ? "x64" : void 0;
};
const loadWinreg = async () => {
	const module = await import("winreg");
	return "default" in module ? module.default : module;
};
const isMissingRegistryError = (error) => {
	if (!error) return false;
	const errorCode = Number(error.code);
	return Number.isFinite(errorCode) && errorCode === 1;
};
const createWindowsRegistry = async () => {
	const Winreg = await loadWinreg();
	return {
		registry: new Winreg({
			hive: Winreg.HKCU,
			key: WINDOWS_DEVICE_ID_KEY,
			arch: getWindowsRegistryArch()
		}),
		regSz: Winreg.REG_SZ
	};
};
const readRegistryString = async (registry, name) => {
	return new Promise((resolve, reject) => {
		registry.get(name, (error, item) => {
			if (isMissingRegistryError(error)) {
				resolve(void 0);
				return;
			}
			if (error) {
				reject(error instanceof Error ? error : /* @__PURE__ */ new Error("Unknown registry error"));
				return;
			}
			resolve(item?.value);
		});
	});
};
const writeRegistryString = async ({ registry, regSz, name, value }) => {
	return new Promise((resolve, reject) => {
		registry.set(name, regSz, value, (error) => {
			if (error) {
				reject(error instanceof Error ? error : /* @__PURE__ */ new Error("Unknown registry error"));
				return;
			}
			resolve();
		});
	});
};
const getStoredVSCodeDeviceId = async () => {
	switch (process.platform) {
		case "win32": {
			const { registry } = await createWindowsRegistry();
			return readRegistryString(registry, WINDOWS_DEVICE_ID_NAME);
		}
		case "darwin":
		case "linux": return readStoredDeviceIdFile(getDeviceIdFilePath());
		default: throw new Error("Unsupported platform");
	}
};
const setStoredVSCodeDeviceId = async (deviceId) => {
	switch (process.platform) {
		case "win32": {
			const { registry, regSz } = await createWindowsRegistry();
			await writeRegistryString({
				registry,
				regSz,
				name: WINDOWS_DEVICE_ID_NAME,
				value: deviceId
			});
			return;
		}
		case "darwin":
		case "linux":
			await writeStoredDeviceIdFile(getDeviceIdFilePath(), deviceId);
			return;
		default: throw new Error("Unsupported platform");
	}
};
const createVSCodeDeviceId = () => randomUUID().toLowerCase();
async function getVSCodeDeviceId() {
	let deviceId;
	try {
		deviceId = await getStoredVSCodeDeviceId();
	} catch (error) {
		consola.debug("Failed to read VSCode device id", error);
	}
	if (deviceId) return deviceId;
	const newDeviceId = createVSCodeDeviceId();
	try {
		await setStoredVSCodeDeviceId(newDeviceId);
	} catch (error) {
		consola.warn("Failed to persist VSCode device id, using ephemeral id", error);
	}
	return newDeviceId;
}

//#endregion
//#region src/lib/model-alias.ts
/**
* Model ID aliases.
*
* Claude Code (verified against the v2.1.80 bundled binary) detects the
* 1M-context code path with this regex:
*
*     function Mk(id) { return /\[1m\]/i.test(id) }
*
* i.e. the model ID must contain the literal substring `[1m]` (any case,
* any position). Model IDs that look 1M-ish but lack the `[1m]` marker
* (e.g. `claude-opus-4.7-1m-internal`) silently fall back to the default
* 200k window.
*
* To work around this we expose an *alias* ID to the client (Claude Code)
* that contains `[1m]`, and translate it back to the real upstream ID
* before forwarding requests to GitHub Copilot. The alias is surfaced in:
*   1. `/v1/models` (so the picker / IDE can see it)
*   2. The `--claude-code` env-script generator (so the env vars use it)
* And translated back in:
*   3. `findEndpointModel` (the central resolver used by every route)
*   4. Each handler entry point (defensive cover for direct lookups)
*
* Add new entries to {@link UPSTREAM_TO_ALIAS} as needed.
*/
/** Map of real upstream Copilot model ID -> client-facing alias ID. */
const UPSTREAM_TO_ALIAS = { "claude-opus-4.7-1m-internal": "claude-opus-4.7-internal[1m]" };
/** Reverse: client-facing alias ID -> real upstream Copilot model ID. */
const ALIAS_TO_UPSTREAM = Object.fromEntries(Object.entries(UPSTREAM_TO_ALIAS).map(([upstream, alias]) => [alias, upstream]));
/** Returns the alias for a given upstream id, or undefined if none. */
const getAliasForUpstream = (upstreamId) => UPSTREAM_TO_ALIAS[upstreamId];
/** Returns the real upstream id for a given alias, or undefined if not an alias. */
const getUpstreamForAlias = (aliasId) => ALIAS_TO_UPSTREAM[aliasId];
/** Convenience: replace alias -> upstream if applicable, else identity. */
const resolveToUpstream = (id) => ALIAS_TO_UPSTREAM[id] ?? id;
/** Convenience: replace upstream -> alias if applicable, else identity. */
const exposeAlias = (id) => UPSTREAM_TO_ALIAS[id] ?? id;

//#endregion
//#region src/lib/utils.ts
const sleep = (ms) => new Promise((resolve) => {
	setTimeout(resolve, ms);
});
const isNullish = (value) => value === null || value === void 0;
async function cacheModels() {
	const models = await getModels();
	const filtered = models.data.filter((model) => model.model_picker_enabled || model.capabilities.type === "embeddings");
	const aliasEntries = filtered.flatMap((model) => {
		const alias = getAliasForUpstream(model.id);
		if (!alias) return [];
		return [{
			...model,
			id: alias
		}];
	});
	state.models = {
		...models,
		data: [...filtered, ...aliasEntries]
	};
}
const cacheVSCodeVersion = async () => {
	const response = await getVSCodeVersion();
	state.vsCodeVersion = response;
	consola.info(`Using VSCode version: ${response}`);
};
const invalidMacAddresses = new Set([
	"00:00:00:00:00:00",
	"ff:ff:ff:ff:ff:ff",
	"ac:de:48:00:11:22"
]);
function validateMacAddress(candidate) {
	const tempCandidate = candidate.replaceAll("-", ":").toLowerCase();
	return !invalidMacAddresses.has(tempCandidate);
}
function getMac() {
	const ifaces = networkInterfaces();
	for (const name in ifaces) {
		const networkInterface = ifaces[name];
		if (networkInterface) {
			for (const { mac } of networkInterface) if (validateMacAddress(mac)) return mac;
		}
	}
	return null;
}
const cacheMacMachineId = () => {
	const macAddress = getMac() ?? randomUUID();
	state.macMachineId = createHash("sha256").update(macAddress, "utf8").digest("hex");
	consola.debug(`Using machine ID: ${state.macMachineId}`);
};
const cacheVsCodeDeviceId = async () => {
	state.vsCodeDeviceId = await getVSCodeDeviceId();
	consola.debug(`Using VSCode device ID: ${state.vsCodeDeviceId}`);
};
const SESSION_REFRESH_BASE_MS = 3600 * 1e3;
const SESSION_REFRESH_JITTER_MS = 1200 * 1e3;
let vsCodeSessionRefreshTimer = null;
const generateSessionId = () => {
	state.vsCodeSessionId = randomUUID() + Date.now().toString();
	consola.debug(`Generated VSCode session ID: ${state.vsCodeSessionId}`);
};
const stopVsCodeSessionRefreshLoop = () => {
	if (vsCodeSessionRefreshTimer) {
		clearTimeout(vsCodeSessionRefreshTimer);
		vsCodeSessionRefreshTimer = null;
	}
};
const scheduleSessionIdRefresh = () => {
	const delay = SESSION_REFRESH_BASE_MS + Math.floor(Math.random() * SESSION_REFRESH_JITTER_MS);
	consola.debug(`Scheduling next VSCode session ID refresh in ${Math.round(delay / 1e3)} seconds`);
	stopVsCodeSessionRefreshLoop();
	vsCodeSessionRefreshTimer = setTimeout(() => {
		try {
			generateSessionId();
		} catch (error) {
			consola.error("Failed to refresh session ID, rescheduling...", error);
		} finally {
			scheduleSessionIdRefresh();
		}
	}, delay);
};
const cacheVsCodeSessionId = () => {
	stopVsCodeSessionRefreshLoop();
	generateSessionId();
	scheduleSessionIdRefresh();
};
const isRecord = (value) => typeof value === "object" && value !== null;
const getUserIdJsonField = (userIdPayload, field) => {
	const value = userIdPayload?.[field];
	return typeof value === "string" && value.length > 0 ? value : null;
};
const parseJsonUserId = (userId) => {
	try {
		const parsed = JSON.parse(userId);
		return isRecord(parsed) ? parsed : null;
	} catch {
		return null;
	}
};
const parseUserIdMetadata = (userId) => {
	if (!userId || typeof userId !== "string") return {
		safetyIdentifier: null,
		sessionId: null
	};
	const legacySafetyIdentifier = userId.match(/user_([^_]+)_account/)?.[1] ?? null;
	const legacySessionId = userId.match(/_session_(.+)$/)?.[1] ?? null;
	const parsedUserId = legacySafetyIdentifier && legacySessionId ? null : parseJsonUserId(userId);
	return {
		safetyIdentifier: legacySafetyIdentifier ?? getUserIdJsonField(parsedUserId, "device_id") ?? getUserIdJsonField(parsedUserId, "account_uuid"),
		sessionId: legacySessionId ?? getUserIdJsonField(parsedUserId, "session_id")
	};
};
const findLastUserContent = (messages) => {
	for (let i = messages.length - 1; i >= 0; i--) {
		const msg = messages[i];
		if (msg.role === "user" && msg.content) {
			if (typeof msg.content === "string") return msg.content;
			else if (Array.isArray(msg.content)) {
				const array = msg.content.filter((n) => n.type !== "tool_result").map((n) => ({
					...n,
					cache_control: void 0
				}));
				if (array.length > 0) return JSON.stringify(array);
			}
		}
	}
	return null;
};
const generateRequestIdFromPayload = (payload, sessionId) => {
	const messages = payload.messages;
	if (messages) {
		const lastUserContent = typeof messages === "string" ? messages : findLastUserContent(messages);
		if (lastUserContent) return getUUID((sessionId ?? "") + (state.macMachineId ?? "") + lastUserContent);
	}
	return randomUUID();
};
const getRootSessionId = (anthropicPayload, c) => {
	const userId = anthropicPayload.metadata?.user_id;
	const sessionId = userId ? parseUserIdMetadata(userId).sessionId || void 0 : c.req.header("x-session-id");
	return sessionId ? getUUID(sessionId) : sessionId;
};
const getUUID = (content) => {
	const uuidBytes = createHash("sha256").update(content).digest().subarray(0, 16);
	uuidBytes[6] = uuidBytes[6] & 15 | 64;
	uuidBytes[8] = uuidBytes[8] & 63 | 128;
	const uuidHex = uuidBytes.toString("hex");
	return `${uuidHex.slice(0, 8)}-${uuidHex.slice(8, 12)}-${uuidHex.slice(12, 16)}-${uuidHex.slice(16, 20)}-${uuidHex.slice(20)}`;
};

//#endregion
export { requestContext as A, state as B, githubHeaders as C, prepareInteractionHeaders as D, prepareForCompact as E, compactAutoContinuePromptStarts as F, compactMessageSections as I, compactSummaryPromptStart as L, initOpencodeVersion as M, COMPACT_AUTO_CONTINUE as N, prepareMessageProxyHeaders as O, COMPACT_REQUEST as P, compactSystemPromptStarts as R, getOauthUrls as S, isOpencodeOauthApp as T, forwardError as _, cacheVsCodeSessionId as a, getGitHubApiBaseUrl as b, getUUID as c, sleep as d, exposeAlias as f, HTTPError as g, getCopilotUsage as h, cacheVsCodeDeviceId as i, resolveTraceId as j, generateTraceId as k, isNullish as l, resolveToUpstream as m, cacheModels as n, generateRequestIdFromPayload as o, getUpstreamForAlias as p, cacheVSCodeVersion as r, getRootSessionId as s, cacheMacMachineId as t, parseUserIdMetadata as u, copilotBaseUrl as v, githubUserHeaders as w, getOauthAppConfig as x, copilotHeaders as y, compactTextOnlyGuard as z };
//# sourceMappingURL=utils-CBc0KiDM.js.map