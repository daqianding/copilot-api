import { PATHS } from "./paths-Cla6y5eD.js";
import { COMPACT_AUTO_CONTINUE, COMPACT_REQUEST, HTTPError, cacheModels, compactAutoContinuePromptStarts, compactMessageSections, compactSummaryPromptStart, compactSystemPromptStarts, compactTextOnlyGuard, copilotBaseUrl, copilotHeaders, forwardError, generateRequestIdFromPayload, generateTraceId, getCopilotUsage, getRootSessionId, getUUID, getUpstreamForAlias, isNullish, parseUserIdMetadata, prepareForCompact, prepareInteractionHeaders, prepareMessageProxyHeaders, requestContext, resolveToUpstream, resolveTraceId as resolveTraceId$1, sleep, state } from "./utils-Caw-6iPt.js";
import { getAnthropicApiKey, getClaudeTokenMultiplier, getConfig, getExtraPromptForModel, getProviderConfig, getReasoningEffortForModel, getSmallModel, isMessagesApiEnabled, isResponsesApiContextManagementModel, isResponsesApiWebSearchEnabled } from "./config-BQvWqYh_.js";
import consola from "consola";
import fs from "node:fs/promises";
import path from "node:path";
import { Hono } from "hono";
import { cors } from "hono/cors";
import { logger } from "hono/logger";
import fs$1, { readFileSync } from "node:fs";
import { streamSSE } from "hono/streaming";
import util from "node:util";
import { events } from "fetch-event-stream";

//#region src/lib/request-auth.ts
function normalizeApiKeys(apiKeys) {
	if (!Array.isArray(apiKeys)) {
		if (apiKeys !== void 0) consola.warn("Invalid auth.apiKeys config. Expected an array of strings.");
		return [];
	}
	const normalizedKeys = apiKeys.filter((key) => typeof key === "string").map((key) => key.trim()).filter((key) => key.length > 0);
	if (normalizedKeys.length !== apiKeys.length) consola.warn("Invalid auth.apiKeys entries found. Only non-empty strings are allowed.");
	return [...new Set(normalizedKeys)];
}
function getConfiguredApiKeys() {
	const config = getConfig();
	return normalizeApiKeys(config.auth?.apiKeys);
}
function extractRequestApiKey(c) {
	const xApiKey = c.req.header("x-api-key")?.trim();
	if (xApiKey) return xApiKey;
	const authorization = c.req.header("authorization");
	if (!authorization) return null;
	const [scheme, ...rest] = authorization.trim().split(/\s+/);
	if (scheme.toLowerCase() !== "bearer") return null;
	return rest.join(" ").trim() || null;
}
function createUnauthorizedResponse(c) {
	c.header("WWW-Authenticate", "Bearer realm=\"copilot-api\"");
	return c.json({ error: {
		message: "Unauthorized",
		type: "authentication_error"
	} }, 401);
}
function createAuthMiddleware(options = {}) {
	const getApiKeys = options.getApiKeys ?? getConfiguredApiKeys;
	const allowUnauthenticatedPaths = options.allowUnauthenticatedPaths ?? ["/"];
	const allowOptionsBypass = options.allowOptionsBypass ?? true;
	return async (c, next) => {
		if (allowOptionsBypass && c.req.method === "OPTIONS") return next();
		if (allowUnauthenticatedPaths.includes(c.req.path)) return next();
		const apiKeys = getApiKeys();
		if (apiKeys.length === 0) return next();
		const requestApiKey = extractRequestApiKey(c);
		if (!requestApiKey || !apiKeys.includes(requestApiKey)) return createUnauthorizedResponse(c);
		return next();
	};
}

//#endregion
//#region src/lib/trace.ts
const traceIdMiddleware = async (c, next) => {
	const traceId = resolveTraceId$1(c.req.header("x-trace-id"));
	c.header("x-trace-id", traceId);
	const context = {
		traceId,
		startTime: Date.now(),
		userAgent: c.req.header("user-agent") || "",
		sessionAffinity: c.req.header("x-session-affinity"),
		parentSessionId: c.req.header("x-parent-session-id")
	};
	await requestContext.run(context, async () => {
		await next();
	});
};

//#endregion
//#region src/lib/approval.ts
const awaitApproval = async () => {
	if (!await consola.prompt(`Accept incoming request?`, { type: "confirm" })) throw new HTTPError("Request rejected", Response.json({ message: "Request rejected" }, { status: 403 }));
};

//#endregion
//#region src/lib/process-cleanup.ts
const cleanupHandlers = /* @__PURE__ */ new Set();
let cleanupPromise = null;
let cleanupState = "idle";
let runtimeInitialized$1 = false;
function initializeProcessCleanupRuntime() {
	if (runtimeInitialized$1) return;
	runtimeInitialized$1 = true;
	process.once("beforeExit", () => {
		runProcessCleanups();
	});
	process.once("exit", runProcessCleanupsSync);
	process.once("SIGINT", () => {
		shutdownProcess(0);
	});
	process.once("SIGTERM", () => {
		shutdownProcess(0);
	});
}
function runProcessCleanupsSync() {
	if (cleanupState !== "idle") return;
	cleanupState = "done";
	for (const handler of Array.from(cleanupHandlers)) try {
		handler();
	} catch {}
}
async function runProcessCleanups() {
	if (cleanupPromise) return cleanupPromise;
	if (cleanupState === "done") return;
	cleanupState = "running";
	cleanupPromise = (async () => {
		for (const handler of Array.from(cleanupHandlers)) await handler();
		cleanupState = "done";
	})();
	return cleanupPromise;
}
async function shutdownProcess(exitCode) {
	try {
		await runProcessCleanups();
	} finally {
		process.exit(exitCode);
	}
}
function registerProcessCleanup(handler) {
	initializeProcessCleanupRuntime();
	cleanupHandlers.add(handler);
	return () => {
		cleanupHandlers.delete(handler);
	};
}

//#endregion
//#region src/lib/logger.ts
const LOG_RETENTION_MS = 10080 * 60 * 1e3;
const CLEANUP_INTERVAL_MS = 1440 * 60 * 1e3;
const LOG_DIR = path.join(PATHS.APP_DIR, "logs");
const FLUSH_INTERVAL_MS = 1e3;
const MAX_BUFFER_SIZE = 100;
const logStreams = /* @__PURE__ */ new Map();
const logBuffers = /* @__PURE__ */ new Map();
let runtimeInitialized = false;
let flushInterval;
let cleanupInterval;
const ensureLogDirectory = () => {
	if (!fs$1.existsSync(LOG_DIR)) fs$1.mkdirSync(LOG_DIR, { recursive: true });
};
const cleanupOldLogs = () => {
	if (!fs$1.existsSync(LOG_DIR)) return;
	const now = Date.now();
	for (const entry of fs$1.readdirSync(LOG_DIR)) {
		const filePath = path.join(LOG_DIR, entry);
		let stats;
		try {
			stats = fs$1.statSync(filePath);
		} catch {
			continue;
		}
		if (!stats.isFile()) continue;
		if (now - stats.mtimeMs > LOG_RETENTION_MS) try {
			fs$1.rmSync(filePath);
		} catch {
			continue;
		}
	}
};
const formatArgs = (args) => args.map((arg) => typeof arg === "string" ? arg : util.inspect(arg, {
	depth: null,
	colors: false
})).join(" ");
const sanitizeName = (name) => {
	const normalized = name.toLowerCase().replaceAll(/[^a-z0-9]+/g, "-").replaceAll(/^-+|-+$/g, "");
	return normalized === "" ? "handler" : normalized;
};
const maybeUnref = (timer) => {
	timer.unref();
};
const flushBuffer = (filePath) => {
	const buffer = logBuffers.get(filePath);
	if (!buffer || buffer.length === 0) return;
	const stream = getLogStream(filePath);
	const content = buffer.join("\n") + "\n";
	stream.write(content, (error) => {
		if (error) console.warn("Failed to write handler log", error);
	});
	logBuffers.set(filePath, []);
};
const flushAllBuffers = () => {
	for (const filePath of logBuffers.keys()) flushBuffer(filePath);
};
const cleanup = () => {
	if (flushInterval) {
		clearInterval(flushInterval);
		flushInterval = void 0;
	}
	if (cleanupInterval) {
		clearInterval(cleanupInterval);
		cleanupInterval = void 0;
	}
	flushAllBuffers();
	for (const stream of logStreams.values()) stream.end();
	logStreams.clear();
	logBuffers.clear();
};
const initializeLoggerRuntime = () => {
	if (runtimeInitialized) return;
	runtimeInitialized = true;
	ensureLogDirectory();
	cleanupOldLogs();
	flushInterval = setInterval(flushAllBuffers, FLUSH_INTERVAL_MS);
	maybeUnref(flushInterval);
	cleanupInterval = setInterval(cleanupOldLogs, CLEANUP_INTERVAL_MS);
	maybeUnref(cleanupInterval);
	registerProcessCleanup(cleanup);
};
const getLogStream = (filePath) => {
	initializeLoggerRuntime();
	let stream = logStreams.get(filePath);
	if (!stream || stream.destroyed) {
		stream = fs$1.createWriteStream(filePath, { flags: "a" });
		logStreams.set(filePath, stream);
		stream.on("error", (error) => {
			console.warn("Log stream error", error);
			logStreams.delete(filePath);
		});
	}
	return stream;
};
const appendLine = (filePath, line) => {
	let buffer = logBuffers.get(filePath);
	if (!buffer) {
		buffer = [];
		logBuffers.set(filePath, buffer);
	}
	buffer.push(line);
	if (buffer.length >= MAX_BUFFER_SIZE) flushBuffer(filePath);
};
const debugLazy = (logger$7, factory) => {
	if (!state.verbose) return;
	logger$7.debug(...factory());
};
const debugJson = (logger$7, label, value) => {
	debugLazy(logger$7, () => [label, JSON.stringify(value)]);
};
const debugJsonTail = (logger$7, label, { value, tailLength = 400 }) => {
	debugLazy(logger$7, () => [label, JSON.stringify(value).slice(-tailLength)]);
};
const createHandlerLogger = (name) => {
	const sanitizedName = sanitizeName(name);
	const instance = consola.withTag(name);
	if (state.verbose) instance.level = 5;
	instance.setReporters([]);
	instance.addReporter({ log(logObj) {
		initializeLoggerRuntime();
		const traceId = requestContext.getStore()?.traceId;
		const date = logObj.date;
		const dateKey = date.toLocaleDateString("sv-SE");
		const timestamp = date.toLocaleString("sv-SE", { hour12: false });
		const filePath = path.join(LOG_DIR, `${sanitizedName}-${dateKey}.log`);
		const message = formatArgs(logObj.args);
		const traceIdStr = traceId ? ` [${traceId}]` : "";
		const line = `[${timestamp}] [${logObj.type}] [${logObj.tag || name}]${traceIdStr}${message ? ` ${message}` : ""}`;
		appendLine(filePath, line);
	} });
	return instance;
};

//#endregion
//#region src/lib/rate-limit.ts
async function checkRateLimit(state$1) {
	if (state$1.rateLimitSeconds === void 0) return;
	const now = Date.now();
	if (!state$1.lastRequestTimestamp) {
		state$1.lastRequestTimestamp = now;
		return;
	}
	const elapsedSeconds = (now - state$1.lastRequestTimestamp) / 1e3;
	if (elapsedSeconds > state$1.rateLimitSeconds) {
		state$1.lastRequestTimestamp = now;
		return;
	}
	const waitTimeSeconds = Math.ceil(state$1.rateLimitSeconds - elapsedSeconds);
	if (!state$1.rateLimitWait) {
		consola.warn(`Rate limit exceeded. Need to wait ${waitTimeSeconds} more seconds.`);
		throw new HTTPError("Rate limit exceeded", Response.json({ message: "Rate limit exceeded" }, { status: 429 }));
	}
	const waitTimeMs = waitTimeSeconds * 1e3;
	consola.warn(`Rate limit reached. Waiting ${waitTimeSeconds} seconds before proceeding...`);
	await sleep(waitTimeMs);
	state$1.lastRequestTimestamp = now;
	consola.info("Rate limit wait completed, proceeding with request");
}

//#endregion
//#region src/lib/event-bus.ts
var EventBus = class {
	handlers = /* @__PURE__ */ new Map();
	publish(name, event) {
		const handlers = this.handlers.get(name);
		if (!handlers) return;
		for (const handler of Array.from(handlers)) handler(event);
	}
	subscribe(name, handler) {
		let handlers = this.handlers.get(name);
		if (!handlers) {
			handlers = /* @__PURE__ */ new Set();
			this.handlers.set(name, handlers);
		}
		const registeredHandler = handler;
		handlers.add(registeredHandler);
		return () => {
			handlers.delete(registeredHandler);
			if (handlers.size === 0) this.handlers.delete(name);
		};
	}
};

//#endregion
//#region src/lib/sqlite.ts
const MINIMUM_NODE_SQLITE_VERSION = "22.13.0";
const isBunRuntime = () => Boolean(globalThis.Bun);
function parseNodeVersion(version) {
	return version.split(".", 3).map((part) => {
		const parsed = Number.parseInt(part, 10);
		return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
	});
}
function isNodeSqliteSupportedVersion(version) {
	const current = parseNodeVersion(version);
	const minimum = parseNodeVersion(MINIMUM_NODE_SQLITE_VERSION);
	for (const [index, minimumPart] of minimum.entries()) {
		const currentPart = current[index] ?? 0;
		if (currentPart > minimumPart) return true;
		if (currentPart < minimumPart) return false;
	}
	return true;
}
function isSqliteRuntimeSupported(input = {}) {
	if (input.isBun ?? isBunRuntime()) return true;
	return isNodeSqliteSupportedVersion(input.nodeVersion ?? process.versions.node);
}
function getUnsupportedNodeSqliteMessage(nodeVersion) {
	return `SQLite-backed token usage requires Bun or Node.js >= ${MINIMUM_NODE_SQLITE_VERSION}. Detected Node.js ${nodeVersion}. Upgrade Node.js or run the CLI with Bun, for example \`bunx --bun @jeffreycao/copilot-api@latest start\` or \`bun run start start\`.`;
}
var UnsupportedNodeSqliteRuntimeError = class extends Error {
	constructor(nodeVersion, cause) {
		super(getUnsupportedNodeSqliteMessage(nodeVersion), { cause });
		this.name = "UnsupportedNodeSqliteRuntimeError";
	}
};
async function openBunDatabase(dbPath) {
	return new (await (import(["bun", "sqlite"].join(":")))).Database(dbPath);
}
async function loadNodeSqliteModule() {
	const nodeVersion = process.versions.node;
	if (!isNodeSqliteSupportedVersion(nodeVersion)) throw new UnsupportedNodeSqliteRuntimeError(nodeVersion);
	const specifier = ["node", "sqlite"].join(":");
	try {
		return await import(specifier);
	} catch (error) {
		throw new UnsupportedNodeSqliteRuntimeError(nodeVersion, error);
	}
}
async function openNodeDatabase(dbPath) {
	return new (await (loadNodeSqliteModule())).DatabaseSync(dbPath);
}
async function openSqliteDatabase(dbPath) {
	const dir = path.dirname(dbPath);
	if (dbPath !== ":memory:" && dir !== ".") await fs.mkdir(dir, { recursive: true });
	return isBunRuntime() ? openBunDatabase(dbPath) : openNodeDatabase(dbPath);
}
var SqliteDbStore = class {
	dbPromise = null;
	options;
	constructor(options) {
		this.options = options;
	}
	getDb() {
		this.dbPromise ??= this.open();
		return this.dbPromise;
	}
	async close(input) {
		const currentDbPromise = this.dbPromise;
		this.dbPromise = null;
		if (!currentDbPromise) return;
		const db = await currentDbPromise;
		input?.beforeClose?.(db);
		db.close?.();
	}
	async open() {
		const db = await openSqliteDatabase(this.options.getPath());
		this.options.initialize?.(db);
		return db;
	}
};

//#endregion
//#region src/lib/token-usage/store.ts
const DB_PATH_ENV = "COPILOT_API_SQLITE_DB_PATH";
const DEFAULT_DB_FILENAME = "copilot-api.sqlite";
let writeQueue = Promise.resolve();
function getDbPath() {
	return process.env[DB_PATH_ENV] ?? path.join(PATHS.APP_DIR, DEFAULT_DB_FILENAME);
}
const tokenUsageDbStore = new SqliteDbStore({
	getPath: getDbPath,
	initialize: initializeTokenUsageDb
});
function getDb() {
	return tokenUsageDbStore.getDb();
}
function isTokenUsageStorageEnabled() {
	return isSqliteRuntimeSupported();
}
function initializeTokenUsageDb(db) {
	db.exec("PRAGMA journal_mode = WAL");
	db.exec("PRAGMA busy_timeout = 5000");
	db.exec(`
    CREATE TABLE IF NOT EXISTS token_usage_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at_ms INTEGER NOT NULL,
      created_at_utc TEXT NOT NULL,
      trace_id TEXT NOT NULL,
      session_id TEXT NOT NULL DEFAULT '',
      user_id TEXT NOT NULL DEFAULT '',
      source TEXT NOT NULL,
      endpoint TEXT NOT NULL,
      provider_name TEXT,
      model TEXT NOT NULL,
      input_tokens INTEGER NOT NULL DEFAULT 0,
      output_tokens INTEGER NOT NULL DEFAULT 0,
      cache_read_input_tokens INTEGER NOT NULL DEFAULT 0,
      cache_creation_input_tokens INTEGER NOT NULL DEFAULT 0,
      total_tokens INTEGER NOT NULL DEFAULT 0
    )
  `);
	ensureColumn(db, "user_id", "TEXT NOT NULL DEFAULT ''");
	ensureColumn(db, "total_tokens", "INTEGER NOT NULL DEFAULT 0");
	db.exec(`
    CREATE INDEX IF NOT EXISTS idx_token_usage_events_created_at_ms
    ON token_usage_events(created_at_ms)
  `);
	db.exec(`
    CREATE INDEX IF NOT EXISTS idx_token_usage_events_model
    ON token_usage_events(model)
  `);
	db.exec(`
    CREATE INDEX IF NOT EXISTS idx_token_usage_events_trace_id
    ON token_usage_events(trace_id)
  `);
	db.exec(`
    CREATE INDEX IF NOT EXISTS idx_token_usage_events_session_id
    ON token_usage_events(session_id)
  `);
	db.exec(`
    CREATE INDEX IF NOT EXISTS idx_token_usage_events_user_id
    ON token_usage_events(user_id)
  `);
}
function ensureColumn(db, name, definition) {
	if (!db.prepare("PRAGMA table_info(token_usage_events)").all().some((row) => row.name === name)) db.exec(`ALTER TABLE token_usage_events ADD COLUMN ${name} ${definition}`);
}
function normalizeToken(value) {
	if (typeof value !== "number" || !Number.isFinite(value)) return 0;
	return Math.max(0, Math.floor(value));
}
function normalizeOptionalToken(value) {
	return value === null || value === void 0 ? void 0 : normalizeToken(value);
}
function hasAnyToken(tokens) {
	return normalizeToken(tokens.input_tokens) > 0 || normalizeToken(tokens.output_tokens) > 0 || normalizeToken(tokens.cache_read_input_tokens) > 0 || normalizeToken(tokens.cache_creation_input_tokens) > 0 || normalizeToken(tokens.total_tokens) > 0;
}
function resolveTotalTokens(input) {
	const explicitTotal = normalizeOptionalToken(input.total_tokens);
	if (explicitTotal !== void 0) return explicitTotal;
	return normalizeToken(input.input_tokens) + normalizeToken(input.output_tokens) + normalizeToken(input.cache_read_input_tokens) + normalizeToken(input.cache_creation_input_tokens);
}
async function writeTokenUsageEvent(event) {
	(await getDb()).prepare(`
      INSERT INTO token_usage_events (
        created_at_ms,
        created_at_utc,
        trace_id,
        session_id,
        user_id,
        source,
        endpoint,
        provider_name,
        model,
        input_tokens,
        output_tokens,
        cache_read_input_tokens,
        cache_creation_input_tokens,
        total_tokens
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(event.created_at_ms, event.created_at_utc, event.trace_id, event.session_id, event.user_id, event.source, event.endpoint, event.provider_name, event.model, event.input_tokens, event.output_tokens, event.cache_read_input_tokens, event.cache_creation_input_tokens, event.total_tokens);
}
function enqueueTokenUsageWrite(event) {
	if (!isTokenUsageStorageEnabled()) return;
	writeQueue = writeQueue.then(() => writeTokenUsageEvent(event)).catch((error) => {
		consola.warn("Failed to record token usage", error);
	});
}
async function flushTokenUsageEvents() {
	let currentQueue = writeQueue;
	while (true) {
		await currentQueue;
		if (currentQueue === writeQueue) return;
		currentQueue = writeQueue;
	}
}
function getPeriodRange(period, now = /* @__PURE__ */ new Date()) {
	const start = new Date(now);
	switch (period) {
		case "day":
			start.setHours(0, 0, 0, 0);
			break;
		case "week": {
			const daysSinceMonday = (start.getDay() + 6) % 7;
			start.setDate(start.getDate() - daysSinceMonday);
			start.setHours(0, 0, 0, 0);
			break;
		}
		case "month":
			start.setDate(1);
			start.setHours(0, 0, 0, 0);
			break;
		default: break;
	}
	const end = new Date(start);
	switch (period) {
		case "day":
			end.setDate(end.getDate() + 1);
			break;
		case "week":
			end.setDate(end.getDate() + 7);
			break;
		case "month":
			end.setMonth(end.getMonth() + 1);
			break;
		default: break;
	}
	return {
		endMs: end.getTime(),
		startMs: start.getTime()
	};
}
function createEmptyTotals() {
	return {
		cache_creation_input_tokens: 0,
		cache_read_input_tokens: 0,
		input_tokens: 0,
		output_tokens: 0,
		request_count: 0,
		total_tokens: 0
	};
}
function createEmptySummary(period) {
	const range = getPeriodRange(period);
	return {
		byModel: [],
		period,
		range: {
			end_ms: range.endMs,
			end_utc: new Date(range.endMs).toISOString(),
			start_ms: range.startMs,
			start_utc: new Date(range.startMs).toISOString()
		},
		totals: createEmptyTotals()
	};
}
function createEmptyEventsPage(input) {
	const range = getPeriodRange(input.period);
	const page = Math.max(1, Math.floor(input.page));
	const pageSize = Math.min(100, Math.max(1, Math.floor(input.pageSize)));
	return {
		items: [],
		page,
		page_size: pageSize,
		period: input.period,
		range: {
			end_ms: range.endMs,
			end_utc: new Date(range.endMs).toISOString(),
			start_ms: range.startMs,
			start_utc: new Date(range.startMs).toISOString()
		},
		total: 0,
		total_pages: 1
	};
}
function numberFromRow(row, key) {
	const value = row?.[key];
	return typeof value === "number" && Number.isFinite(value) ? value : 0;
}
function totalsFromRow(row) {
	return {
		cache_creation_input_tokens: numberFromRow(row, "cache_creation_input_tokens"),
		cache_read_input_tokens: numberFromRow(row, "cache_read_input_tokens"),
		input_tokens: numberFromRow(row, "input_tokens"),
		output_tokens: numberFromRow(row, "output_tokens"),
		request_count: numberFromRow(row, "request_count"),
		total_tokens: numberFromRow(row, "total_tokens")
	};
}
function stringFromRow(row, key) {
	const value = row[key];
	return typeof value === "string" ? value : "";
}
function nullableStringFromRow(row, key) {
	const value = row[key];
	return typeof value === "string" ? value : null;
}
function usageEventFromRow(row) {
	return {
		cache_creation_input_tokens: numberFromRow(row, "cache_creation_input_tokens"),
		cache_read_input_tokens: numberFromRow(row, "cache_read_input_tokens"),
		created_at_ms: numberFromRow(row, "created_at_ms"),
		created_at_utc: stringFromRow(row, "created_at_utc"),
		endpoint: stringFromRow(row, "endpoint"),
		id: numberFromRow(row, "id"),
		input_tokens: numberFromRow(row, "input_tokens"),
		model: stringFromRow(row, "model") || "unknown",
		output_tokens: numberFromRow(row, "output_tokens"),
		provider_name: nullableStringFromRow(row, "provider_name"),
		session_id: stringFromRow(row, "session_id"),
		source: stringFromRow(row, "source"),
		total_tokens: numberFromRow(row, "total_tokens"),
		trace_id: stringFromRow(row, "trace_id"),
		user_id: stringFromRow(row, "user_id")
	};
}
async function getTokenUsageSummary(period) {
	if (!isTokenUsageStorageEnabled()) return createEmptySummary(period);
	await flushTokenUsageEvents();
	const range = getPeriodRange(period);
	const db = await getDb();
	const totalsRow = db.prepare(`
    SELECT
      COUNT(*) AS request_count,
      COALESCE(SUM(input_tokens), 0) AS input_tokens,
      COALESCE(SUM(output_tokens), 0) AS output_tokens,
      COALESCE(SUM(cache_read_input_tokens), 0) AS cache_read_input_tokens,
      COALESCE(SUM(cache_creation_input_tokens), 0) AS cache_creation_input_tokens,
      COALESCE(SUM(total_tokens), 0) AS total_tokens
    FROM token_usage_events
    WHERE created_at_ms >= ? AND created_at_ms < ?
  `).get(range.startMs, range.endMs);
	return {
		byModel: db.prepare(`
    SELECT
      model,
      COUNT(*) AS request_count,
      COALESCE(SUM(input_tokens), 0) AS input_tokens,
      COALESCE(SUM(output_tokens), 0) AS output_tokens,
      COALESCE(SUM(cache_read_input_tokens), 0) AS cache_read_input_tokens,
      COALESCE(SUM(cache_creation_input_tokens), 0) AS cache_creation_input_tokens,
      COALESCE(SUM(total_tokens), 0) AS total_tokens
    FROM token_usage_events
    WHERE created_at_ms >= ? AND created_at_ms < ?
    GROUP BY model
    ORDER BY
      total_tokens DESC,
      model ASC
  `).all(range.startMs, range.endMs).map((row) => ({
			...totalsFromRow(row),
			model: typeof row.model === "string" ? row.model : "unknown"
		})),
		period,
		range: {
			end_ms: range.endMs,
			end_utc: new Date(range.endMs).toISOString(),
			start_ms: range.startMs,
			start_utc: new Date(range.startMs).toISOString()
		},
		totals: totalsFromRow(totalsRow)
	};
}
async function getTokenUsageEventsPage(input) {
	if (!isTokenUsageStorageEnabled()) return createEmptyEventsPage(input);
	await flushTokenUsageEvents();
	const range = getPeriodRange(input.period);
	const page = Math.max(1, Math.floor(input.page));
	const pageSize = Math.min(100, Math.max(1, Math.floor(input.pageSize)));
	const offset = (page - 1) * pageSize;
	const db = await getDb();
	const totalRow = db.prepare(`
    SELECT COUNT(*) AS total
    FROM token_usage_events
    WHERE created_at_ms >= ? AND created_at_ms < ?
  `).get(range.startMs, range.endMs);
	const rows = db.prepare(`
    SELECT
      id,
      created_at_ms,
      created_at_utc,
      trace_id,
      session_id,
      user_id,
      source,
      endpoint,
      provider_name,
      model,
      input_tokens,
      output_tokens,
      cache_read_input_tokens,
      cache_creation_input_tokens,
      total_tokens
    FROM token_usage_events
    WHERE created_at_ms >= ? AND created_at_ms < ?
    ORDER BY created_at_ms DESC, id DESC
    LIMIT ? OFFSET ?
  `).all(range.startMs, range.endMs, pageSize, offset);
	const total = numberFromRow(totalRow, "total");
	return {
		items: rows.map((row) => usageEventFromRow(row)),
		page,
		page_size: pageSize,
		period: input.period,
		range: {
			end_ms: range.endMs,
			end_utc: new Date(range.endMs).toISOString(),
			start_ms: range.startMs,
			start_utc: new Date(range.startMs).toISOString()
		},
		total,
		total_pages: Math.max(1, Math.ceil(total / pageSize))
	};
}
async function closeUsageStore() {
	await flushTokenUsageEvents();
	await tokenUsageDbStore.close({ beforeClose: (db) => {
		try {
			db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
		} catch {}
	} });
	writeQueue = Promise.resolve();
}
registerProcessCleanup(closeUsageStore);

//#endregion
//#region src/lib/token-usage/index.ts
const tokenUsageEventBus = new EventBus();
function resolveTraceId(traceId) {
	return traceId?.trim() || requestContext.getStore()?.traceId || generateTraceId();
}
function resolveTokenUsageSessionId(sessionId, fallbackSessionId) {
	return requestContext.getStore()?.sessionAffinity?.trim() || sessionId?.trim() || fallbackSessionId?.trim() || "";
}
function resolveUserId(input) {
	if (input.source === "provider") return input.providerName?.trim() || "";
	return state.userName?.trim() || "";
}
function toPersistedEvent(input) {
	if (!hasAnyToken(input)) return null;
	const now = /* @__PURE__ */ new Date();
	return {
		cache_creation_input_tokens: normalizeToken(input.cache_creation_input_tokens),
		cache_read_input_tokens: normalizeToken(input.cache_read_input_tokens),
		created_at_ms: now.getTime(),
		created_at_utc: now.toISOString(),
		endpoint: input.endpoint,
		input_tokens: normalizeToken(input.input_tokens),
		model: input.model.trim() || "unknown",
		output_tokens: normalizeToken(input.output_tokens),
		provider_name: input.providerName?.trim() || null,
		session_id: resolveTokenUsageSessionId(input.sessionId, input.fallbackSessionId),
		source: input.source,
		total_tokens: resolveTotalTokens(input),
		trace_id: resolveTraceId(input.traceId),
		user_id: resolveUserId(input)
	};
}
tokenUsageEventBus.subscribe("token_usage.recorded", enqueueTokenUsageWrite);
function recordTokenUsageEvent(input) {
	const event = toPersistedEvent(input);
	if (!event) return;
	tokenUsageEventBus.publish("token_usage.recorded", event);
}
function createTokenUsageRecorder(options) {
	return (usage) => {
		recordTokenUsageEvent({
			...usage,
			...options
		});
	};
}
function createCopilotTokenUsageRecorder(options) {
	return createTokenUsageRecorder({
		...options,
		source: "copilot"
	});
}
function createProviderTokenUsageRecorder(options) {
	return createTokenUsageRecorder({
		...options,
		source: "provider"
	});
}
function normalizeOpenAIUsage(usage) {
	const cachedTokens = normalizeToken(usage?.prompt_tokens_details?.cached_tokens);
	const promptTokens = normalizeToken(usage?.prompt_tokens);
	return {
		cache_read_input_tokens: cachedTokens,
		input_tokens: Math.max(0, promptTokens - cachedTokens),
		output_tokens: normalizeToken(usage?.completion_tokens),
		total_tokens: normalizeOptionalToken(usage?.total_tokens)
	};
}
function normalizeResponsesUsage(usage) {
	const cachedTokens = normalizeToken(usage?.input_tokens_details?.cached_tokens);
	const inputTokens = normalizeToken(usage?.input_tokens);
	return {
		cache_read_input_tokens: cachedTokens,
		input_tokens: Math.max(0, inputTokens - cachedTokens),
		output_tokens: normalizeToken(usage?.output_tokens),
		total_tokens: normalizeOptionalToken(usage?.total_tokens)
	};
}
function normalizeAnthropicUsage(usage) {
	return {
		cache_creation_input_tokens: normalizeOptionalToken(usage?.cache_creation_input_tokens),
		cache_read_input_tokens: normalizeOptionalToken(usage?.cache_read_input_tokens),
		input_tokens: normalizeOptionalToken(usage?.input_tokens),
		output_tokens: normalizeOptionalToken(usage?.output_tokens),
		total_tokens: normalizeOptionalToken(usage?.total_tokens)
	};
}
function mergeAnthropicUsage(current, next) {
	return {
		cache_creation_input_tokens: next.cache_creation_input_tokens ?? current.cache_creation_input_tokens,
		cache_read_input_tokens: next.cache_read_input_tokens ?? current.cache_read_input_tokens,
		input_tokens: next.input_tokens ?? current.input_tokens,
		output_tokens: next.output_tokens ?? current.output_tokens,
		total_tokens: next.total_tokens ?? current.total_tokens
	};
}

//#endregion
//#region src/lib/copilot-rate-limit.ts
const copilotRateLimitTypes = ["session", "weekly"];
const copilotRateLimitHeaders = {
	session: "x-usage-ratelimit-session",
	weekly: "x-usage-ratelimit-weekly"
};
const hasGetMethod = (headers) => {
	return "get" in headers && typeof headers.get === "function";
};
const getHeaderValue = (headers, headerName) => {
	if (hasGetMethod(headers)) return headers.get(headerName);
	const normalizedHeaderName = headerName.toLowerCase();
	return Object.entries(headers).find(([key]) => key.toLowerCase() === normalizedHeaderName)?.[1] ?? null;
};
const parseCopilotRateLimitHeader = (headerValue) => {
	const params = new URLSearchParams(headerValue);
	const remaining = params.get("rem");
	const resetAt = params.get("rst");
	if (!remaining || !resetAt) return null;
	return {
		remaining,
		resetAt
	};
};
const getCopilotRateLimitUsage = (headers, type) => {
	const headerName = copilotRateLimitHeaders[type];
	const headerValue = getHeaderValue(headers, headerName);
	if (!headerValue) return null;
	const parsed = parseCopilotRateLimitHeader(headerValue);
	if (!parsed) return null;
	return {
		type,
		...parsed
	};
};
const logCopilotRateLimits = (headers) => {
	for (const type of copilotRateLimitTypes) {
		const usage = getCopilotRateLimitUsage(headers, type);
		if (!usage) continue;
		const d = new Date(usage.resetAt);
		const dateStr = Number.isNaN(d.getTime()) ? usage.resetAt : d.toLocaleString();
		consola.info(`Copilot ${usage.type} quota remaining: ${usage.remaining}, resets at: ${dateStr}`);
	}
};

//#endregion
//#region src/services/copilot/create-chat-completions.ts
const createChatCompletions = async (payload, options) => {
	if (!state.copilotToken) throw new Error("Copilot token not found");
	const enableVision = payload.messages.some((x) => typeof x.content !== "string" && x.content?.some((x$1) => x$1.type === "image_url"));
	let isAgentCall = false;
	if (payload.messages.length > 0) {
		const lastMessage = payload.messages.at(-1);
		if (lastMessage) isAgentCall = ["assistant", "tool"].includes(lastMessage.role);
	}
	const headers = {
		...copilotHeaders(state, options.requestId, enableVision),
		"x-initiator": isAgentCall ? "agent" : "user"
	};
	prepareInteractionHeaders(options.sessionId, Boolean(options.subagentMarker), headers);
	prepareForCompact(headers, options.compactType);
	consola.log(`<-- model: ${payload.model}`);
	const response = await fetch(`${copilotBaseUrl(state)}/chat/completions`, {
		method: "POST",
		headers,
		body: JSON.stringify(payload)
	});
	logCopilotRateLimits(response.headers);
	if (!response.ok) {
		consola.error("Failed to create chat completions", response);
		throw new HTTPError("Failed to create chat completions", response);
	}
	if (payload.stream) return events(response);
	return await response.json();
};

//#endregion
//#region src/routes/chat-completions/handler.ts
const logger$6 = createHandlerLogger("chat-completions-handler");
async function handleCompletion$1(c) {
	await checkRateLimit(state);
	let payload = await c.req.json();
	payload.model = resolveToUpstream(payload.model);
	debugJsonTail(logger$6, "Request payload:", {
		value: payload,
		tailLength: 400
	});
	const selectedModel = state.models?.data.find((model) => model.id === payload.model);
	if (selectedModel?.id === "gpt-5.4") return c.json({ error: {
		message: "Please use `/v1/responses` or `/v1/messages` API",
		type: "invalid_request_error"
	} }, 400);
	if (state.manualApprove) await awaitApproval();
	if (isNullish(payload.max_tokens)) {
		payload = {
			...payload,
			max_tokens: selectedModel?.capabilities.limits.max_output_tokens
		};
		debugJson(logger$6, "Set max_tokens to:", payload.max_tokens);
	}
	const requestId = generateRequestIdFromPayload(payload);
	logger$6.debug("Generated request ID:", requestId);
	const sessionId = getUUID(requestId);
	logger$6.debug("Extracted session ID:", sessionId);
	const recordUsage = createCopilotTokenUsageRecorder({
		endpoint: "chat_completions",
		fallbackSessionId: sessionId,
		model: payload.model
	});
	const response = await createChatCompletions(payload, {
		requestId,
		sessionId
	});
	if (isNonStreaming$1(response)) {
		debugJson(logger$6, "Non-streaming response:", response);
		recordUsage(normalizeOpenAIUsage(response.usage));
		return c.json(response);
	}
	logger$6.debug("Streaming response");
	return streamSSE(c, async (stream) => {
		let usage = {};
		for await (const chunk of response) {
			debugJson(logger$6, "Streaming chunk:", chunk);
			const parsedChunk = parseChatCompletionChunk(chunk);
			if (parsedChunk?.usage) usage = normalizeOpenAIUsage(parsedChunk.usage);
			await stream.writeSSE(chunk);
		}
		recordUsage(usage);
	});
}
const isNonStreaming$1 = (response) => Object.hasOwn(response, "choices");
const parseChatCompletionChunk = (chunk) => {
	const data = chunk.data;
	if (!data || data === "[DONE]") return null;
	try {
		return JSON.parse(data);
	} catch {
		return null;
	}
};

//#endregion
//#region src/routes/chat-completions/route.ts
const completionRoutes = new Hono();
completionRoutes.post("/", async (c) => {
	try {
		return await handleCompletion$1(c);
	} catch (error) {
		return await forwardError(c, error);
	}
});

//#endregion
//#region src/services/copilot/create-embeddings.ts
const createEmbeddings = async (payload) => {
	if (!state.copilotToken) throw new Error("Copilot token not found");
	const response = await fetch(`${copilotBaseUrl(state)}/embeddings`, {
		method: "POST",
		headers: copilotHeaders(state),
		body: JSON.stringify(payload)
	});
	if (!response.ok) throw new HTTPError("Failed to create embeddings", response);
	return await response.json();
};

//#endregion
//#region src/routes/embeddings/route.ts
const embeddingRoutes = new Hono();
embeddingRoutes.post("/", async (c) => {
	try {
		const paylod = await c.req.json();
		const response = await createEmbeddings(paylod);
		createCopilotTokenUsageRecorder({
			endpoint: "embeddings",
			model: paylod.model
		})({
			input_tokens: response.usage.prompt_tokens,
			output_tokens: 0
		});
		return c.json(response);
	} catch (error) {
		return await forwardError(c, error);
	}
});

//#endregion
//#region src/lib/tokenizer.ts
const ENCODING_MAP = {
	o200k_base: () => import("gpt-tokenizer/encoding/o200k_base"),
	cl100k_base: () => import("gpt-tokenizer/encoding/cl100k_base"),
	p50k_base: () => import("gpt-tokenizer/encoding/p50k_base"),
	p50k_edit: () => import("gpt-tokenizer/encoding/p50k_edit"),
	r50k_base: () => import("gpt-tokenizer/encoding/r50k_base")
};
const encodingCache = /* @__PURE__ */ new Map();
/**
* Calculate tokens for tool calls
*/
const calculateToolCallsTokens = (toolCalls, encoder, constants) => {
	let tokens = 0;
	for (const toolCall of toolCalls) {
		tokens += constants.funcInit;
		tokens += encoder.encode(toolCall.id).length;
		tokens += encoder.encode(toolCall.function.name).length;
		tokens += encoder.encode(toolCall.function.arguments).length;
	}
	tokens += constants.funcEnd;
	return tokens;
};
/**
* Calculate tokens for content parts
*/
const calculateContentPartsTokens = (contentParts, encoder) => {
	let tokens = 0;
	for (const part of contentParts) if (part.type === "image_url") tokens += encoder.encode(part.image_url.url).length + 85;
	else if (part.text) tokens += encoder.encode(part.text).length;
	return tokens;
};
/**
* Calculate tokens for a single message
*/
const calculateMessageTokens = (message, encoder, constants) => {
	const tokensPerMessage = 3;
	const tokensPerName = 1;
	let tokens = tokensPerMessage;
	for (const [key, value] of Object.entries(message)) {
		if (key === "reasoning_opaque") continue;
		if (typeof value === "string") tokens += encoder.encode(value).length;
		if (key === "name") tokens += tokensPerName;
		if (key === "tool_calls") tokens += calculateToolCallsTokens(value, encoder, constants);
		if (key === "content" && Array.isArray(value)) tokens += calculateContentPartsTokens(value, encoder);
	}
	return tokens;
};
/**
* Calculate tokens using custom algorithm
*/
const calculateTokens = (messages, encoder, constants) => {
	if (messages.length === 0) return 0;
	let numTokens = 0;
	for (const message of messages) numTokens += calculateMessageTokens(message, encoder, constants);
	numTokens += 3;
	return numTokens;
};
/**
* Get the corresponding encoder module based on encoding type
*/
const getEncodeChatFunction = async (encoding) => {
	if (encodingCache.has(encoding)) {
		const cached = encodingCache.get(encoding);
		if (cached) return cached;
	}
	const supportedEncoding = encoding;
	if (!(supportedEncoding in ENCODING_MAP)) {
		const fallbackModule = await ENCODING_MAP.o200k_base();
		encodingCache.set(encoding, fallbackModule);
		return fallbackModule;
	}
	const encodingModule = await ENCODING_MAP[supportedEncoding]();
	encodingCache.set(encoding, encodingModule);
	return encodingModule;
};
/**
* Get tokenizer type from model information
*/
const getTokenizerFromModel = (model) => {
	return model.capabilities.tokenizer || "o200k_base";
};
/**
* Get model-specific constants for token calculation
*/
const getModelConstants = (model) => {
	return model.id === "gpt-3.5-turbo" || model.id === "gpt-4" ? {
		funcInit: 10,
		propInit: 3,
		propKey: 3,
		enumInit: -3,
		enumItem: 3,
		funcEnd: 12,
		isGpt: true
	} : {
		funcInit: 7,
		propInit: 3,
		propKey: 3,
		enumInit: -3,
		enumItem: 3,
		funcEnd: 12,
		isGpt: model.id.startsWith("gpt-")
	};
};
/**
* Calculate tokens for a single parameter
*/
const calculateParameterTokens = (key, prop, context) => {
	const { encoder, constants } = context;
	let tokens = constants.propKey;
	if (typeof prop !== "object" || prop === null) return tokens;
	const param = prop;
	const paramName = key;
	const paramType = param.type || "string";
	let paramDesc = param.description || "";
	if (param.enum && Array.isArray(param.enum)) {
		tokens += constants.enumInit;
		for (const item of param.enum) {
			tokens += constants.enumItem;
			tokens += encoder.encode(String(item)).length;
		}
	}
	if (paramDesc.endsWith(".")) paramDesc = paramDesc.slice(0, -1);
	const line = `${paramName}:${paramType}:${paramDesc}`;
	tokens += encoder.encode(line).length;
	if (param.type === "array" && param["items"]) tokens += calculateParametersTokens(param["items"], encoder, constants);
	const excludedKeys = new Set([
		"type",
		"description",
		"enum",
		"items"
	]);
	for (const propertyName of Object.keys(param)) if (!excludedKeys.has(propertyName)) {
		const propertyValue = param[propertyName];
		const propertyText = typeof propertyValue === "string" ? propertyValue : JSON.stringify(propertyValue);
		tokens += encoder.encode(`${propertyName}:${propertyText}`).length;
	}
	return tokens;
};
/**
* Calculate tokens for properties object
*/
const calculatePropertiesTokens = (properties, encoder, constants) => {
	let tokens = 0;
	if (Object.keys(properties).length > 0) {
		tokens += constants.propInit;
		for (const propKey of Object.keys(properties)) tokens += calculateParameterTokens(propKey, properties[propKey], {
			encoder,
			constants
		});
	}
	return tokens;
};
/**
* Calculate tokens for function parameters
*/
const calculateParametersTokens = (parameters, encoder, constants) => {
	if (!parameters || typeof parameters !== "object") return 0;
	const params = parameters;
	let tokens = 0;
	const excludedKeys = new Set(["$schema", "additionalProperties"]);
	for (const [key, value] of Object.entries(params)) {
		if (excludedKeys.has(key)) continue;
		if (key === "properties") tokens += calculatePropertiesTokens(value, encoder, constants);
		else {
			const paramText = typeof value === "string" ? value : JSON.stringify(value);
			tokens += encoder.encode(`${key}:${paramText}`).length;
		}
	}
	return tokens;
};
/**
* Calculate tokens for a single tool
*/
const calculateToolTokens = (tool, encoder, constants) => {
	let tokens = constants.funcInit;
	const func = tool.function;
	const fName = func.name;
	let fDesc = func.description || "";
	if (fDesc.endsWith(".")) fDesc = fDesc.slice(0, -1);
	const line = fName + ":" + fDesc;
	tokens += encoder.encode(line).length;
	if (typeof func.parameters === "object" && func.parameters !== null) tokens += calculateParametersTokens(func.parameters, encoder, constants);
	return tokens;
};
/**
* Calculate token count for tools based on model
*/
const numTokensForTools = (tools, encoder, constants) => {
	let funcTokenCount = 0;
	if (constants.isGpt) {
		for (const tool of tools) funcTokenCount += calculateToolTokens(tool, encoder, constants);
		funcTokenCount += constants.funcEnd;
	} else for (const tool of tools) funcTokenCount += encoder.encode(JSON.stringify(tool)).length;
	return funcTokenCount;
};
/**
* Calculate the token count of messages, supporting multiple GPT encoders
*/
const getTokenCount = async (payload, model) => {
	const tokenizer = getTokenizerFromModel(model);
	const encoder = await getEncodeChatFunction(tokenizer);
	const simplifiedMessages = payload.messages;
	const inputMessages = simplifiedMessages.filter((msg) => msg.role !== "assistant");
	const outputMessages = simplifiedMessages.filter((msg) => msg.role === "assistant");
	const constants = getModelConstants(model);
	let inputTokens = calculateTokens(inputMessages, encoder, constants);
	if (payload.tools && payload.tools.length > 0) inputTokens += numTokensForTools(payload.tools, encoder, constants);
	const outputTokens = calculateTokens(outputMessages, encoder, constants);
	return {
		input: inputTokens,
		output: outputTokens
	};
};

//#endregion
//#region src/lib/models.ts
const findEndpointModel = (sdkModelId) => {
	const models = state.models?.data ?? [];
	const exactMatch = models.find((m) => m.id === sdkModelId);
	if (exactMatch) {
		const upstream = getUpstreamForAlias(exactMatch.id);
		if (upstream) return {
			...exactMatch,
			id: upstream
		};
		return exactMatch;
	}
	const normalized = _normalizeSdkModelId(sdkModelId);
	if (!normalized) return;
	const modelName = `claude-${normalized.family}-${normalized.version}`;
	const model = models.find((m) => m.id === modelName);
	if (model) return model;
};
/**
* Normalizes an SDK model ID to extract the model family and version.
* this method from github copilot extension
* Examples:
* - "claude-opus-4-5-20251101" -> { family: "opus", version: "4.5" }
* - "claude-3-5-sonnet-20241022" -> { family: "sonnet", version: "3.5" }
* - "claude-sonnet-4-20250514" -> { family: "sonnet", version: "4" }
* - "claude-haiku-3-5-20250514" -> { family: "haiku", version: "3.5" }
* - "claude-haiku-4.5" -> { family: "haiku", version: "4.5" }
*/
const _normalizeSdkModelId = (sdkModelId) => {
	const withoutDate = sdkModelId.toLowerCase().replace(/-\d{8}$/, "");
	const pattern1 = withoutDate.match(/^claude-(\w+)-(\d+)-(\d+)$/);
	if (pattern1) return {
		family: pattern1[1],
		version: `${pattern1[2]}.${pattern1[3]}`
	};
	const pattern2 = withoutDate.match(/^claude-(\d+)-(\d+)-(\w+)$/);
	if (pattern2) return {
		family: pattern2[3],
		version: `${pattern2[1]}.${pattern2[2]}`
	};
	const pattern3 = withoutDate.match(/^claude-(\w+)-(\d+)\.(\d+)$/);
	if (pattern3) return {
		family: pattern3[1],
		version: `${pattern3[2]}.${pattern3[3]}`
	};
	const pattern4 = withoutDate.match(/^claude-(\w+)-(\d+)$/);
	if (pattern4) return {
		family: pattern4[1],
		version: pattern4[2]
	};
	const pattern5 = withoutDate.match(/^claude-(\d+)-(\w+)$/);
	if (pattern5) return {
		family: pattern5[2],
		version: pattern5[1]
	};
};

//#endregion
//#region src/routes/messages/utils.ts
function mapOpenAIStopReasonToAnthropic(finishReason) {
	if (finishReason === null) return null;
	return {
		stop: "end_turn",
		length: "max_tokens",
		tool_calls: "tool_use",
		content_filter: "end_turn"
	}[finishReason];
}

//#endregion
//#region src/routes/messages/non-stream-translation.ts
const THINKING_TEXT = "Thinking...";
function translateToOpenAI(payload) {
	const modelId = payload.model;
	const model = state.models?.data.find((m) => m.id === modelId);
	const thinkingBudget = getThinkingBudget(payload, model);
	return {
		model: modelId,
		messages: translateAnthropicMessagesToOpenAI(payload, modelId, thinkingBudget),
		max_tokens: payload.max_tokens,
		stop: payload.stop_sequences,
		stream: payload.stream,
		temperature: payload.temperature,
		top_p: payload.top_p,
		user: payload.metadata?.user_id,
		tools: translateAnthropicToolsToOpenAI(payload.tools),
		tool_choice: translateAnthropicToolChoiceToOpenAI(payload.tool_choice),
		thinking_budget: thinkingBudget
	};
}
function getThinkingBudget(payload, model) {
	const thinking = payload.thinking;
	if (model && thinking) {
		const maxThinkingBudget = Math.min(model.capabilities.supports.max_thinking_budget ?? 0, (model.capabilities.limits.max_output_tokens ?? 0) - 1);
		thinking.budget_tokens ??= maxThinkingBudget;
		if (maxThinkingBudget > 0) {
			const budgetTokens = Math.min(thinking.budget_tokens, maxThinkingBudget);
			return Math.max(budgetTokens, model.capabilities.supports.min_thinking_budget ?? 1024);
		}
	}
}
function translateAnthropicMessagesToOpenAI(payload, modelId, _thinkingBudget) {
	const systemMessages = handleSystemPrompt(payload.system);
	const otherMessages = payload.messages.flatMap((message) => message.role === "user" ? handleUserMessage(message) : handleAssistantMessage(message, modelId));
	return [...systemMessages, ...otherMessages];
}
function handleSystemPrompt(system) {
	if (!system) return [];
	if (typeof system === "string") return [{
		role: "system",
		content: system
	}];
	else return [{
		role: "system",
		content: system.map((block) => {
			return block.text;
		}).join("\n\n")
	}];
}
function handleUserMessage(message) {
	const newMessages = [];
	if (Array.isArray(message.content)) {
		const toolResultBlocks = message.content.filter((block) => block.type === "tool_result");
		const otherBlocks = message.content.filter((block) => block.type !== "tool_result");
		for (const block of toolResultBlocks) newMessages.push({
			role: "tool",
			tool_call_id: block.tool_use_id,
			content: mapContent(block.content)
		});
		if (otherBlocks.length > 0) newMessages.push({
			role: "user",
			content: mapContent(otherBlocks)
		});
	} else newMessages.push({
		role: "user",
		content: mapContent(message.content)
	});
	return newMessages;
}
function handleAssistantMessage(message, modelId) {
	if (!Array.isArray(message.content)) return [{
		role: "assistant",
		content: mapContent(message.content)
	}];
	const toolUseBlocks = message.content.filter((block) => block.type === "tool_use");
	let thinkingBlocks = message.content.filter((block) => block.type === "thinking");
	if (modelId.startsWith("claude")) thinkingBlocks = thinkingBlocks.filter((b) => b.thinking && b.thinking !== THINKING_TEXT && b.signature && !b.signature.includes("@"));
	const thinkingContents = thinkingBlocks.filter((b) => b.thinking && b.thinking !== THINKING_TEXT).map((b) => b.thinking);
	const allThinkingContent = thinkingContents.length > 0 ? thinkingContents.join("\n\n") : void 0;
	const signature = thinkingBlocks.find((b) => b.signature)?.signature;
	return toolUseBlocks.length > 0 ? [{
		role: "assistant",
		content: mapContent(message.content),
		reasoning_text: allThinkingContent,
		reasoning_opaque: signature,
		tool_calls: toolUseBlocks.map((toolUse) => ({
			id: toolUse.id,
			type: "function",
			function: {
				name: toolUse.name,
				arguments: JSON.stringify(toolUse.input)
			}
		}))
	}] : [{
		role: "assistant",
		content: mapContent(message.content),
		reasoning_text: allThinkingContent,
		reasoning_opaque: signature
	}];
}
function mapContent(content) {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return null;
	const contentParts = [];
	for (const block of content) switch (block.type) {
		case "text":
			contentParts.push({
				type: "text",
				text: block.text
			});
			break;
		case "image":
			contentParts.push({
				type: "image_url",
				image_url: { url: `data:${block.source.media_type};base64,${block.source.data}` }
			});
			break;
		case "document":
			contentParts.push(createDocumentTextPart());
			break;
		case "tool_reference":
			contentParts.push({
				type: "text",
				text: `Tool ${block.tool_name} loaded`
			});
			break;
	}
	return contentParts;
}
function createDocumentTextPart() {
	return {
		type: "text",
		text: "A PDF document was attached, but this api cannot send PDF inputs directly. Analyze using other tools."
	};
}
function translateAnthropicToolsToOpenAI(anthropicTools) {
	if (!anthropicTools) return;
	return anthropicTools.map((tool) => ({
		type: "function",
		function: {
			name: tool.name,
			description: tool.description,
			parameters: normalizeToolSchema(tool.input_schema)
		}
	}));
}
/**
* Ensures `type: "object"` schema has a `properties` field.
* OpenAI's API rejects object schemas without it.
*/
const normalizeToolSchema = (schema) => {
	if (schema.type === "object" && !schema.properties) return {
		...schema,
		properties: {}
	};
	return schema;
};
function translateAnthropicToolChoiceToOpenAI(anthropicToolChoice) {
	if (!anthropicToolChoice) return;
	switch (anthropicToolChoice.type) {
		case "auto": return "auto";
		case "any": return "required";
		case "tool":
			if (anthropicToolChoice.name) return {
				type: "function",
				function: { name: anthropicToolChoice.name }
			};
			return;
		case "none": return "none";
		default: return;
	}
}
function translateToAnthropic(response) {
	const assistantContentBlocks = [];
	let stopReason = response.choices[0]?.finish_reason ?? null;
	for (const choice of response.choices) {
		const textBlocks = getAnthropicTextBlocks(choice.message.content);
		const thinkBlocks = getAnthropicThinkBlocks(choice.message.reasoning_text, choice.message.reasoning_opaque);
		const toolUseBlocks = getAnthropicToolUseBlocks(choice.message.tool_calls);
		assistantContentBlocks.push(...thinkBlocks, ...textBlocks, ...toolUseBlocks);
		if (choice.finish_reason === "tool_calls" || stopReason === "stop") stopReason = choice.finish_reason;
	}
	return {
		id: response.id,
		type: "message",
		role: "assistant",
		model: response.model,
		content: assistantContentBlocks,
		stop_reason: mapOpenAIStopReasonToAnthropic(stopReason),
		stop_sequence: null,
		usage: {
			input_tokens: (response.usage?.prompt_tokens ?? 0) - (response.usage?.prompt_tokens_details?.cached_tokens ?? 0),
			output_tokens: response.usage?.completion_tokens ?? 0,
			...response.usage?.prompt_tokens_details?.cached_tokens !== void 0 && { cache_read_input_tokens: response.usage.prompt_tokens_details.cached_tokens }
		}
	};
}
function getAnthropicTextBlocks(messageContent) {
	if (typeof messageContent === "string" && messageContent.length > 0) return [{
		type: "text",
		text: messageContent
	}];
	if (Array.isArray(messageContent)) return messageContent.filter((part) => part.type === "text").map((part) => ({
		type: "text",
		text: part.text
	}));
	return [];
}
function getAnthropicThinkBlocks(reasoningText, reasoningOpaque) {
	if (reasoningText && reasoningText.length > 0) return [{
		type: "thinking",
		thinking: reasoningText,
		signature: reasoningOpaque || ""
	}];
	if (reasoningOpaque && reasoningOpaque.length > 0) return [{
		type: "thinking",
		thinking: THINKING_TEXT,
		signature: reasoningOpaque
	}];
	return [];
}
function getAnthropicToolUseBlocks(toolCalls) {
	if (!toolCalls) return [];
	return toolCalls.map((toolCall) => ({
		type: "tool_use",
		id: toolCall.id,
		name: toolCall.function.name,
		input: JSON.parse(toolCall.function.arguments)
	}));
}

//#endregion
//#region src/routes/messages/count-tokens-handler.ts
/**
* Forwards token counting to Anthropic's real /v1/messages/count_tokens endpoint.
* Returns the result on success, or null to fall through to estimation.
*/
async function countTokensViaAnthropic(c, payload) {
	if (!payload.model.startsWith("claude")) return null;
	const apiKey = getAnthropicApiKey();
	if (!apiKey) return null;
	const model = payload.model.replaceAll(".", "-");
	const res = await fetch("https://api.anthropic.com/v1/messages/count_tokens", {
		method: "POST",
		headers: {
			"content-type": "application/json",
			"x-api-key": apiKey,
			"anthropic-version": "2023-06-01",
			"anthropic-beta": "token-counting-2024-11-01"
		},
		body: JSON.stringify({
			...payload,
			model
		})
	});
	if (!res.ok) {
		consola.warn("Anthropic count_tokens failed:", res.status, await res.text().catch(() => ""), "- falling back to estimation");
		return null;
	}
	const result = await res.json();
	consola.info("Token count (Anthropic API):", result.input_tokens);
	return c.json(result);
}
/**
* Handles token counting for Anthropic messages.
*
* When an Anthropic API key is available (via config or ANTHROPIC_API_KEY env var)
* and the model is a Claude model, forwards to Anthropic's free /v1/messages/count_tokens
* endpoint for accurate counts. Otherwise falls back to GPT tokenizer estimation.
*/
async function handleCountTokens(c) {
	try {
		const anthropicPayload = await c.req.json();
		anthropicPayload.model = resolveToUpstream(anthropicPayload.model);
		const anthropicResult = await countTokensViaAnthropic(c, anthropicPayload);
		if (anthropicResult) return anthropicResult;
		const anthropicBeta = c.req.header("anthropic-beta");
		const openAIPayload = translateToOpenAI(anthropicPayload);
		const selectedModel = findEndpointModel(anthropicPayload.model);
		anthropicPayload.model = selectedModel?.id ?? anthropicPayload.model;
		if (!selectedModel) {
			consola.warn("Model not found, returning default token count");
			return c.json({ input_tokens: 1 });
		}
		const tokenCount = await getTokenCount(openAIPayload, selectedModel);
		if (anthropicPayload.tools && anthropicPayload.tools.length > 0) {
			let addToolSystemPromptCount = false;
			if (anthropicBeta) {
				const toolsLength = anthropicPayload.tools.length;
				addToolSystemPromptCount = !anthropicPayload.tools.some((tool) => tool.name.startsWith("mcp__") || tool.name === "Skill" && toolsLength === 1);
			}
			if (addToolSystemPromptCount) {
				if (anthropicPayload.model.startsWith("claude")) tokenCount.input = tokenCount.input + 346;
				else if (anthropicPayload.model.startsWith("grok")) tokenCount.input = tokenCount.input + 120;
			}
		}
		let finalTokenCount = tokenCount.input + tokenCount.output;
		if (anthropicPayload.model.startsWith("claude")) finalTokenCount = Math.round(finalTokenCount * getClaudeTokenMultiplier());
		consola.info("Token count:", finalTokenCount);
		return c.json({ input_tokens: finalTokenCount });
	} catch (error) {
		consola.error("Error counting tokens:", error);
		return c.json({ input_tokens: 1 });
	}
}

//#endregion
//#region src/services/copilot/create-messages.ts
const INTERLEAVED_THINKING_BETA = "interleaved-thinking-2025-05-14";
const allowedAnthropicBetas = new Set([
	INTERLEAVED_THINKING_BETA,
	"context-management-2025-06-27",
	"advanced-tool-use-2025-11-20"
]);
const buildAnthropicBetaHeader = (anthropicBetaHeader, thinking, _model) => {
	const isAdaptiveThinking = thinking?.type === "adaptive";
	if (anthropicBetaHeader) {
		const uniqueFilteredBetas = [...anthropicBetaHeader.split(",").map((item) => item.trim()).filter((item) => item.length > 0).filter((item) => allowedAnthropicBetas.has(item))];
		if (uniqueFilteredBetas.length > 0) return uniqueFilteredBetas.join(",");
		return;
	}
	if (thinking?.budget_tokens && !isAdaptiveThinking) return INTERLEAVED_THINKING_BETA;
};
const createMessages = async (payload, anthropicBetaHeader, options) => {
	if (!state.copilotToken) throw new Error("Copilot token not found");
	const enableVision = payload.messages.some((message) => {
		if (!Array.isArray(message.content)) return false;
		return message.content.some((block) => block.type === "image" || block.type === "tool_result" && Array.isArray(block.content) && block.content.some((inner) => inner.type === "image"));
	});
	let isInitiateRequest = false;
	const lastMessage = payload.messages.at(-1);
	if (lastMessage?.role === "user") isInitiateRequest = Array.isArray(lastMessage.content) ? lastMessage.content.some((block) => block.type !== "tool_result") : true;
	const headers = {
		...copilotHeaders(state, options.requestId, enableVision),
		"x-initiator": isInitiateRequest ? "user" : "agent"
	};
	prepareInteractionHeaders(options.sessionId, Boolean(options.subagentMarker), headers);
	prepareForCompact(headers, options.compactType);
	const { safetyIdentifier, sessionId } = parseUserIdMetadata(payload.metadata?.user_id);
	if (safetyIdentifier && sessionId) prepareMessageProxyHeaders(headers);
	const anthropicBeta = buildAnthropicBetaHeader(anthropicBetaHeader, payload.thinking, payload.model);
	if (anthropicBeta) headers["anthropic-beta"] = anthropicBeta;
	consola.log(`<-- model: ${payload.model}`);
	const response = await fetch(`${copilotBaseUrl(state)}/v1/messages`, {
		method: "POST",
		headers,
		body: JSON.stringify(payload)
	});
	logCopilotRateLimits(response.headers);
	if (!response.ok) {
		consola.error("Failed to create messages", response);
		throw new HTTPError("Failed to create messages", response);
	}
	if (payload.stream) return events(response);
	return await response.json();
};

//#endregion
//#region src/services/copilot/create-responses.ts
const createResponses = async (payload, { vision, initiator, subagentMarker, requestId, sessionId, compactType }) => {
	if (!state.copilotToken) throw new Error("Copilot token not found");
	const headers = {
		...copilotHeaders(state, requestId, vision),
		"x-initiator": initiator
	};
	prepareInteractionHeaders(sessionId, Boolean(subagentMarker), headers);
	prepareForCompact(headers, compactType);
	payload.service_tier = void 0;
	consola.log(`<-- model: ${payload.model}`);
	const response = await fetch(`${copilotBaseUrl(state)}/responses`, {
		method: "POST",
		headers,
		body: JSON.stringify(payload)
	});
	logCopilotRateLimits(response.headers);
	if (!response.ok) {
		consola.error("Failed to create responses", response);
		throw new HTTPError("Failed to create responses", response);
	}
	if (payload.stream) return events(response);
	return await response.json();
};

//#endregion
//#region src/routes/messages/responses-translation.ts
const MESSAGE_TYPE = "message";
const COMPACTION_SIGNATURE_PREFIX = "cm1#";
const COMPACTION_SIGNATURE_SEPARATOR = "@";
const THINKING_TEXT$1 = "Thinking...";
const translateAnthropicMessagesToResponsesPayload = (payload) => {
	const input = [];
	const applyPhase = shouldApplyPhase(payload.model);
	for (const message of payload.messages) input.push(...translateMessage(message, payload.model, applyPhase));
	const translatedTools = convertAnthropicTools(payload.tools);
	const toolChoice = convertAnthropicToolChoice(payload.tool_choice);
	const { sessionId: promptCacheKey } = parseUserIdMetadata(payload.metadata?.user_id);
	return {
		model: payload.model,
		input,
		instructions: translateSystemPrompt(payload.system, payload.model),
		temperature: 1,
		top_p: payload.top_p ?? null,
		max_output_tokens: Math.max(payload.max_tokens, 12800),
		tools: translatedTools,
		tool_choice: toolChoice,
		metadata: payload.metadata ? { ...payload.metadata } : null,
		prompt_cache_key: promptCacheKey,
		stream: payload.stream ?? null,
		store: false,
		parallel_tool_calls: true,
		reasoning: {
			effort: getReasoningEffortForModel(payload.model),
			summary: "detailed"
		},
		include: ["reasoning.encrypted_content"]
	};
};
const encodeCompactionCarrierSignature = (compaction) => {
	return `${COMPACTION_SIGNATURE_PREFIX}${compaction.encrypted_content}${COMPACTION_SIGNATURE_SEPARATOR}${compaction.id}`;
};
const decodeCompactionCarrierSignature = (signature) => {
	if (signature.startsWith(COMPACTION_SIGNATURE_PREFIX)) {
		const raw = signature.slice(4);
		const separatorIndex = raw.indexOf(COMPACTION_SIGNATURE_SEPARATOR);
		if (separatorIndex <= 0 || separatorIndex === raw.length - 1) return;
		const encrypted_content = raw.slice(0, separatorIndex);
		const id = raw.slice(separatorIndex + 1);
		if (!encrypted_content) return;
		return {
			id,
			encrypted_content
		};
	}
};
const translateMessage = (message, model, applyPhase) => {
	if (message.role === "user") return translateUserMessage(message);
	return translateAssistantMessage(message, model, applyPhase);
};
const translateUserMessage = (message) => {
	if (typeof message.content === "string") return [createMessage("user", message.content)];
	if (!Array.isArray(message.content)) return [];
	const items = [];
	const pendingContent = [];
	for (const block of message.content) {
		if (block.type === "tool_result") {
			flushPendingContent(pendingContent, items, { role: "user" });
			items.push(createFunctionCallOutput(block));
			continue;
		}
		const converted = translateUserContentBlock(block);
		if (converted.length > 0) pendingContent.push(...converted);
	}
	flushPendingContent(pendingContent, items, { role: "user" });
	return items;
};
const translateAssistantMessage = (message, model, applyPhase) => {
	const assistantPhase = resolveAssistantPhase(model, message.content, applyPhase);
	if (typeof message.content === "string") return [createMessage("assistant", message.content, assistantPhase)];
	if (!Array.isArray(message.content)) return [];
	const items = [];
	const pendingContent = [];
	for (const block of message.content) {
		if (block.type === "tool_use") {
			flushPendingContent(pendingContent, items, {
				role: "assistant",
				phase: assistantPhase
			});
			items.push(createFunctionToolCall(block));
			continue;
		}
		if (block.type === "thinking" && block.signature) {
			const compactionContent = createCompactionContent(block);
			if (compactionContent) {
				flushPendingContent(pendingContent, items, {
					role: "assistant",
					phase: assistantPhase
				});
				items.push(compactionContent);
				continue;
			}
			if (block.signature.includes("@")) {
				flushPendingContent(pendingContent, items, {
					role: "assistant",
					phase: assistantPhase
				});
				items.push(createReasoningContent(block));
				continue;
			}
		}
		const converted = translateAssistantContentBlock(block);
		if (converted) pendingContent.push(converted);
	}
	flushPendingContent(pendingContent, items, {
		role: "assistant",
		phase: assistantPhase
	});
	return items;
};
const translateUserContentBlock = (block) => {
	switch (block.type) {
		case "text": return [createTextContent(block.text)];
		case "image": return [createImageContent(block)];
		case "document": return [createFileContent(block)];
		default: return [];
	}
};
const translateAssistantContentBlock = (block) => {
	switch (block.type) {
		case "text": return createOutPutTextContent(block.text);
		default: return;
	}
};
const flushPendingContent = (pendingContent, target, message) => {
	if (pendingContent.length === 0) return;
	const messageContent = [...pendingContent];
	target.push(createMessage(message.role, messageContent, message.phase));
	pendingContent.length = 0;
};
const createMessage = (role, content, phase) => ({
	type: MESSAGE_TYPE,
	role,
	content,
	...role === "assistant" && phase ? { phase } : {}
});
const resolveAssistantPhase = (_model, content, applyPhase) => {
	if (!applyPhase) return;
	if (typeof content === "string") return "final_answer";
	if (!Array.isArray(content)) return;
	if (!content.some((block) => block.type === "text")) return;
	return content.some((block) => block.type === "tool_use") ? "commentary" : "final_answer";
};
const shouldApplyPhase = (model) => {
	return getExtraPromptForModel(model).includes("## Intermediary updates");
};
const createTextContent = (text) => ({
	type: "input_text",
	text
});
const createOutPutTextContent = (text) => ({
	type: "output_text",
	text
});
const createImageContent = (block) => ({
	type: "input_image",
	image_url: `data:${block.source.media_type};base64,${block.source.data}`,
	detail: "auto"
});
const createFileContent = (block) => ({
	type: "input_file",
	file_data: `data:${block.source.media_type};base64,${block.source.data}`,
	filename: block.title ?? "document.pdf"
});
const createReasoningContent = (block) => {
	const { encryptedContent, id } = parseReasoningSignature(block.signature);
	const thinking = block.thinking === THINKING_TEXT$1 ? "" : block.thinking;
	return {
		id,
		type: "reasoning",
		summary: thinking ? [{
			type: "summary_text",
			text: thinking
		}] : [],
		encrypted_content: encryptedContent
	};
};
const createCompactionContent = (block) => {
	const compaction = decodeCompactionCarrierSignature(block.signature);
	if (!compaction) return;
	return {
		id: compaction.id,
		type: "compaction",
		encrypted_content: compaction.encrypted_content
	};
};
const parseReasoningSignature = (signature) => {
	const splitIndex = signature.lastIndexOf("@");
	if (splitIndex <= 0 || splitIndex === signature.length - 1) return {
		encryptedContent: signature,
		id: ""
	};
	return {
		encryptedContent: signature.slice(0, splitIndex),
		id: signature.slice(splitIndex + 1)
	};
};
const createFunctionToolCall = (block) => ({
	type: "function_call",
	call_id: block.id,
	name: block.name,
	arguments: JSON.stringify(block.input),
	status: "completed"
});
const createFunctionCallOutput = (block) => ({
	type: "function_call_output",
	call_id: block.tool_use_id,
	output: convertToolResultContent(block.content),
	status: block.is_error ? "incomplete" : "completed"
});
const translateSystemPrompt = (system, model) => {
	if (!system) return null;
	const extraPrompt = getExtraPromptForModel(model);
	if (typeof system === "string") return system + extraPrompt;
	const text = system.map((block, index) => {
		if (index === 0) return block.text + "\n\n" + extraPrompt + "\n\n";
		return block.text;
	}).join(" ");
	return text.length > 0 ? text : null;
};
const convertAnthropicTools = (tools) => {
	if (!tools || tools.length === 0) return null;
	return tools.map((tool) => {
		const serverType = tool.type;
		if (serverType && serverType.startsWith("web_search")) return { type: "web_search" };
		return {
			type: "function",
			name: tool.name,
			parameters: normalizeToolSchema(tool.input_schema),
			strict: false,
			...tool.description ? { description: tool.description } : {}
		};
	});
};
const convertAnthropicToolChoice = (choice) => {
	if (!choice) return "auto";
	switch (choice.type) {
		case "auto": return "auto";
		case "any": return "required";
		case "tool": return choice.name ? {
			type: "function",
			name: choice.name
		} : "auto";
		case "none": return "none";
		default: return "auto";
	}
};
const translateResponsesResultToAnthropic = (response) => {
	const contentBlocks = mapOutputToAnthropicContent(response.output);
	const usage = mapResponsesUsage(response);
	let anthropicContent = fallbackContentBlocks(response.output_text);
	if (contentBlocks.length > 0) anthropicContent = contentBlocks;
	const stopReason = mapResponsesStopReason(response);
	return {
		id: response.id,
		type: "message",
		role: "assistant",
		content: anthropicContent,
		model: response.model,
		stop_reason: stopReason,
		stop_sequence: null,
		usage
	};
};
const mapOutputToAnthropicContent = (output) => {
	const contentBlocks = [];
	for (const item of output) switch (item.type) {
		case "reasoning": {
			const thinkingText = extractReasoningText(item);
			if (thinkingText.length > 0) contentBlocks.push({
				type: "thinking",
				thinking: thinkingText,
				signature: (item.encrypted_content ?? "") + "@" + item.id
			});
			break;
		}
		case "function_call": {
			const toolUseBlock = createToolUseContentBlock(item);
			if (toolUseBlock) contentBlocks.push(toolUseBlock);
			break;
		}
		case "message": {
			const combinedText = combineMessageTextContent(item.content);
			if (combinedText.length > 0) contentBlocks.push({
				type: "text",
				text: combinedText
			});
			break;
		}
		case "compaction": {
			const compactionBlock = createCompactionThinkingBlock(item);
			if (compactionBlock) contentBlocks.push(compactionBlock);
			break;
		}
		default: {
			const combinedText = combineMessageTextContent(item.content);
			if (combinedText.length > 0) contentBlocks.push({
				type: "text",
				text: combinedText
			});
		}
	}
	return contentBlocks;
};
const combineMessageTextContent = (content) => {
	if (!Array.isArray(content)) return "";
	let aggregated = "";
	for (const block of content) {
		if (isResponseOutputText(block)) {
			aggregated += block.text;
			continue;
		}
		if (isResponseOutputRefusal(block)) {
			aggregated += block.refusal;
			continue;
		}
		if (typeof block.text === "string") {
			aggregated += block.text;
			continue;
		}
		if (typeof block.reasoning === "string") {
			aggregated += block.reasoning;
			continue;
		}
	}
	return aggregated;
};
const extractReasoningText = (item) => {
	const segments = [];
	const collectFromBlocks = (blocks) => {
		if (!Array.isArray(blocks)) return;
		for (const block of blocks) if (typeof block.text === "string") {
			segments.push(block.text);
			continue;
		}
	};
	if (!item.summary || item.summary.length === 0) return THINKING_TEXT$1;
	collectFromBlocks(item.summary);
	return segments.join("").trim();
};
const createToolUseContentBlock = (call) => {
	const toolId = call.call_id;
	if (!call.name || !toolId) return null;
	const input = parseFunctionCallArguments(call.arguments);
	return {
		type: "tool_use",
		id: toolId,
		name: call.name,
		input
	};
};
const createCompactionThinkingBlock = (item) => {
	if (!item.id || !item.encrypted_content) return null;
	return {
		type: "thinking",
		thinking: THINKING_TEXT$1,
		signature: encodeCompactionCarrierSignature({
			id: item.id,
			encrypted_content: item.encrypted_content
		})
	};
};
const parseFunctionCallArguments = (rawArguments) => {
	if (typeof rawArguments !== "string" || rawArguments.trim().length === 0) return {};
	try {
		const parsed = JSON.parse(rawArguments);
		if (Array.isArray(parsed)) return { arguments: parsed };
		if (parsed && typeof parsed === "object") return parsed;
	} catch (error) {
		consola.warn("Failed to parse function call arguments", {
			error,
			rawArguments
		});
	}
	return { raw_arguments: rawArguments };
};
const fallbackContentBlocks = (outputText) => {
	if (!outputText) return [];
	return [{
		type: "text",
		text: outputText
	}];
};
const mapResponsesStopReason = (response) => {
	const { status, incomplete_details: incompleteDetails } = response;
	if (status === "completed") {
		if (response.output.some((item) => item.type === "function_call")) return "tool_use";
		return "end_turn";
	}
	if (status === "incomplete") {
		if (incompleteDetails?.reason === "max_output_tokens") return "max_tokens";
		if (incompleteDetails?.reason === "content_filter") return "end_turn";
	}
	return null;
};
const mapResponsesUsage = (response) => {
	const inputTokens = response.usage?.input_tokens ?? 0;
	const outputTokens = response.usage?.output_tokens ?? 0;
	const inputCachedTokens = response.usage?.input_tokens_details?.cached_tokens;
	return {
		input_tokens: inputTokens - (inputCachedTokens ?? 0),
		output_tokens: outputTokens,
		...response.usage?.input_tokens_details?.cached_tokens !== void 0 && { cache_read_input_tokens: response.usage.input_tokens_details.cached_tokens }
	};
};
const isRecord = (value) => typeof value === "object" && value !== null;
const isResponseOutputText = (block) => isRecord(block) && "type" in block && block.type === "output_text";
const isResponseOutputRefusal = (block) => isRecord(block) && "type" in block && block.type === "refusal";
const convertToolResultContent = (content) => {
	if (typeof content === "string") return content;
	if (Array.isArray(content)) {
		const result = [];
		for (const block of content) switch (block.type) {
			case "text":
				result.push(createTextContent(block.text));
				break;
			case "image":
				result.push(createImageContent(block));
				break;
			case "document":
				result.push(createFileContent(block));
				break;
			case "tool_reference":
				result.push(createTextContent(`Tool ${block.tool_name} loaded`));
				break;
			default: break;
		}
		return result;
	}
	return "";
};

//#endregion
//#region src/routes/messages/responses-stream-translation.ts
const MAX_CONSECUTIVE_FUNCTION_CALL_WHITESPACE = 20;
var FunctionCallArgumentsValidationError = class extends Error {
	constructor(message) {
		super(message);
		this.name = "FunctionCallArgumentsValidationError";
	}
};
const updateWhitespaceRunState = (previousCount, chunk) => {
	let count = previousCount;
	for (const char of chunk) {
		if (char === "\r" || char === "\n" || char === "	") {
			count += 1;
			if (count > MAX_CONSECUTIVE_FUNCTION_CALL_WHITESPACE) return {
				nextCount: count,
				exceeded: true
			};
			continue;
		}
		if (char !== " ") count = 0;
	}
	return {
		nextCount: count,
		exceeded: false
	};
};
const createResponsesStreamState = () => ({
	messageStartSent: false,
	messageCompleted: false,
	nextContentBlockIndex: 0,
	blockIndexByKey: /* @__PURE__ */ new Map(),
	openBlocks: /* @__PURE__ */ new Set(),
	blockHasDelta: /* @__PURE__ */ new Set(),
	functionCallStateByOutputIndex: /* @__PURE__ */ new Map()
});
const translateResponsesStreamEvent = (rawEvent, state$1) => {
	switch (rawEvent.type) {
		case "response.created": return handleResponseCreated(rawEvent, state$1);
		case "response.output_item.added": return handleOutputItemAdded$1(rawEvent, state$1);
		case "response.reasoning_summary_text.delta": return handleReasoningSummaryTextDelta(rawEvent, state$1);
		case "response.output_text.delta": return handleOutputTextDelta(rawEvent, state$1);
		case "response.reasoning_summary_text.done": return handleReasoningSummaryTextDone(rawEvent, state$1);
		case "response.output_text.done": return handleOutputTextDone(rawEvent, state$1);
		case "response.output_item.done": return handleOutputItemDone$1(rawEvent, state$1);
		case "response.function_call_arguments.delta": return handleFunctionCallArgumentsDelta(rawEvent, state$1);
		case "response.function_call_arguments.done": return handleFunctionCallArgumentsDone(rawEvent, state$1);
		case "response.completed":
		case "response.incomplete": return handleResponseCompleted(rawEvent, state$1);
		case "response.failed": return handleResponseFailed(rawEvent, state$1);
		case "error": return handleErrorEvent(rawEvent, state$1);
		default: return [];
	}
};
const handleResponseCreated = (rawEvent, state$1) => {
	return messageStart(state$1, rawEvent.response);
};
const handleOutputItemAdded$1 = (rawEvent, state$1) => {
	const events$1 = new Array();
	const functionCallDetails = extractFunctionCallDetails(rawEvent);
	if (!functionCallDetails) return events$1;
	const { outputIndex, toolCallId, name, initialArguments } = functionCallDetails;
	const blockIndex = openFunctionCallBlock(state$1, {
		outputIndex,
		toolCallId,
		name,
		events: events$1
	});
	if (initialArguments !== void 0 && initialArguments.length > 0) {
		events$1.push({
			type: "content_block_delta",
			index: blockIndex,
			delta: {
				type: "input_json_delta",
				partial_json: initialArguments
			}
		});
		state$1.blockHasDelta.add(blockIndex);
	}
	return events$1;
};
const handleOutputItemDone$1 = (rawEvent, state$1) => {
	const events$1 = new Array();
	const item = rawEvent.item;
	const itemType = item.type;
	const outputIndex = rawEvent.output_index;
	if (itemType === "compaction") {
		if (!item.id || !item.encrypted_content) return events$1;
		const blockIndex$1 = openThinkingBlockIfNeeded(state$1, outputIndex, events$1);
		if (!state$1.blockHasDelta.has(blockIndex$1)) events$1.push({
			type: "content_block_delta",
			index: blockIndex$1,
			delta: {
				type: "thinking_delta",
				thinking: THINKING_TEXT$1
			}
		});
		events$1.push({
			type: "content_block_delta",
			index: blockIndex$1,
			delta: {
				type: "signature_delta",
				signature: encodeCompactionCarrierSignature({
					id: item.id,
					encrypted_content: item.encrypted_content
				})
			}
		});
		state$1.blockHasDelta.add(blockIndex$1);
		return events$1;
	}
	if (itemType !== "reasoning") return events$1;
	const blockIndex = openThinkingBlockIfNeeded(state$1, outputIndex, events$1);
	const signature = (item.encrypted_content ?? "") + "@" + item.id;
	if (signature) {
		if (!item.summary || item.summary.length === 0) events$1.push({
			type: "content_block_delta",
			index: blockIndex,
			delta: {
				type: "thinking_delta",
				thinking: THINKING_TEXT$1
			}
		});
		events$1.push({
			type: "content_block_delta",
			index: blockIndex,
			delta: {
				type: "signature_delta",
				signature
			}
		});
		state$1.blockHasDelta.add(blockIndex);
	}
	return events$1;
};
const handleFunctionCallArgumentsDelta = (rawEvent, state$1) => {
	const events$1 = new Array();
	const outputIndex = rawEvent.output_index;
	const deltaText = rawEvent.delta;
	if (!deltaText) return events$1;
	const blockIndex = openFunctionCallBlock(state$1, {
		outputIndex,
		events: events$1
	});
	const functionCallState = state$1.functionCallStateByOutputIndex.get(outputIndex);
	if (!functionCallState) return handleFunctionCallArgumentsValidationError(new FunctionCallArgumentsValidationError("Received function call arguments delta without an open tool call block."), state$1, events$1);
	const { nextCount, exceeded } = updateWhitespaceRunState(functionCallState.consecutiveWhitespaceCount, deltaText);
	if (exceeded) return handleFunctionCallArgumentsValidationError(new FunctionCallArgumentsValidationError("Received function call arguments delta containing more than 20 consecutive whitespace characters."), state$1, events$1);
	functionCallState.consecutiveWhitespaceCount = nextCount;
	events$1.push({
		type: "content_block_delta",
		index: blockIndex,
		delta: {
			type: "input_json_delta",
			partial_json: deltaText
		}
	});
	state$1.blockHasDelta.add(blockIndex);
	return events$1;
};
const handleFunctionCallArgumentsDone = (rawEvent, state$1) => {
	const events$1 = new Array();
	const outputIndex = rawEvent.output_index;
	const blockIndex = openFunctionCallBlock(state$1, {
		outputIndex,
		events: events$1
	});
	const finalArguments = typeof rawEvent.arguments === "string" ? rawEvent.arguments : void 0;
	if (!state$1.blockHasDelta.has(blockIndex) && finalArguments) {
		events$1.push({
			type: "content_block_delta",
			index: blockIndex,
			delta: {
				type: "input_json_delta",
				partial_json: finalArguments
			}
		});
		state$1.blockHasDelta.add(blockIndex);
	}
	state$1.functionCallStateByOutputIndex.delete(outputIndex);
	return events$1;
};
const handleOutputTextDelta = (rawEvent, state$1) => {
	const events$1 = new Array();
	const outputIndex = rawEvent.output_index;
	const contentIndex = rawEvent.content_index;
	const deltaText = rawEvent.delta;
	if (!deltaText) return events$1;
	const blockIndex = openTextBlockIfNeeded(state$1, {
		outputIndex,
		contentIndex,
		events: events$1
	});
	events$1.push({
		type: "content_block_delta",
		index: blockIndex,
		delta: {
			type: "text_delta",
			text: deltaText
		}
	});
	state$1.blockHasDelta.add(blockIndex);
	return events$1;
};
const handleReasoningSummaryTextDelta = (rawEvent, state$1) => {
	const outputIndex = rawEvent.output_index;
	const deltaText = rawEvent.delta;
	const events$1 = new Array();
	const blockIndex = openThinkingBlockIfNeeded(state$1, outputIndex, events$1);
	events$1.push({
		type: "content_block_delta",
		index: blockIndex,
		delta: {
			type: "thinking_delta",
			thinking: deltaText
		}
	});
	state$1.blockHasDelta.add(blockIndex);
	return events$1;
};
const handleReasoningSummaryTextDone = (rawEvent, state$1) => {
	const outputIndex = rawEvent.output_index;
	const text = rawEvent.text;
	const events$1 = new Array();
	const blockIndex = openThinkingBlockIfNeeded(state$1, outputIndex, events$1);
	if (text && !state$1.blockHasDelta.has(blockIndex)) events$1.push({
		type: "content_block_delta",
		index: blockIndex,
		delta: {
			type: "thinking_delta",
			thinking: text
		}
	});
	return events$1;
};
const handleOutputTextDone = (rawEvent, state$1) => {
	const events$1 = new Array();
	const outputIndex = rawEvent.output_index;
	const contentIndex = rawEvent.content_index;
	const text = rawEvent.text;
	const blockIndex = openTextBlockIfNeeded(state$1, {
		outputIndex,
		contentIndex,
		events: events$1
	});
	if (text && !state$1.blockHasDelta.has(blockIndex)) events$1.push({
		type: "content_block_delta",
		index: blockIndex,
		delta: {
			type: "text_delta",
			text
		}
	});
	return events$1;
};
const handleResponseCompleted = (rawEvent, state$1) => {
	const response = rawEvent.response;
	const events$1 = new Array();
	closeAllOpenBlocks(state$1, events$1);
	const anthropic = translateResponsesResultToAnthropic(response);
	events$1.push({
		type: "message_delta",
		delta: {
			stop_reason: anthropic.stop_reason,
			stop_sequence: anthropic.stop_sequence
		},
		usage: anthropic.usage
	}, { type: "message_stop" });
	state$1.messageCompleted = true;
	return events$1;
};
const handleResponseFailed = (rawEvent, state$1) => {
	const response = rawEvent.response;
	const events$1 = new Array();
	closeAllOpenBlocks(state$1, events$1);
	const message = response.error?.message ?? "The response failed due to an unknown error.";
	events$1.push(buildErrorEvent(message));
	state$1.messageCompleted = true;
	return events$1;
};
const handleErrorEvent = (rawEvent, state$1) => {
	const message = typeof rawEvent.message === "string" ? rawEvent.message : "An unexpected error occurred during streaming.";
	state$1.messageCompleted = true;
	return [buildErrorEvent(message)];
};
const handleFunctionCallArgumentsValidationError = (error, state$1, events$1 = []) => {
	const reason = error.message;
	closeAllOpenBlocks(state$1, events$1);
	state$1.messageCompleted = true;
	events$1.push(buildErrorEvent(reason));
	return events$1;
};
const messageStart = (state$1, response) => {
	state$1.messageStartSent = true;
	const inputCachedTokens = response.usage?.input_tokens_details?.cached_tokens;
	const inputTokens = (response.usage?.input_tokens ?? 0) - (inputCachedTokens ?? 0);
	return [{
		type: "message_start",
		message: {
			id: response.id,
			type: "message",
			role: "assistant",
			content: [],
			model: response.model,
			stop_reason: null,
			stop_sequence: null,
			usage: {
				input_tokens: inputTokens,
				output_tokens: 0,
				cache_read_input_tokens: inputCachedTokens ?? 0
			}
		}
	}];
};
const openTextBlockIfNeeded = (state$1, params) => {
	const { outputIndex, contentIndex, events: events$1 } = params;
	const key = getBlockKey(outputIndex, contentIndex);
	let blockIndex = state$1.blockIndexByKey.get(key);
	if (blockIndex === void 0) {
		blockIndex = state$1.nextContentBlockIndex;
		state$1.nextContentBlockIndex += 1;
		state$1.blockIndexByKey.set(key, blockIndex);
	}
	if (!state$1.openBlocks.has(blockIndex)) {
		closeOpenBlocks(state$1, events$1);
		events$1.push({
			type: "content_block_start",
			index: blockIndex,
			content_block: {
				type: "text",
				text: ""
			}
		});
		state$1.openBlocks.add(blockIndex);
	}
	return blockIndex;
};
const openThinkingBlockIfNeeded = (state$1, outputIndex, events$1) => {
	const key = getBlockKey(outputIndex, 0);
	let blockIndex = state$1.blockIndexByKey.get(key);
	if (blockIndex === void 0) {
		blockIndex = state$1.nextContentBlockIndex;
		state$1.nextContentBlockIndex += 1;
		state$1.blockIndexByKey.set(key, blockIndex);
	}
	if (!state$1.openBlocks.has(blockIndex)) {
		closeOpenBlocks(state$1, events$1);
		events$1.push({
			type: "content_block_start",
			index: blockIndex,
			content_block: {
				type: "thinking",
				thinking: ""
			}
		});
		state$1.openBlocks.add(blockIndex);
	}
	return blockIndex;
};
const closeBlockIfOpen = (state$1, blockIndex, events$1) => {
	if (!state$1.openBlocks.has(blockIndex)) return;
	events$1.push({
		type: "content_block_stop",
		index: blockIndex
	});
	state$1.openBlocks.delete(blockIndex);
	state$1.blockHasDelta.delete(blockIndex);
};
const closeOpenBlocks = (state$1, events$1) => {
	for (const blockIndex of state$1.openBlocks) closeBlockIfOpen(state$1, blockIndex, events$1);
};
const closeAllOpenBlocks = (state$1, events$1) => {
	closeOpenBlocks(state$1, events$1);
	state$1.functionCallStateByOutputIndex.clear();
};
const buildErrorEvent = (message) => ({
	type: "error",
	error: {
		type: "api_error",
		message
	}
});
const getBlockKey = (outputIndex, contentIndex) => `${outputIndex}:${contentIndex}`;
const openFunctionCallBlock = (state$1, params) => {
	const { outputIndex, toolCallId, name, events: events$1 } = params;
	let functionCallState = state$1.functionCallStateByOutputIndex.get(outputIndex);
	if (!functionCallState) {
		const blockIndex$1 = state$1.nextContentBlockIndex;
		state$1.nextContentBlockIndex += 1;
		const resolvedToolCallId = toolCallId ?? `tool_call_${blockIndex$1}`;
		functionCallState = {
			blockIndex: blockIndex$1,
			toolCallId: resolvedToolCallId,
			name: name ?? "function",
			consecutiveWhitespaceCount: 0
		};
		state$1.functionCallStateByOutputIndex.set(outputIndex, functionCallState);
	}
	const { blockIndex } = functionCallState;
	if (!state$1.openBlocks.has(blockIndex)) {
		closeOpenBlocks(state$1, events$1);
		events$1.push({
			type: "content_block_start",
			index: blockIndex,
			content_block: {
				type: "tool_use",
				id: functionCallState.toolCallId,
				name: functionCallState.name,
				input: {}
			}
		});
		state$1.openBlocks.add(blockIndex);
	}
	return blockIndex;
};
const extractFunctionCallDetails = (rawEvent) => {
	const item = rawEvent.item;
	if (item.type !== "function_call") return;
	const outputIndex = rawEvent.output_index;
	const toolCallId = item.call_id;
	const name = item.name;
	const initialArguments = item.arguments;
	return {
		outputIndex,
		toolCallId,
		name,
		initialArguments
	};
};

//#endregion
//#region src/routes/responses/utils.ts
const getResponsesRequestOptions = (payload) => {
	const vision = hasVisionInput(payload);
	const initiator = hasAgentInitiator(payload) ? "agent" : "user";
	return {
		vision,
		initiator
	};
};
const hasAgentInitiator = (payload) => {
	const lastItem = getPayloadItems(payload).at(-1);
	if (!lastItem) return false;
	if (!("role" in lastItem) || !lastItem.role) return true;
	return (typeof lastItem.role === "string" ? lastItem.role.toLowerCase() : "") === "assistant";
};
const hasVisionInput = (payload) => {
	return getPayloadItems(payload).some((item) => containsVisionContent(item));
};
const resolveResponsesCompactThreshold = (maxPromptTokens) => {
	if (typeof maxPromptTokens === "number" && maxPromptTokens > 0) return Math.floor(maxPromptTokens * .9);
	return 5e4;
};
const createCompactionContextManagement = (compactThreshold) => [{
	type: "compaction",
	compact_threshold: compactThreshold
}];
const applyResponsesApiContextManagement = (payload, maxPromptTokens) => {
	if (payload.context_management !== void 0) return;
	if (!isResponsesApiContextManagementModel(payload.model)) return;
	payload.context_management = createCompactionContextManagement(resolveResponsesCompactThreshold(maxPromptTokens));
};
const compactInputByLatestCompaction = (payload) => {
	if (!Array.isArray(payload.input) || payload.input.length === 0) return;
	const latestCompactionMessageIndex = getLatestCompactionMessageIndex(payload.input);
	if (latestCompactionMessageIndex === void 0) return;
	payload.input = payload.input.slice(latestCompactionMessageIndex);
};
const getLatestCompactionMessageIndex = (input) => {
	for (let index = input.length - 1; index >= 0; index -= 1) if (isCompactionInputItem(input[index])) return index;
};
const isCompactionInputItem = (value) => {
	return "type" in value && typeof value.type === "string" && value.type === "compaction";
};
const getPayloadItems = (payload) => {
	const result = [];
	const { input } = payload;
	if (Array.isArray(input)) result.push(...input);
	return result;
};
const containsVisionContent = (value) => {
	if (!value) return false;
	if (Array.isArray(value)) return value.some((entry) => containsVisionContent(entry));
	if (typeof value !== "object") return false;
	const record = value;
	if ((typeof record.type === "string" ? record.type.toLowerCase() : void 0) === "input_image") return true;
	if (Array.isArray(record.content)) return record.content.some((entry) => containsVisionContent(entry));
	return false;
};

//#endregion
//#region src/routes/messages/preprocess.ts
const TOOL_REFERENCE_TURN_BOUNDARY = "Tool loaded.";
const IDE_EXECUTE_CODE_TOOL = "mcp__ide__executeCode";
const IDE_GET_DIAGNOSTICS_TOOL = "mcp__ide__getDiagnostics";
const IDE_GET_DIAGNOSTICS_DESCRIPTION = "Get language diagnostics from VS Code. Returns errors, warnings, information, and hints for files in the workspace.";
const PDF_FILE_READ_PREFIX = "PDF file read:";
const getCompactCandidateText = (message) => {
	if (message.role !== "user") return "";
	if (typeof message.content === "string") return message.content;
	return message.content.filter((block) => block.type === "text").map((block) => block.text.startsWith("<system-reminder>") ? "" : block.text).filter((text) => text.length > 0).join("\n\n");
};
const isCompactMessage = (lastMessage) => {
	const text = getCompactCandidateText(lastMessage);
	if (!text) return false;
	return text.includes(compactTextOnlyGuard) && text.includes(compactSummaryPromptStart) && compactMessageSections.some((section) => text.includes(section));
};
const isCompactAutoContinueMessage = (lastMessage) => {
	const text = getCompactCandidateText(lastMessage);
	return Boolean(text) && compactAutoContinuePromptStarts.some((promptStart) => text.startsWith(promptStart));
};
const getCompactType = (anthropicPayload) => {
	const lastMessage = anthropicPayload.messages.at(-1);
	if (lastMessage && isCompactMessage(lastMessage)) return COMPACT_REQUEST;
	if (lastMessage && isCompactAutoContinueMessage(lastMessage)) return COMPACT_AUTO_CONTINUE;
	const system = anthropicPayload.system;
	if (typeof system === "string") return compactSystemPromptStarts.some((promptStart) => system.startsWith(promptStart)) ? COMPACT_REQUEST : 0;
	if (!Array.isArray(system)) return 0;
	if (system.some((msg) => typeof msg.text === "string" && compactSystemPromptStarts.some((promptStart) => msg.text.startsWith(promptStart)))) return COMPACT_REQUEST;
	return 0;
};
const mergeContentWithText = (tr, textBlock) => {
	if (typeof tr.content === "string") return {
		...tr,
		content: `${tr.content}\n\n${textBlock.text}`
	};
	if (hasToolRef(tr)) return tr;
	return {
		...tr,
		content: [...tr.content, textBlock]
	};
};
const mergeContentWithTexts = (tr, textBlocks) => {
	if (typeof tr.content === "string") {
		const appendedTexts = textBlocks.map((tb) => tb.text).join("\n\n");
		return {
			...tr,
			content: `${tr.content}\n\n${appendedTexts}`
		};
	}
	if (hasToolRef(tr)) return tr;
	return {
		...tr,
		content: [...tr.content, ...textBlocks]
	};
};
const mergeContentWithAttachments = (tr, attachments) => {
	if (typeof tr.content === "string") return {
		...tr,
		content: [{
			type: "text",
			text: tr.content
		}, ...attachments]
	};
	return {
		...tr,
		content: [...tr.content, ...attachments]
	};
};
const isAttachmentBlock = (block) => {
	return block.type === "image" || block.type === "document";
};
const getMergeableToolResultIndices = (toolResults) => {
	return toolResults.flatMap((block, index) => block.is_error || hasToolRef(block) ? [] : [index]);
};
const mergeAttachmentsIntoToolResults = (toolResults, attachmentsByToolResultIndex) => {
	if (attachmentsByToolResultIndex.size === 0) return toolResults;
	return toolResults.map((block, index) => {
		const matchedAttachments = attachmentsByToolResultIndex.get(index);
		if (!matchedAttachments) return block;
		const orderedAttachments = [...matchedAttachments].sort((left, right) => left.order - right.order).map(({ attachment }) => attachment);
		return mergeContentWithAttachments(block, orderedAttachments);
	});
};
const assignAttachmentsToToolResults = (target, attachments, options) => {
	const { toolResultIndices } = options;
	const fallbackToolResultIndices = options.fallbackToolResultIndices ?? toolResultIndices;
	if (attachments.length === 0) return;
	if (toolResultIndices.length > 0 && toolResultIndices.length === attachments.length) {
		for (const [index, toolResultIndex] of toolResultIndices.entries()) {
			const currentAttachments$1 = target.get(toolResultIndex);
			if (currentAttachments$1) {
				currentAttachments$1.push(attachments[index]);
				continue;
			}
			target.set(toolResultIndex, [attachments[index]]);
		}
		return;
	}
	const lastToolResultIndex = fallbackToolResultIndices.at(-1);
	if (lastToolResultIndex === void 0) return;
	const currentAttachments = target.get(lastToolResultIndex);
	if (currentAttachments) {
		currentAttachments.push(...attachments);
		return;
	}
	target.set(lastToolResultIndex, [...attachments]);
};
const startsWithPdfFileRead = (toolResult) => {
	if (typeof toolResult.content === "string") return toolResult.content.startsWith(PDF_FILE_READ_PREFIX);
	if (toolResult.content.some((block) => block.type === "document")) return false;
	if (toolResult.content.length === 0) return false;
	const firstBlock = toolResult.content[0];
	if (firstBlock.type !== "text") return false;
	return firstBlock.text.startsWith(PDF_FILE_READ_PREFIX);
};
const collectMergeableUserContent = (content) => {
	const toolResults = [];
	const textBlocks = [];
	const attachments = [];
	for (const [order, block] of content.entries()) {
		if (block.type === "tool_result") {
			toolResults.push(block);
			continue;
		}
		if (block.type === "text") {
			textBlocks.push(block);
			continue;
		}
		if (isAttachmentBlock(block)) {
			attachments.push({
				attachment: block,
				order
			});
			continue;
		}
		return null;
	}
	return {
		toolResults,
		textBlocks,
		attachments
	};
};
const mergeAttachmentsForToolResults = (toolResults, attachments) => {
	if (attachments.length === 0) return toolResults;
	const documentBlocks = attachments.filter(({ attachment }) => attachment.type === "document");
	const mergeableToolResultIndices = getMergeableToolResultIndices(toolResults);
	const pdfReadToolResultIndices = mergeableToolResultIndices.filter((index) => startsWithPdfFileRead(toolResults[index]));
	const attachmentsByToolResultIndex = /* @__PURE__ */ new Map();
	let remainingAttachments = attachments;
	let countMatchToolResultIndices = mergeableToolResultIndices;
	if (documentBlocks.length > 0 && pdfReadToolResultIndices.length > 0) {
		const matchedDocumentCount = Math.min(pdfReadToolResultIndices.length, documentBlocks.length);
		const matchedDocuments = documentBlocks.slice(0, matchedDocumentCount);
		const matchedDocumentOrders = new Set(matchedDocuments.map(({ order }) => order));
		const matchedPdfToolResultIndices = pdfReadToolResultIndices.slice(0, matchedDocumentCount);
		const matchedPdfToolResultIndexSet = new Set(matchedPdfToolResultIndices);
		assignAttachmentsToToolResults(attachmentsByToolResultIndex, matchedDocuments, { toolResultIndices: matchedPdfToolResultIndices });
		countMatchToolResultIndices = mergeableToolResultIndices.filter((index) => !matchedPdfToolResultIndexSet.has(index));
		remainingAttachments = attachments.filter(({ attachment, order }) => attachment.type !== "document" || !matchedDocumentOrders.has(order));
	}
	assignAttachmentsToToolResults(attachmentsByToolResultIndex, remainingAttachments, {
		toolResultIndices: countMatchToolResultIndices,
		fallbackToolResultIndices: mergeableToolResultIndices
	});
	return mergeAttachmentsIntoToolResults(toolResults, attachmentsByToolResultIndex);
};
const mergeUserMessageContent = (content) => {
	const mergeableContent = collectMergeableUserContent(content);
	if (!mergeableContent) return null;
	const { toolResults, textBlocks, attachments } = mergeableContent;
	if (toolResults.length === 0 || textBlocks.length === 0 && attachments.length === 0) return null;
	const mergedToolResults = textBlocks.length === 0 ? toolResults : mergeToolResult(toolResults, textBlocks);
	return mergeAttachmentsForToolResults(mergedToolResults, attachments);
};
const mergeToolResult = (toolResults, textBlocks) => {
	if (toolResults.length === textBlocks.length) return toolResults.map((tr, i) => mergeContentWithText(tr, textBlocks[i]));
	const lastIndex = toolResults.length - 1;
	return toolResults.map((tr, i) => i === lastIndex ? mergeContentWithTexts(tr, textBlocks) : tr);
};
const stripToolReferenceTurnBoundary = (anthropicPayload) => {
	for (const msg of anthropicPayload.messages) {
		if (msg.role !== "user" || !Array.isArray(msg.content)) continue;
		if (!msg.content.some((block) => block.type === "tool_result" && hasToolRef(block))) continue;
		msg.content = msg.content.filter((block) => block.type !== "text" || block.text.trim() !== TOOL_REFERENCE_TURN_BOUNDARY);
	}
};
const mergeToolResultForClaude = (anthropicPayload, options) => {
	const lastMessageIndex = anthropicPayload.messages.length - 1;
	for (const [index, msg] of anthropicPayload.messages.entries()) {
		if (options?.skipLastMessage && index === lastMessageIndex) continue;
		if (msg.role !== "user" || !Array.isArray(msg.content)) continue;
		const mergedContent = mergeUserMessageContent(msg.content);
		if (mergedContent) msg.content = mergedContent;
	}
};
const sanitizeIdeTools = (payload) => {
	if (!payload.tools || payload.tools.length === 0) return;
	payload.tools = payload.tools.flatMap((tool) => {
		if (tool.name === IDE_EXECUTE_CODE_TOOL && !tool.defer_loading) return [];
		if (tool.name === IDE_GET_DIAGNOSTICS_TOOL) return [{
			...tool,
			description: IDE_GET_DIAGNOSTICS_DESCRIPTION
		}];
		return [tool];
	});
};
const hasToolRef = (block) => {
	return Array.isArray(block.content) && block.content.some((c) => c.type === "tool_reference");
};
const stripCacheControl = (payload) => {
	if (Array.isArray(payload.system)) for (const block of payload.system) {
		const systemBlock = block;
		const cacheControl = systemBlock.cache_control;
		if (cacheControl && typeof cacheControl === "object") {
			const { scope,...rest } = cacheControl;
			systemBlock.cache_control = rest;
		}
	}
};
const filterAssistantThinkingBlocks = (payload) => {
	for (const msg of payload.messages) if (msg.role === "assistant" && Array.isArray(msg.content)) msg.content = msg.content.filter((block) => {
		if (block.type !== "thinking") return true;
		return block.thinking && block.thinking !== "Thinking..." && block.signature && !block.signature.includes("@");
	});
};
const prepareMessagesApiPayload = (payload, selectedModel) => {
	stripCacheControl(payload);
	filterAssistantThinkingBlocks(payload);
	const hasThinking = Boolean(payload.thinking);
	const toolChoice = payload.tool_choice;
	const disableThink = toolChoice?.type === "any" || toolChoice?.type === "tool";
	if (selectedModel?.capabilities.supports.adaptive_thinking && !disableThink) {
		payload.thinking = { type: "adaptive" };
		if (!hasThinking) payload.thinking.display = "summarized";
		if (payload.model === "claude-opus-4.7") payload.thinking.display = "summarized";
		let effort = getReasoningEffortForModel(payload.model);
		if (effort === "none" || effort === "minimal") effort = "low";
		const reasoningEffort = selectedModel.capabilities.supports.reasoning_effort;
		if (reasoningEffort && !reasoningEffort.includes(effort)) effort = reasoningEffort.at(-1);
		payload.output_config = { effort };
	}
};

//#endregion
//#region src/routes/messages/stream-translation.ts
function isToolBlockOpen(state$1) {
	if (!state$1.contentBlockOpen) return false;
	return Object.values(state$1.toolCalls).some((tc) => tc.anthropicBlockIndex === state$1.contentBlockIndex);
}
function translateChunkToAnthropicEvents(chunk, state$1) {
	const events$1 = [];
	if (chunk.choices.length === 0) return events$1;
	const choice = chunk.choices[0];
	const { delta } = choice;
	handleMessageStart(state$1, events$1, chunk);
	handleThinkingText(delta, state$1, events$1);
	handleContent(delta, state$1, events$1);
	handleToolCalls(delta, state$1, events$1);
	handleFinish(choice, state$1, {
		events: events$1,
		chunk
	});
	return events$1;
}
function handleFinish(choice, state$1, context) {
	const { events: events$1, chunk } = context;
	if (choice.finish_reason && choice.finish_reason.length > 0) {
		if (state$1.contentBlockOpen) {
			const toolBlockOpen = isToolBlockOpen(state$1);
			context.events.push({
				type: "content_block_stop",
				index: state$1.contentBlockIndex
			});
			state$1.contentBlockOpen = false;
			state$1.contentBlockIndex++;
			if (!toolBlockOpen) handleReasoningOpaque(choice.delta, events$1, state$1);
		}
		events$1.push({
			type: "message_delta",
			delta: {
				stop_reason: mapOpenAIStopReasonToAnthropic(choice.finish_reason),
				stop_sequence: null
			},
			usage: {
				input_tokens: (chunk.usage?.prompt_tokens ?? 0) - (chunk.usage?.prompt_tokens_details?.cached_tokens ?? 0),
				output_tokens: chunk.usage?.completion_tokens ?? 0,
				...chunk.usage?.prompt_tokens_details?.cached_tokens !== void 0 && { cache_read_input_tokens: chunk.usage.prompt_tokens_details.cached_tokens }
			}
		}, { type: "message_stop" });
	}
}
function handleToolCalls(delta, state$1, events$1) {
	if (delta.tool_calls && delta.tool_calls.length > 0) {
		closeThinkingBlockIfOpen(state$1, events$1);
		handleReasoningOpaqueInToolCalls(state$1, events$1, delta);
		for (const toolCall of delta.tool_calls) {
			if (toolCall.id && toolCall.function?.name) {
				if (state$1.contentBlockOpen) {
					events$1.push({
						type: "content_block_stop",
						index: state$1.contentBlockIndex
					});
					state$1.contentBlockIndex++;
					state$1.contentBlockOpen = false;
				}
				const anthropicBlockIndex = state$1.contentBlockIndex;
				state$1.toolCalls[toolCall.index] = {
					id: toolCall.id,
					name: toolCall.function.name,
					anthropicBlockIndex
				};
				events$1.push({
					type: "content_block_start",
					index: anthropicBlockIndex,
					content_block: {
						type: "tool_use",
						id: toolCall.id,
						name: toolCall.function.name,
						input: {}
					}
				});
				state$1.contentBlockOpen = true;
			}
			if (toolCall.function?.arguments) {
				const toolCallInfo = state$1.toolCalls[toolCall.index];
				if (toolCallInfo) events$1.push({
					type: "content_block_delta",
					index: toolCallInfo.anthropicBlockIndex,
					delta: {
						type: "input_json_delta",
						partial_json: toolCall.function.arguments
					}
				});
			}
		}
	}
}
function handleReasoningOpaqueInToolCalls(state$1, events$1, delta) {
	if (state$1.contentBlockOpen && !isToolBlockOpen(state$1)) {
		events$1.push({
			type: "content_block_stop",
			index: state$1.contentBlockIndex
		});
		state$1.contentBlockIndex++;
		state$1.contentBlockOpen = false;
	}
	handleReasoningOpaque(delta, events$1, state$1);
}
function handleContent(delta, state$1, events$1) {
	if (delta.content && delta.content.length > 0) {
		closeThinkingBlockIfOpen(state$1, events$1);
		if (isToolBlockOpen(state$1)) {
			events$1.push({
				type: "content_block_stop",
				index: state$1.contentBlockIndex
			});
			state$1.contentBlockIndex++;
			state$1.contentBlockOpen = false;
		}
		if (!state$1.contentBlockOpen) {
			events$1.push({
				type: "content_block_start",
				index: state$1.contentBlockIndex,
				content_block: {
					type: "text",
					text: ""
				}
			});
			state$1.contentBlockOpen = true;
		}
		events$1.push({
			type: "content_block_delta",
			index: state$1.contentBlockIndex,
			delta: {
				type: "text_delta",
				text: delta.content
			}
		});
	}
	if (delta.content === "" && delta.reasoning_opaque && delta.reasoning_opaque.length > 0 && state$1.thinkingBlockOpen) {
		events$1.push({
			type: "content_block_delta",
			index: state$1.contentBlockIndex,
			delta: {
				type: "signature_delta",
				signature: delta.reasoning_opaque
			}
		}, {
			type: "content_block_stop",
			index: state$1.contentBlockIndex
		});
		state$1.contentBlockIndex++;
		state$1.thinkingBlockOpen = false;
	}
}
function handleMessageStart(state$1, events$1, chunk) {
	if (!state$1.messageStartSent) {
		events$1.push({
			type: "message_start",
			message: {
				id: chunk.id,
				type: "message",
				role: "assistant",
				content: [],
				model: chunk.model,
				stop_reason: null,
				stop_sequence: null,
				usage: {
					input_tokens: (chunk.usage?.prompt_tokens ?? 0) - (chunk.usage?.prompt_tokens_details?.cached_tokens ?? 0),
					output_tokens: 0,
					...chunk.usage?.prompt_tokens_details?.cached_tokens !== void 0 && { cache_read_input_tokens: chunk.usage.prompt_tokens_details.cached_tokens }
				}
			}
		});
		state$1.messageStartSent = true;
	}
}
function handleReasoningOpaque(delta, events$1, state$1) {
	if (delta.reasoning_opaque && delta.reasoning_opaque.length > 0) {
		events$1.push({
			type: "content_block_start",
			index: state$1.contentBlockIndex,
			content_block: {
				type: "thinking",
				thinking: ""
			}
		}, {
			type: "content_block_delta",
			index: state$1.contentBlockIndex,
			delta: {
				type: "thinking_delta",
				thinking: THINKING_TEXT
			}
		}, {
			type: "content_block_delta",
			index: state$1.contentBlockIndex,
			delta: {
				type: "signature_delta",
				signature: delta.reasoning_opaque
			}
		}, {
			type: "content_block_stop",
			index: state$1.contentBlockIndex
		});
		state$1.contentBlockIndex++;
	}
}
function handleThinkingText(delta, state$1, events$1) {
	if (delta.reasoning_text && delta.reasoning_text.length > 0) {
		if (state$1.contentBlockOpen) {
			delta.content = delta.reasoning_text;
			delta.reasoning_text = void 0;
			return;
		}
		if (!state$1.thinkingBlockOpen) {
			events$1.push({
				type: "content_block_start",
				index: state$1.contentBlockIndex,
				content_block: {
					type: "thinking",
					thinking: ""
				}
			});
			state$1.thinkingBlockOpen = true;
		}
		events$1.push({
			type: "content_block_delta",
			index: state$1.contentBlockIndex,
			delta: {
				type: "thinking_delta",
				thinking: delta.reasoning_text
			}
		});
	}
}
function closeThinkingBlockIfOpen(state$1, events$1) {
	if (state$1.thinkingBlockOpen) {
		events$1.push({
			type: "content_block_delta",
			index: state$1.contentBlockIndex,
			delta: {
				type: "signature_delta",
				signature: ""
			}
		}, {
			type: "content_block_stop",
			index: state$1.contentBlockIndex
		});
		state$1.contentBlockIndex++;
		state$1.thinkingBlockOpen = false;
	}
}

//#endregion
//#region src/routes/messages/api-flows.ts
const handleWithChatCompletions = async (c, anthropicPayload, options) => {
	const { logger: logger$7, subagentMarker, requestId, sessionId, compactType } = options;
	const openAIPayload = translateToOpenAI(anthropicPayload);
	const recordUsage = createCopilotUsageRecorder({
		endpoint: "chat_completions",
		fallbackSessionId: sessionId,
		model: openAIPayload.model,
		payload: anthropicPayload
	});
	debugJson(logger$7, "Translated OpenAI request payload:", openAIPayload);
	const response = await createChatCompletions(openAIPayload, {
		subagentMarker,
		requestId,
		sessionId,
		compactType
	});
	if (isNonStreaming(response)) {
		debugJson(logger$7, "Non-streaming response from Copilot:", response);
		recordUsage(normalizeOpenAIUsage(response.usage));
		const anthropicResponse = translateToAnthropic(response);
		debugJson(logger$7, "Translated Anthropic response:", anthropicResponse);
		return c.json(anthropicResponse);
	}
	logger$7.debug("Streaming response from Copilot");
	return streamSSE(c, async (stream) => {
		let usage = {};
		const streamState = {
			messageStartSent: false,
			contentBlockIndex: 0,
			contentBlockOpen: false,
			toolCalls: {},
			thinkingBlockOpen: false
		};
		for await (const rawEvent of response) {
			debugJson(logger$7, "Copilot raw stream event:", rawEvent);
			if (rawEvent.data === "[DONE]") break;
			if (!rawEvent.data) continue;
			const chunk = JSON.parse(rawEvent.data);
			if (chunk.usage) usage = normalizeOpenAIUsage(chunk.usage);
			const events$1 = translateChunkToAnthropicEvents(chunk, streamState);
			for (const event of events$1) {
				const eventData = JSON.stringify(event);
				debugLazy(logger$7, () => ["Translated Anthropic event:", eventData]);
				await stream.writeSSE({
					event: event.type,
					data: eventData
				});
			}
		}
		recordUsage(usage);
	});
};
const handleWithResponsesApi = async (c, anthropicPayload, options) => {
	const { logger: logger$7, selectedModel,...requestOptions } = options;
	const responsesPayload = translateAnthropicMessagesToResponsesPayload(anthropicPayload);
	const recordUsage = createCopilotUsageRecorder({
		endpoint: "responses",
		fallbackSessionId: requestOptions.sessionId,
		model: responsesPayload.model,
		payload: anthropicPayload
	});
	applyResponsesApiContextManagement(responsesPayload, selectedModel?.capabilities.limits.max_prompt_tokens);
	compactInputByLatestCompaction(responsesPayload);
	debugJson(logger$7, "Translated Responses payload:", responsesPayload);
	const { vision, initiator } = getResponsesRequestOptions(responsesPayload);
	const response = await createResponses(responsesPayload, {
		vision,
		initiator,
		...requestOptions
	});
	if (responsesPayload.stream && isAsyncIterable$1(response)) {
		logger$7.debug("Streaming response from Copilot (Responses API)");
		return streamSSE(c, async (stream) => {
			const streamState = createResponsesStreamState();
			let usage = {};
			for await (const chunk of response) {
				if (chunk.event === "ping") {
					await stream.writeSSE({
						event: "ping",
						data: "{\"type\":\"ping\"}"
					});
					continue;
				}
				const data = chunk.data;
				if (!data) continue;
				debugLazy(logger$7, () => ["Responses raw stream event:", data]);
				const responseEvent = JSON.parse(data);
				if (responseEvent.type === "response.completed" || responseEvent.type === "response.failed" || responseEvent.type === "response.incomplete") usage = normalizeResponsesUsage(responseEvent.response.usage);
				const events$1 = translateResponsesStreamEvent(responseEvent, streamState);
				for (const event of events$1) {
					const eventData = JSON.stringify(event);
					debugLazy(logger$7, () => ["Translated Anthropic event:", eventData]);
					await stream.writeSSE({
						event: event.type,
						data: eventData
					});
				}
				if (streamState.messageCompleted) {
					logger$7.debug("Message completed, ending stream");
					break;
				}
			}
			if (!streamState.messageCompleted) {
				logger$7.warn("Responses stream ended without completion; sending error event");
				const errorEvent = buildErrorEvent("Responses stream ended without completion");
				await stream.writeSSE({
					event: errorEvent.type,
					data: JSON.stringify(errorEvent)
				});
			}
			recordUsage(usage);
		});
	}
	debugJsonTail(logger$7, "Non-streaming Responses result:", {
		value: response,
		tailLength: 400
	});
	const anthropicResponse = translateResponsesResultToAnthropic(response);
	recordUsage(normalizeResponsesUsage(response.usage));
	debugJson(logger$7, "Translated Anthropic response:", anthropicResponse);
	return c.json(anthropicResponse);
};
const handleWithMessagesApi = async (c, anthropicPayload, options) => {
	const { logger: logger$7, anthropicBetaHeader, subagentMarker, selectedModel, requestId, sessionId, compactType } = options;
	prepareMessagesApiPayload(anthropicPayload, selectedModel);
	const recordUsage = createCopilotUsageRecorder({
		endpoint: "messages",
		fallbackSessionId: sessionId,
		model: anthropicPayload.model,
		payload: anthropicPayload
	});
	debugJson(logger$7, "Translated Messages payload:", anthropicPayload);
	const response = await createMessages(anthropicPayload, anthropicBetaHeader, {
		subagentMarker,
		requestId,
		sessionId,
		compactType
	});
	if (isAsyncIterable$1(response)) {
		logger$7.debug("Streaming response from Copilot (Messages API)");
		return streamSSE(c, async (stream) => {
			let usage = {};
			for await (const event of response) {
				const eventName = event.event;
				const data = event.data ?? "";
				if (data === "[DONE]") break;
				if (!data) continue;
				debugLazy(logger$7, () => ["Messages raw stream event:", data]);
				const parsedEvent = parseAnthropicStreamEvent(data);
				if (parsedEvent?.type === "message_start") usage = mergeAnthropicUsage(usage, normalizeAnthropicUsage(parsedEvent.message.usage));
				else if (parsedEvent?.type === "message_delta") usage = mergeAnthropicUsage(usage, normalizeAnthropicUsage(parsedEvent.usage));
				await stream.writeSSE({
					event: eventName,
					data
				});
			}
			recordUsage(usage);
		});
	}
	debugJsonTail(logger$7, "Non-streaming Messages result:", {
		value: response,
		tailLength: 400
	});
	recordUsage(normalizeAnthropicUsage(response.usage));
	return c.json(response);
};
const isNonStreaming = (response) => Object.hasOwn(response, "choices");
const isAsyncIterable$1 = (value) => Boolean(value) && typeof value[Symbol.asyncIterator] === "function";
const createCopilotUsageRecorder = (options) => createCopilotTokenUsageRecorder({
	endpoint: options.endpoint,
	fallbackSessionId: options.fallbackSessionId,
	model: options.model,
	sessionId: getMetadataSessionId(options.payload)
});
const getMetadataSessionId = (payload) => parseUserIdMetadata(payload.metadata?.user_id).sessionId;
const parseAnthropicStreamEvent = (data) => {
	try {
		return JSON.parse(data);
	} catch {
		return null;
	}
};

//#endregion
//#region src/lib/subagent.ts
const subagentMarkerPrefix = "__SUBAGENT_MARKER__";

//#endregion
//#region src/routes/messages/subagent-marker.ts
const parseSubagentMarkerFromFirstUser = (payload) => {
	const firstUserMessage = payload.messages.find((msg) => msg.role === "user" && Array.isArray(msg.content));
	if (!firstUserMessage || !Array.isArray(firstUserMessage.content)) return null;
	for (const block of firstUserMessage.content) {
		if (block.type !== "text") continue;
		const marker = parseSubagentMarkerFromSystemReminder(block.text);
		if (marker) return marker;
	}
	return null;
};
const parseSubagentMarkerFromSystemReminder = (text) => {
	const startTag = "<system-reminder>";
	const endTag = "</system-reminder>";
	let searchFrom = 0;
	while (true) {
		const reminderStart = text.indexOf(startTag, searchFrom);
		if (reminderStart === -1) break;
		const contentStart = reminderStart + 17;
		const reminderEnd = text.indexOf(endTag, contentStart);
		if (reminderEnd === -1) break;
		const reminderContent = text.slice(contentStart, reminderEnd);
		const markerIndex = reminderContent.indexOf(subagentMarkerPrefix);
		if (markerIndex === -1) {
			searchFrom = reminderEnd + 18;
			continue;
		}
		const markerJson = reminderContent.slice(markerIndex + subagentMarkerPrefix.length).trim();
		try {
			const parsed = JSON.parse(markerJson);
			if (!parsed.session_id || !parsed.agent_id || !parsed.agent_type) {
				searchFrom = reminderEnd + 18;
				continue;
			}
			return parsed;
		} catch {
			searchFrom = reminderEnd + 18;
			continue;
		}
	}
	return null;
};

//#endregion
//#region src/routes/messages/web-search-shim.ts
const WEB_SEARCH_TOOL_NAME = "web_search";
const WEB_SEARCH_DESCRIPTION = "Search the web for up-to-date information. Provide a focused query string. Returns a textual summary of search results with source URLs.";
const WEB_SEARCH_INPUT_SCHEMA = {
	type: "object",
	properties: { query: {
		type: "string",
		description: "The search query."
	} },
	required: ["query"]
};
const MAX_TOOL_LOOP_ITERATIONS = 5;
/**
* Returns true if any tool in the payload is an Anthropic server-side
* web_search tool (e.g. `web_search_20250305`).
*/
const hasAnthropicWebSearch = (payload) => {
	if (!payload.tools || payload.tools.length === 0) return false;
	return payload.tools.some((t) => isAnthropicWebSearchTool(t));
};
const isAnthropicWebSearchTool = (tool) => {
	return (tool.type ?? "").startsWith("web_search");
};
/**
* Rewrites Anthropic server-side web_search tool entries into a normal
* function tool that Copilot's Anthropic transport accepts. Mutates payload.
*/
const rewriteWebSearchToolsToFunction = (payload) => {
	if (!payload.tools) return;
	payload.tools = payload.tools.map((tool) => {
		if (!isAnthropicWebSearchTool(tool)) return tool;
		return {
			name: WEB_SEARCH_TOOL_NAME,
			description: WEB_SEARCH_DESCRIPTION,
			input_schema: WEB_SEARCH_INPUT_SCHEMA
		};
	});
};
/**
* Inspects an assistant response for tool_use blocks targeting our
* shimmed web_search tool.
*/
const extractWebSearchCalls = (response) => {
	const calls = [];
	for (const block of response.content) if (block.type === "tool_use" && block.name === WEB_SEARCH_TOOL_NAME) calls.push(block);
	return calls;
};
/**
* Run a single web search by delegating to a Copilot Responses-API model
* with the native `{type: "web_search"}` server tool. Returns a plain-text
* summary suitable for handing back to Claude as a tool_result.
*/
const performWebSearchViaResponses = async (query, ctx) => {
	const searchModel = ctx.searchModel;
	if (!state.copilotToken) throw new Error("Copilot token not found");
	const result = await createResponses({
		model: searchModel,
		input: [{
			type: "message",
			role: "user",
			content: [{
				type: "input_text",
				text: query
			}]
		}],
		instructions: "You are a web search assistant. Use the web_search tool to find current information for the user's query. Then write a concise answer (a few short paragraphs) that synthesises the results, and include the most relevant source URLs inline.",
		tools: [{ type: "web_search" }],
		tool_choice: "auto",
		temperature: 1,
		max_output_tokens: 4096,
		stream: false,
		store: false,
		parallel_tool_calls: true,
		reasoning: {
			effort: "low",
			summary: "auto"
		}
	}, {
		vision: false,
		initiator: "agent",
		requestId: `${ctx.requestId}-websearch`,
		sessionId: ctx.sessionId
	});
	const summary = extractResponsesText(result);
	if (summary && summary.trim().length > 0) return summary;
	return `(Web search returned no synthesizable text for query: ${query})`;
};
const extractResponsesText = (result) => {
	if (!result || typeof result !== "object") return "";
	const r = result;
	if (typeof r.output_text === "string" && r.output_text.length > 0) return r.output_text;
	if (!Array.isArray(r.output)) return "";
	return r.output.filter((item) => isAssistantMessageItem(item)).flatMap((item) => getMessageTextParts(item)).join("\n").trim();
};
const isAssistantMessageItem = (item) => {
	if (item.type !== "message") return false;
	if (item.role !== void 0 && item.role !== "assistant") return false;
	return Array.isArray(item.content);
};
const getMessageTextParts = (item) => {
	if (!Array.isArray(item.content)) return [];
	const out = [];
	for (const c of item.content) if ((c.type === "output_text" || c.type === "text") && typeof c.text === "string") out.push(c.text);
	return out;
};
/**
* Append the assistant turn (containing tool_use blocks) and the
* corresponding tool_result blocks to the conversation, so the next
* createMessages call sees them as prior turns.
*/
const appendToolRoundTrip = (payload, assistantContent, toolResults) => {
	const assistantMsg = {
		role: "assistant",
		content: assistantContent
	};
	const userContent = toolResults;
	payload.messages.push(assistantMsg, {
		role: "user",
		content: userContent
	});
};
const buildToolResult = (toolUseId, text, isError = false) => ({
	type: "tool_result",
	tool_use_id: toolUseId,
	content: [{
		type: "text",
		text
	}],
	is_error: isError
});
const WEB_SEARCH_SYSTEM_HINT = "You have access to a `web_search` tool. When the user asks about current events, recent data, or anything that may have changed, call `web_search` with a focused query. You may call it multiple times. After you have enough information, answer the user with citations to the source URLs returned.";
const injectWebSearchHint = (payload) => {
	const hint = {
		type: "text",
		text: WEB_SEARCH_SYSTEM_HINT
	};
	if (!payload.system) {
		payload.system = [hint];
		return;
	}
	if (typeof payload.system === "string") {
		payload.system = [{
			type: "text",
			text: payload.system
		}, hint];
		return;
	}
	payload.system = [hint, ...payload.system];
};
const logShim = (msg, extra) => {
	if (extra) consola.info(`[web-search-shim] ${msg}`, extra);
	else consola.info(`[web-search-shim] ${msg}`);
};

//#endregion
//#region src/routes/messages/handler.ts
const logger$5 = createHandlerLogger("messages-handler");
async function handleCompletion(c) {
	await checkRateLimit(state);
	const anthropicPayload = await c.req.json();
	anthropicPayload.model = resolveToUpstream(anthropicPayload.model);
	debugJson(logger$5, "Anthropic request payload:", anthropicPayload);
	sanitizeIdeTools(anthropicPayload);
	const subagentMarker = parseSubagentMarkerFromFirstUser(anthropicPayload);
	if (subagentMarker) debugJson(logger$5, "Detected Subagent marker:", subagentMarker);
	const sessionId = getRootSessionId(anthropicPayload, c);
	logger$5.debug("Extracted session ID:", sessionId);
	const compactType = getCompactType(anthropicPayload);
	const anthropicBeta = c.req.header("anthropic-beta");
	logger$5.debug("Anthropic Beta header:", anthropicBeta);
	const noTools = !anthropicPayload.tools || anthropicPayload.tools.length === 0;
	if (anthropicBeta && noTools && compactType === 0) anthropicPayload.model = getSmallModel();
	if (compactType) logger$5.debug("Compact request type:", compactType);
	stripToolReferenceTurnBoundary(anthropicPayload);
	mergeToolResultForClaude(anthropicPayload, { skipLastMessage: compactType === COMPACT_REQUEST });
	const requestId = generateRequestIdFromPayload(anthropicPayload, sessionId);
	logger$5.debug("Generated request ID:", requestId);
	if (state.manualApprove) await awaitApproval();
	const selectedModel = findEndpointModel(anthropicPayload.model);
	anthropicPayload.model = selectedModel?.id ?? anthropicPayload.model;
	const wantsWebSearch = hasAnthropicWebSearch(anthropicPayload);
	if (wantsWebSearch && shouldUseResponsesApi(selectedModel)) return await handleWithResponsesApi(c, anthropicPayload, {
		subagentMarker,
		selectedModel,
		requestId,
		sessionId,
		compactType,
		logger: logger$5
	});
	if (wantsWebSearch && shouldUseMessagesApi(selectedModel)) return c.json(await runWebSearchShimLoop(anthropicPayload, {
		anthropicBetaHeader: anthropicBeta,
		subagentMarker,
		requestId,
		sessionId,
		compactType
	}));
	if (shouldUseMessagesApi(selectedModel)) return await handleWithMessagesApi(c, anthropicPayload, {
		anthropicBetaHeader: anthropicBeta,
		subagentMarker,
		selectedModel,
		requestId,
		sessionId,
		compactType,
		logger: logger$5
	});
	if (shouldUseResponsesApi(selectedModel)) return await handleWithResponsesApi(c, anthropicPayload, {
		subagentMarker,
		selectedModel,
		requestId,
		sessionId,
		compactType,
		logger: logger$5
	});
	return await handleWithChatCompletions(c, anthropicPayload, {
		subagentMarker,
		requestId,
		sessionId,
		compactType,
		logger: logger$5
	});
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
async function runWebSearchShimLoop(payload, opts) {
	rewriteWebSearchToolsToFunction(payload);
	injectWebSearchHint(payload);
	payload.stream = false;
	const searchModel = pickSearchModel();
	logShim("Entering shim loop", {
		primaryModel: payload.model,
		searchModel
	});
	let iterations = 0;
	let lastResponse;
	while (iterations < MAX_TOOL_LOOP_ITERATIONS) {
		iterations += 1;
		const response = await createMessages(payload, opts.anthropicBetaHeader, {
			subagentMarker: opts.subagentMarker,
			requestId: `${opts.requestId}-shim-${iterations}`,
			sessionId: opts.sessionId,
			compactType: opts.compactType
		});
		lastResponse = response;
		const calls = extractWebSearchCalls(response);
		if (calls.length === 0) {
			logShim(`Loop done after ${iterations} iteration(s)`);
			break;
		}
		logShim(`Iteration ${iterations}: ${calls.length} web_search call(s)`);
		const results = await Promise.all(calls.map(async (call) => {
			const query = typeof call.input.query === "string" ? call.input.query : JSON.stringify(call.input);
			try {
				const text = await performWebSearchViaResponses(query, {
					searchModel,
					requestId: opts.requestId,
					sessionId: opts.sessionId,
					subagentMarker: opts.subagentMarker,
					compactType: opts.compactType
				});
				return buildToolResult(call.id, text, false);
			} catch (err) {
				const msg = err instanceof Error ? err.message : "unknown web_search error";
				logShim(`web_search failed for query "${query}": ${msg}`);
				return buildToolResult(call.id, `Web search failed: ${msg}`, true);
			}
		}));
		appendToolRoundTrip(payload, response.content, results);
	}
	if (!lastResponse) throw new Error("Web search shim produced no response");
	return lastResponse;
}
const pickSearchModel = () => {
	const models = state.models?.data ?? [];
	for (const id of [
		"gpt-5-mini",
		"gpt-5.4-mini",
		"gpt-5.4",
		"gpt-5.3-codex"
	]) if (models.find((x) => x.id === id)?.supported_endpoints?.includes("/responses")) return id;
	const any = models.find((m) => m.supported_endpoints?.includes("/responses"));
	if (any) return any.id;
	return "gpt-5-mini";
};
const RESPONSES_ENDPOINT$1 = "/responses";
const MESSAGES_ENDPOINT = "/v1/messages";
const shouldUseResponsesApi = (selectedModel) => {
	return selectedModel?.supported_endpoints?.includes(RESPONSES_ENDPOINT$1) ?? false;
};
const shouldUseMessagesApi = (selectedModel) => {
	if (!isMessagesApiEnabled()) return false;
	return selectedModel?.supported_endpoints?.includes(MESSAGES_ENDPOINT) ?? false;
};

//#endregion
//#region src/routes/messages/route.ts
const messageRoutes = new Hono();
messageRoutes.post("/", async (c) => {
	try {
		return await handleCompletion(c);
	} catch (error) {
		return await forwardError(c, error);
	}
});
messageRoutes.post("/count_tokens", async (c) => {
	try {
		return await handleCountTokens(c);
	} catch (error) {
		return await forwardError(c, error);
	}
});

//#endregion
//#region src/routes/models/route.ts
const modelRoutes = new Hono();
modelRoutes.get("/", async (c) => {
	try {
		if (!state.models) await cacheModels();
		const models = state.models?.data.map((model) => ({
			...model,
			id: model.id,
			object: "model",
			type: "model",
			created: 0,
			created_at: (/* @__PURE__ */ new Date(0)).toISOString(),
			owned_by: model.vendor,
			display_name: model.name
		}));
		return c.json({
			object: "list",
			data: models,
			has_more: false
		});
	} catch (error) {
		return await forwardError(c, error);
	}
});

//#endregion
//#region src/routes/provider/messages/count-tokens-handler.ts
const logger$4 = createHandlerLogger("provider-count-tokens-handler");
const createFallbackModel = (modelId) => ({
	capabilities: {
		family: "provider",
		limits: {},
		object: "model_capabilities",
		supports: {},
		tokenizer: "o200k_base",
		type: "chat"
	},
	id: modelId,
	model_picker_enabled: false,
	name: modelId,
	object: "model",
	preview: false,
	vendor: "provider",
	version: "unknown"
});
async function handleProviderCountTokens(c) {
	const provider = c.req.param("provider");
	try {
		const anthropicPayload = await c.req.json();
		const openAIPayload = translateToOpenAI(anthropicPayload);
		const modelId = anthropicPayload.model.trim();
		let selectedModel = state.models?.data.find((model) => model.id === modelId);
		if (!selectedModel && modelId) selectedModel = createFallbackModel(modelId);
		if (!selectedModel) {
			logger$4.warn("provider.count_tokens.model_not_found", {
				provider,
				model: anthropicPayload.model
			});
			return c.json({ input_tokens: 1 });
		}
		const tokenCount = await getTokenCount(openAIPayload, selectedModel);
		const finalTokenCount = tokenCount.input + tokenCount.output;
		logger$4.debug("provider.count_tokens.success", {
			provider,
			model: anthropicPayload.model,
			input_tokens: finalTokenCount
		});
		return c.json({ input_tokens: finalTokenCount });
	} catch (error) {
		logger$4.error("provider.count_tokens.error", {
			provider,
			error
		});
		return c.json({ input_tokens: 1 });
	}
}

//#endregion
//#region src/services/providers/anthropic-proxy.ts
const FORWARDABLE_HEADERS = [
	"anthropic-version",
	"anthropic-beta",
	"accept",
	"user-agent"
];
const STRIPPED_RESPONSE_HEADERS = [
	"connection",
	"content-encoding",
	"content-length",
	"keep-alive",
	"proxy-authenticate",
	"proxy-authorization",
	"te",
	"trailer",
	"transfer-encoding",
	"upgrade"
];
function buildProviderUpstreamHeaders(providerConfig, requestHeaders) {
	const authHeaders = {};
	if (providerConfig.authType === "authorization") authHeaders.authorization = `Bearer ${providerConfig.apiKey}`;
	else authHeaders["x-api-key"] = providerConfig.apiKey;
	const headers = {
		"content-type": "application/json",
		accept: "application/json",
		...authHeaders
	};
	for (const headerName of FORWARDABLE_HEADERS) {
		const headerValue = requestHeaders.get(headerName);
		if (headerValue) headers[headerName] = headerValue;
	}
	return headers;
}
function createProviderProxyResponse(upstreamResponse) {
	const headers = new Headers(upstreamResponse.headers);
	for (const headerName of STRIPPED_RESPONSE_HEADERS) headers.delete(headerName);
	return new Response(upstreamResponse.body, {
		headers,
		status: upstreamResponse.status,
		statusText: upstreamResponse.statusText
	});
}
async function forwardProviderMessages(providerConfig, payload, requestHeaders) {
	return await fetch(`${providerConfig.baseUrl}/v1/messages`, {
		method: "POST",
		headers: buildProviderUpstreamHeaders(providerConfig, requestHeaders),
		body: JSON.stringify(payload)
	});
}
async function forwardProviderModels(providerConfig, requestHeaders) {
	return await fetch(`${providerConfig.baseUrl}/v1/models`, {
		method: "GET",
		headers: buildProviderUpstreamHeaders(providerConfig, requestHeaders)
	});
}

//#endregion
//#region src/routes/provider/messages/handler.ts
const logger$3 = createHandlerLogger("provider-messages-handler");
async function handleProviderMessages(c) {
	const provider = c.req.param("provider");
	const providerConfig = getProviderConfig(provider);
	if (!providerConfig) return c.json({ error: {
		message: `Provider '${provider}' not found or disabled`,
		type: "invalid_request_error"
	} }, 404);
	try {
		const payload = await c.req.json();
		const modelConfig = providerConfig.models?.[payload.model];
		payload.temperature ??= modelConfig?.temperature;
		payload.top_p ??= modelConfig?.topP;
		payload.top_k ??= modelConfig?.topK;
		debugJson(logger$3, "provider.messages.request", {
			payload,
			provider
		});
		const upstreamResponse = await forwardProviderMessages(providerConfig, payload, c.req.raw.headers);
		if (!upstreamResponse.ok) {
			logger$3.error("Failed to create responses", upstreamResponse);
			throw new HTTPError("Failed to create responses", upstreamResponse);
		}
		const contentType = upstreamResponse.headers.get("content-type") ?? "";
		if (Boolean(payload.stream) && contentType.includes("text/event-stream")) return streamProviderMessages({
			c,
			payload,
			provider,
			providerConfig,
			upstreamResponse
		});
		const jsonBody = await upstreamResponse.json();
		return respondProviderMessagesJson(c, {
			body: jsonBody,
			payload,
			provider,
			providerConfig
		});
	} catch (error) {
		logger$3.error("provider.messages.error", {
			provider,
			error
		});
		throw error;
	}
}
const streamProviderMessages = ({ c, payload, provider, providerConfig, upstreamResponse }) => {
	logger$3.debug("provider.messages.streaming");
	const recordUsage = createProviderMessagesUsageRecorder(payload, provider);
	return streamSSE(c, async (stream) => {
		let usage = {};
		for await (const chunk of events(upstreamResponse)) {
			logger$3.debug("provider.messages.raw_stream_event:", chunk.data);
			const eventName = chunk.event;
			if (eventName === "ping") {
				await stream.writeSSE({
					event: "ping",
					data: "{\"type\":\"ping\"}"
				});
				continue;
			}
			let data = chunk.data;
			if (!data) continue;
			if (chunk.data === "[DONE]") break;
			const parsed = parseProviderStreamEvent(data, providerConfig);
			if (parsed) {
				usage = mergeAnthropicUsage(usage, parsed.usage);
				data = parsed.data;
			}
			await stream.writeSSE({
				event: eventName,
				data
			});
		}
		recordUsage(usage);
	});
};
const parseProviderStreamEvent = (data, providerConfig) => {
	try {
		const parsed = JSON.parse(data);
		if (parsed.type === "message_start") {
			adjustInputTokens(providerConfig, parsed.message.usage);
			return {
				data: JSON.stringify(parsed),
				model: parsed.message.model,
				usage: normalizeAnthropicUsage(parsed.message.usage)
			};
		}
		if (parsed.type === "message_delta") {
			adjustInputTokens(providerConfig, parsed.usage);
			return {
				data: JSON.stringify(parsed),
				usage: normalizeAnthropicUsage(parsed.usage)
			};
		}
		return {
			data: JSON.stringify(parsed),
			usage: {}
		};
	} catch (error) {
		logger$3.error("provider.messages.streaming.adjust_tokens_error", {
			error,
			originalData: data
		});
		return null;
	}
};
const respondProviderMessagesJson = (c, options) => {
	const { body, payload, provider, providerConfig } = options;
	const recordUsage = createProviderMessagesUsageRecorder(payload, provider);
	adjustInputTokens(providerConfig, body.usage);
	recordUsage(normalizeAnthropicUsage(body.usage));
	debugJson(logger$3, "provider.messages.no_stream result:", body);
	return c.json(body);
};
const createProviderMessagesUsageRecorder = (payload, provider) => createProviderTokenUsageRecorder({
	endpoint: "provider_messages",
	model: payload.model,
	providerName: provider,
	sessionId: parseUserIdMetadata(payload.metadata?.user_id).sessionId
});
const adjustInputTokens = (providerConfig, usage) => {
	if (!providerConfig.adjustInputTokens || !usage) return;
	usage.input_tokens = Math.max(0, (usage.input_tokens ?? 0) - (usage.cache_read_input_tokens ?? 0) - (usage.cache_creation_input_tokens ?? 0));
	debugJson(logger$3, "provider.messages.adjusted_usage:", usage);
};

//#endregion
//#region src/routes/provider/messages/route.ts
const providerMessageRoutes = new Hono();
providerMessageRoutes.post("/", async (c) => {
	try {
		return await handleProviderMessages(c);
	} catch (error) {
		return await forwardError(c, error);
	}
});
providerMessageRoutes.post("/count_tokens", async (c) => {
	try {
		return await handleProviderCountTokens(c);
	} catch (error) {
		return await forwardError(c, error);
	}
});

//#endregion
//#region src/routes/provider/models/route.ts
const logger$2 = createHandlerLogger("provider-models-handler");
const providerModelRoutes = new Hono();
providerModelRoutes.get("/", async (c) => {
	const provider = c.req.param("provider") ?? "";
	try {
		const providerConfig = getProviderConfig(provider);
		if (!providerConfig) return c.json({ error: {
			message: `Provider '${provider}' not found or disabled`,
			type: "invalid_request_error"
		} }, 404);
		const upstreamResponse = await forwardProviderModels(providerConfig, c.req.raw.headers);
		logger$2.debug("provider.models.response", {
			provider,
			statusCode: upstreamResponse.status
		});
		return createProviderProxyResponse(upstreamResponse);
	} catch (error) {
		logger$2.error("provider.models.error", {
			provider,
			error
		});
		return await forwardError(c, error);
	}
});

//#endregion
//#region src/routes/responses/stream-id-sync.ts
const createStreamIdTracker = () => ({ outputItems: /* @__PURE__ */ new Map() });
const fixStreamIds = (data, event, tracker) => {
	if (!data) return data;
	const parsed = JSON.parse(data);
	switch (event) {
		case "response.output_item.added": return handleOutputItemAdded(parsed, tracker);
		case "response.output_item.done": return handleOutputItemDone(parsed, tracker);
		default: return handleItemId(parsed, tracker);
	}
};
const handleOutputItemAdded = (parsed, tracker) => {
	if (!parsed.item.id) {
		let randomSuffix = "";
		while (randomSuffix.length < 16) randomSuffix += Math.random().toString(36).slice(2);
		parsed.item.id = `oi_${parsed.output_index}_${randomSuffix.slice(0, 16)}`;
	}
	const outputIndex = parsed.output_index;
	tracker.outputItems.set(outputIndex, parsed.item.id);
	return JSON.stringify(parsed);
};
const handleOutputItemDone = (parsed, tracker) => {
	const outputIndex = parsed.output_index;
	const originalId = tracker.outputItems.get(outputIndex);
	if (originalId) parsed.item.id = originalId;
	return JSON.stringify(parsed);
};
const handleItemId = (parsed, tracker) => {
	const outputIndex = parsed.output_index;
	if (outputIndex !== void 0) {
		const itemId = tracker.outputItems.get(outputIndex);
		if (itemId) parsed.item_id = itemId;
	}
	return JSON.stringify(parsed);
};

//#endregion
//#region src/routes/responses/handler.ts
const logger$1 = createHandlerLogger("responses-handler");
const RESPONSES_ENDPOINT = "/responses";
const handleResponses = async (c) => {
	await checkRateLimit(state);
	const payload = await c.req.json();
	payload.model = resolveToUpstream(payload.model);
	debugJson(logger$1, "Responses request payload:", payload);
	const requestId = generateRequestIdFromPayload({ messages: payload.input });
	logger$1.debug("Generated request ID:", requestId);
	const sessionId = getUUID(requestId);
	logger$1.debug("Extracted session ID:", sessionId);
	const recordUsage = createCopilotTokenUsageRecorder({
		endpoint: "responses",
		fallbackSessionId: sessionId,
		model: payload.model
	});
	useFunctionApplyPatch(payload);
	removeUnsupportedTools(payload);
	if (!isResponsesApiWebSearchEnabled()) removeWebSearchTool(payload);
	compactInputByLatestCompaction(payload);
	const selectedModel = state.models?.data.find((model) => model.id === payload.model);
	if (!(selectedModel?.supported_endpoints?.includes(RESPONSES_ENDPOINT) ?? false)) return c.json({ error: {
		message: "This model does not support the responses endpoint. Please choose a different model.",
		type: "invalid_request_error"
	} }, 400);
	applyResponsesApiContextManagement(payload, selectedModel?.capabilities.limits.max_prompt_tokens);
	debugJson(logger$1, "Translated Responses payload:", payload);
	const { vision, initiator } = getResponsesRequestOptions(payload);
	if (state.manualApprove) await awaitApproval();
	const response = await createResponses(payload, {
		vision,
		initiator,
		requestId,
		sessionId
	});
	if (isStreamingRequested(payload) && isAsyncIterable(response)) {
		logger$1.debug("Forwarding native Responses stream");
		return streamSSE(c, async (stream) => {
			const idTracker = createStreamIdTracker();
			let usage = {};
			for await (const chunk of response) {
				debugJson(logger$1, "Responses stream chunk:", chunk);
				const parsedEvent = parseResponsesStreamEvent(chunk);
				if (parsedEvent?.type === "response.completed" || parsedEvent?.type === "response.failed" || parsedEvent?.type === "response.incomplete") usage = normalizeResponsesUsage(parsedEvent.response.usage);
				const processedData = fixStreamIds(chunk.data ?? "", chunk.event, idTracker);
				await stream.writeSSE({
					id: chunk.id,
					event: chunk.event,
					data: processedData
				});
			}
			recordUsage(usage);
		});
	}
	debugJsonTail(logger$1, "Forwarding native Responses result:", {
		value: response,
		tailLength: 400
	});
	recordUsage(normalizeResponsesUsage(response.usage));
	return c.json(response);
};
const isAsyncIterable = (value) => Boolean(value) && typeof value[Symbol.asyncIterator] === "function";
const isStreamingRequested = (payload) => Boolean(payload.stream);
const parseResponsesStreamEvent = (chunk) => {
	const data = chunk.data;
	if (!data || data === "[DONE]") return null;
	try {
		return JSON.parse(data);
	} catch {
		return null;
	}
};
const useFunctionApplyPatch = (payload) => {
	if (getConfig().useFunctionApplyPatch ?? true) {
		logger$1.debug("Using function tool apply_patch for responses");
		if (Array.isArray(payload.tools)) {
			const toolsArr = payload.tools;
			for (let i = 0; i < toolsArr.length; i++) {
				const t = toolsArr[i];
				if (t.type === "custom" && t.name === "apply_patch") toolsArr[i] = {
					type: "function",
					name: t.name,
					description: "Use the `apply_patch` tool to edit files",
					parameters: {
						type: "object",
						properties: { input: {
							type: "string",
							description: "The entire contents of the apply_patch command"
						} },
						required: ["input"]
					},
					strict: false
				};
			}
		}
	}
};
const removeWebSearchTool = (payload) => {
	if (!Array.isArray(payload.tools) || payload.tools.length === 0) return;
	payload.tools = payload.tools.filter((t) => {
		return t.type !== "web_search";
	});
};
const COPILOT_UNSUPPORTED_TOOL_TYPES = new Set(["image_generation"]);
const removeUnsupportedTools = (payload) => {
	if (!Array.isArray(payload.tools) || payload.tools.length === 0) return;
	const dropped = [];
	payload.tools = payload.tools.filter((t) => {
		const type = t.type;
		if (COPILOT_UNSUPPORTED_TOOL_TYPES.has(type)) {
			dropped.push(type);
			return false;
		}
		return true;
	});
	if (dropped.length > 0) logger$1.debug("Removed unsupported tools:", dropped);
};

//#endregion
//#region src/routes/responses/route.ts
const responsesRoutes = new Hono();
responsesRoutes.post("/", async (c) => {
	try {
		return await handleResponses(c);
	} catch (error) {
		return await forwardError(c, error);
	}
});

//#endregion
//#region src/routes/token-usage/route.ts
const tokenUsageRoute = new Hono();
const periods = new Set([
	"day",
	"week",
	"month"
]);
const DEFAULT_EVENTS_PAGE_SIZE = 20;
function parsePeriod(value) {
	return periods.has(value) ? value : "day";
}
function parsePositiveInt(value, fallback) {
	const parsed = Number.parseInt(value ?? "", 10);
	return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}
tokenUsageRoute.get("/", async (c) => {
	const period = parsePeriod(c.req.query("period"));
	const summary = await getTokenUsageSummary(period);
	return c.json(summary);
});
tokenUsageRoute.get("/events", async (c) => {
	const period = parsePeriod(c.req.query("period"));
	const page = parsePositiveInt(c.req.query("page"), 1);
	const pageSize = parsePositiveInt(c.req.query("page_size"), DEFAULT_EVENTS_PAGE_SIZE);
	const eventsPage = await getTokenUsageEventsPage({
		page,
		pageSize,
		period
	});
	return c.json(eventsPage);
});

//#endregion
//#region src/routes/token/route.ts
const tokenRoute = new Hono();
tokenRoute.get("/", (c) => {
	try {
		return c.json({ token: state.copilotToken });
	} catch (error) {
		console.error("Error fetching token:", error);
		return c.json({
			error: "Failed to fetch token",
			token: null
		}, 500);
	}
});

//#endregion
//#region src/routes/usage/route.ts
const usageRoute = new Hono();
usageRoute.get("/", async (c) => {
	try {
		const usage = await getCopilotUsage();
		return c.json(usage);
	} catch (error) {
		console.error("Error fetching Copilot usage:", error);
		return c.json({ error: "Failed to fetch Copilot usage" }, 500);
	}
});

//#endregion
//#region src/server.ts
const server = new Hono();
server.use(traceIdMiddleware);
server.use(logger());
server.use(cors());
server.use("*", createAuthMiddleware({ allowUnauthenticatedPaths: [
	"/",
	"/usage-viewer",
	"/usage-viewer/"
] }));
server.get("/", (c) => c.text("Server running"));
server.get("/usage-viewer", (c) => {
	const usageViewerFileUrl = new URL("../pages/index.html", import.meta.url);
	return c.html(readFileSync(usageViewerFileUrl, "utf8"));
});
server.get("/usage-viewer/", (c) => c.redirect("/usage-viewer", 301));
server.route("/chat/completions", completionRoutes);
server.route("/models", modelRoutes);
server.route("/embeddings", embeddingRoutes);
server.route("/usage", usageRoute);
server.route("/token-usage", tokenUsageRoute);
server.route("/token", tokenRoute);
server.route("/responses", responsesRoutes);
server.route("/v1/chat/completions", completionRoutes);
server.route("/v1/models", modelRoutes);
server.route("/v1/embeddings", embeddingRoutes);
server.route("/v1/responses", responsesRoutes);
server.route("/v1/messages", messageRoutes);
server.route("/:provider/v1/messages", providerMessageRoutes);
server.route("/:provider/v1/models", providerModelRoutes);

//#endregion
export { server };
//# sourceMappingURL=server-CzEuGJyI.js.map