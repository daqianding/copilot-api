import { n as ensurePaths } from "./paths-RsZHsmRX.js";
import { B as state, M as initOpencodeVersion, a as cacheVsCodeSessionId, f as exposeAlias, i as cacheVsCodeDeviceId, n as cacheModels, r as cacheVSCodeVersion, t as cacheMacMachineId } from "./utils-CBc0KiDM.js";
import { n as setupCopilotToken, r as setupGitHubToken, t as logUser } from "./token-TbOrtoLs.js";
import { d as mergeConfigWithDefaults } from "./config-DYMaQsCz.js";
import { defineCommand } from "citty";
import consola from "consola";
import { execSync } from "node:child_process";
import clipboard from "clipboardy";
import { serve } from "srvx";
import invariant from "tiny-invariant";
import { getProxyForUrl } from "proxy-from-env";
import { Agent, ProxyAgent, setGlobalDispatcher } from "undici";
import process from "node:process";

//#region src/lib/proxy.ts
function initProxyFromEnv() {
	if (typeof Bun !== "undefined") return;
	try {
		const direct = new Agent();
		const proxies = /* @__PURE__ */ new Map();
		setGlobalDispatcher({
			dispatch(options, handler) {
				try {
					const origin = typeof options.origin === "string" ? new URL(options.origin) : options.origin;
					const raw = getProxyForUrl(origin.toString());
					const proxyUrl = raw && raw.length > 0 ? raw : void 0;
					if (!proxyUrl) {
						consola.debug(`HTTP proxy bypass: ${origin.hostname}`);
						return direct.dispatch(options, handler);
					}
					let agent = proxies.get(proxyUrl);
					if (!agent) {
						agent = new ProxyAgent(proxyUrl);
						proxies.set(proxyUrl, agent);
					}
					let label = proxyUrl;
					try {
						const u = new URL(proxyUrl);
						label = `${u.protocol}//${u.host}`;
					} catch {}
					consola.debug(`HTTP proxy route: ${origin.hostname} via ${label}`);
					return agent.dispatch(options, handler);
				} catch {
					return direct.dispatch(options, handler);
				}
			},
			close() {
				return direct.close();
			},
			destroy() {
				return direct.destroy();
			}
		});
		consola.debug("HTTP proxy configured from environment (per-URL)");
	} catch (err) {
		consola.debug("Proxy setup skipped:", err);
	}
}

//#endregion
//#region src/lib/shell.ts
function getShell() {
	const { platform, ppid, env } = process;
	if (platform === "win32") {
		try {
			if (execSync(`wmic process get ParentProcessId,Name | findstr "${ppid}"`, { stdio: "pipe" }).toString().toLowerCase().includes("powershell.exe")) return "powershell";
		} catch {
			return "cmd";
		}
		return "cmd";
	} else {
		const shellPath = env.SHELL;
		if (shellPath) {
			if (shellPath.endsWith("zsh")) return "zsh";
			if (shellPath.endsWith("fish")) return "fish";
			if (shellPath.endsWith("bash")) return "bash";
		}
		return "sh";
	}
}
/**
* Generates a copy-pasteable script to set multiple environment variables
* and run a subsequent command.
* @param {EnvVars} envVars - An object of environment variables to set.
* @param {string} commandToRun - The command to run after setting the variables.
* @returns {string} The formatted script string.
*/
function generateEnvScript(envVars, commandToRun = "") {
	const shell = getShell();
	const filteredEnvVars = Object.entries(envVars).filter(([, value]) => value !== void 0);
	let commandBlock;
	switch (shell) {
		case "powershell":
			commandBlock = filteredEnvVars.map(([key, value]) => `$env:${key} = ${value}`).join("; ");
			break;
		case "cmd":
			commandBlock = filteredEnvVars.map(([key, value]) => `set ${key}=${value}`).join(" & ");
			break;
		case "fish":
			commandBlock = filteredEnvVars.map(([key, value]) => `set -gx ${key} ${value}`).join("; ");
			break;
		default: {
			const assignments = filteredEnvVars.map(([key, value]) => `${key}=${value}`).join(" ");
			commandBlock = filteredEnvVars.length > 0 ? `export ${assignments}` : "";
			break;
		}
	}
	if (commandBlock && commandToRun) return `${commandBlock}${shell === "cmd" ? " & " : " && "}${commandToRun}`;
	return commandBlock || commandToRun;
}

//#endregion
//#region src/start.ts
async function runServer(options) {
	consola.options.throttle = 0;
	mergeConfigWithDefaults();
	await initOpencodeVersion();
	if (options.proxyEnv) initProxyFromEnv();
	state.verbose = options.verbose;
	if (options.verbose) {
		consola.level = 5;
		consola.info("Verbose logging enabled");
	}
	state.accountType = options.accountType;
	if (options.accountType !== "individual") consola.info(`Using ${options.accountType} plan GitHub account`);
	state.manualApprove = options.manual;
	state.rateLimitSeconds = options.rateLimit;
	state.rateLimitWait = options.rateLimitWait;
	state.showToken = options.showToken;
	await ensurePaths();
	await cacheVSCodeVersion();
	cacheMacMachineId();
	cacheVsCodeSessionId();
	await cacheVsCodeDeviceId();
	if (options.githubToken) {
		state.githubToken = options.githubToken;
		consola.info("Using provided GitHub token");
		await logUser();
	} else await setupGitHubToken();
	await setupCopilotToken();
	await cacheModels();
	consola.info(`Available models: \n${state.models?.data.map((model) => `- ${model.id}`).join("\n")}`);
	const serverUrl = `http://localhost:${options.port}`;
	if (options.claudeCode) {
		consola.log("\n💡 Tip: The --claude-code flag simply generates a clipboard command for launching Claude Code. \nAll models remain fully accessible without this flag, just configure the model ID directly in your settings.json file.");
		invariant(state.models, "Models should be loaded by now");
		const selectedModel = await consola.prompt("Select a model to use with Claude Code", {
			type: "select",
			options: state.models.data.map((model) => model.id)
		});
		const selectedSmallModel = await consola.prompt("Select a small model to use with Claude Code", {
			type: "select",
			options: state.models.data.map((model) => model.id)
		});
		const command = generateEnvScript({
			ANTHROPIC_BASE_URL: serverUrl,
			ANTHROPIC_AUTH_TOKEN: "dummy",
			ANTHROPIC_MODEL: exposeAlias(selectedModel),
			ANTHROPIC_DEFAULT_SONNET_MODEL: exposeAlias(selectedModel),
			ANTHROPIC_DEFAULT_HAIKU_MODEL: exposeAlias(selectedSmallModel),
			DISABLE_NON_ESSENTIAL_MODEL_CALLS: "1",
			CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1",
			CLAUDE_CODE_ATTRIBUTION_HEADER: "0",
			CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION: "false",
			CLAUDE_CODE_DISABLE_TERMINAL_TITLE: "true",
			CLAUDE_CODE_ENABLE_AWAY_SUMMARY: "0",
			CLAUDE_PLUGIN_ENABLE_QUESTION_RULES: "true"
		}, "claude");
		try {
			clipboard.writeSync(command);
			consola.success("Copied Claude Code command to clipboard!");
		} catch {
			consola.warn("Failed to copy to clipboard. Here is the Claude Code command:");
			consola.log(command);
		}
	}
	consola.box(`🌐 Usage Viewer: ${serverUrl}/usage-viewer?endpoint=${serverUrl}/usage`);
	const { server } = await import("./server-DeQnxCps.js");
	serve({
		fetch: server.fetch,
		port: options.port,
		bun: { idleTimeout: 0 }
	});
}
const start = defineCommand({
	meta: {
		name: "start",
		description: "Start the Copilot API server"
	},
	args: {
		port: {
			alias: "p",
			type: "string",
			default: "4141",
			description: "Port to listen on"
		},
		verbose: {
			alias: "v",
			type: "boolean",
			default: false,
			description: "Enable verbose logging"
		},
		"account-type": {
			alias: "a",
			type: "string",
			default: "individual",
			description: "Account type to use (individual, business, enterprise)"
		},
		manual: {
			type: "boolean",
			default: false,
			description: "Enable manual request approval"
		},
		"rate-limit": {
			alias: "r",
			type: "string",
			description: "Rate limit in seconds between requests"
		},
		wait: {
			alias: "w",
			type: "boolean",
			default: false,
			description: "Wait instead of error when rate limit is hit. Has no effect if rate limit is not set"
		},
		"github-token": {
			alias: "g",
			type: "string",
			description: "Provide GitHub token directly (must be generated using the `auth` subcommand)"
		},
		"claude-code": {
			alias: "c",
			type: "boolean",
			default: false,
			description: "Generate a command to launch Claude Code with Copilot API config"
		},
		"show-token": {
			type: "boolean",
			default: false,
			description: "Show GitHub and Copilot tokens on fetch and refresh"
		},
		"proxy-env": {
			type: "boolean",
			default: false,
			description: "Initialize proxy from environment variables"
		}
	},
	run({ args }) {
		const rateLimitRaw = args["rate-limit"];
		const rateLimit = rateLimitRaw === void 0 ? void 0 : Number.parseInt(rateLimitRaw, 10);
		return runServer({
			port: Number.parseInt(args.port, 10),
			verbose: args.verbose,
			accountType: args["account-type"],
			manual: args.manual,
			rateLimit,
			rateLimitWait: args.wait,
			githubToken: args["github-token"],
			claudeCode: args["claude-code"],
			showToken: args["show-token"],
			proxyEnv: args["proxy-env"]
		});
	}
});

//#endregion
export { runServer, start };
//# sourceMappingURL=start-CPrn5aSK.js.map