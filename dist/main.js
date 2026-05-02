#!/usr/bin/env node
import { createRequire } from "node:module";
import { defineCommand, parseArgs, runMain } from "citty";
import consola from "consola";

//#region src/lib/electron-fetch.ts
const require = createRequire(import.meta.url);
function bindElectronFetch() {
	if (!process.versions.electron) return false;
	try {
		const electronModule = require("electron");
		const netFetch = electronModule.net?.fetch;
		if (typeof netFetch !== "function") return false;
		globalThis.fetch = netFetch.bind(electronModule.net);
		consola.log("Successfully bound Electron's net.fetch to global fetch.");
		return true;
	} catch {
		consola.log("Failed to bind Electron's net.fetch. Falling back to global fetch.");
		return false;
	}
}

//#endregion
//#region src/main.ts
const cliArgs = {
	"api-home": {
		type: "string",
		description: "Path to the API home directory."
	},
	"oauth-app": {
		type: "string",
		description: "OAuth app identifier."
	},
	"enterprise-url": {
		type: "string",
		description: "Enterprise URL for GitHub."
	}
};
const args = parseArgs(process.argv, cliArgs);
if (typeof args["api-home"] === "string") process.env.COPILOT_API_HOME = args["api-home"];
if (typeof args["oauth-app"] === "string") process.env.COPILOT_API_OAUTH_APP = args["oauth-app"];
if (typeof args["enterprise-url"] === "string") process.env.COPILOT_API_ENTERPRISE_URL = args["enterprise-url"];
bindElectronFetch();
const { auth } = await import("./auth-BTgoWhoI.js");
const { checkUsage } = await import("./check-usage-56cYKSRL.js");
const { debug } = await import("./debug-DpsLQXp_.js");
const { start } = await import("./start-CPrn5aSK.js");
await runMain(defineCommand({
	meta: {
		name: "copilot-api",
		description: "A wrapper around GitHub Copilot API to make it OpenAI compatible, making it usable for other tools."
	},
	subCommands: {
		auth,
		start,
		"check-usage": checkUsage,
		debug
	},
	args: cliArgs
}));

//#endregion
export {  };
//# sourceMappingURL=main.js.map