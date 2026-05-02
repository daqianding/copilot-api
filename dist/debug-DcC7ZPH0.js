import { PATHS } from "./paths-Cla6y5eD.js";
import { defineCommand } from "citty";
import consola from "consola";
import fs from "node:fs/promises";
import os from "node:os";

//#region src/debug.ts
async function getPackageVersion() {
	try {
		const packageJsonPath = new URL("../package.json", import.meta.url).pathname;
		return JSON.parse(await fs.readFile(packageJsonPath)).version;
	} catch {
		return "unknown";
	}
}
function getRuntimeInfo() {
	const isBun = typeof Bun !== "undefined";
	return {
		name: isBun ? "bun" : "node",
		version: isBun ? Bun.version : process.version.slice(1),
		platform: os.platform(),
		arch: os.arch()
	};
}
async function checkTokenExists() {
	try {
		if (!(await fs.stat(PATHS.GITHUB_TOKEN_PATH)).isFile()) return false;
		return (await fs.readFile(PATHS.GITHUB_TOKEN_PATH, "utf8")).trim().length > 0;
	} catch {
		return false;
	}
}
async function getDebugInfo() {
	const [version, tokenExists] = await Promise.all([getPackageVersion(), checkTokenExists()]);
	return {
		version,
		runtime: getRuntimeInfo(),
		paths: {
			APP_DIR: PATHS.APP_DIR,
			GITHUB_TOKEN_PATH: PATHS.GITHUB_TOKEN_PATH
		},
		tokenExists
	};
}
function printDebugInfoPlain(info) {
	consola.info(`copilot-api debug

Version: ${info.version}
Runtime: ${info.runtime.name} ${info.runtime.version} (${info.runtime.platform} ${info.runtime.arch})

Paths:
- APP_DIR: ${info.paths.APP_DIR}
- GITHUB_TOKEN_PATH: ${info.paths.GITHUB_TOKEN_PATH}

Token exists: ${info.tokenExists ? "Yes" : "No"}`);
}
function printDebugInfoJson(info) {
	console.log(JSON.stringify(info, null, 2));
}
async function runDebug(options) {
	const debugInfo = await getDebugInfo();
	if (options.json) printDebugInfoJson(debugInfo);
	else printDebugInfoPlain(debugInfo);
}
const debug = defineCommand({
	meta: {
		name: "debug",
		description: "Print debug information about the application"
	},
	args: { json: {
		type: "boolean",
		default: false,
		description: "Output debug information as JSON"
	} },
	run({ args }) {
		return runDebug({ json: args.json });
	}
});

//#endregion
export { debug };
//# sourceMappingURL=debug-DcC7ZPH0.js.map