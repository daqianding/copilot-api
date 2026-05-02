import { PATHS, ensurePaths } from "./paths-Cla6y5eD.js";
import { state } from "./utils-Caw-6iPt.js";
import { setupGitHubToken } from "./token-CyrFVmrS.js";
import { defineCommand } from "citty";
import consola from "consola";

//#region src/auth.ts
async function runAuth(options) {
	if (options.verbose) {
		consola.level = 5;
		consola.info("Verbose logging enabled");
	}
	state.showToken = options.showToken;
	await ensurePaths();
	await setupGitHubToken({ force: true });
	consola.success("GitHub token written to", PATHS.GITHUB_TOKEN_PATH);
}
const auth = defineCommand({
	meta: {
		name: "auth",
		description: "Run GitHub auth flow without running the server"
	},
	args: {
		verbose: {
			alias: "v",
			type: "boolean",
			default: false,
			description: "Enable verbose logging"
		},
		"show-token": {
			type: "boolean",
			default: false,
			description: "Show GitHub token on auth"
		}
	},
	run({ args }) {
		return runAuth({
			verbose: args.verbose,
			showToken: args["show-token"]
		});
	}
});

//#endregion
export { auth };
//# sourceMappingURL=auth-NxTGe57g.js.map