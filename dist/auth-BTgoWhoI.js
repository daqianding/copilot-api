import { n as ensurePaths, t as PATHS } from "./paths-RsZHsmRX.js";
import { B as state } from "./utils-CBc0KiDM.js";
import { r as setupGitHubToken } from "./token-TbOrtoLs.js";
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
export { auth, runAuth };
//# sourceMappingURL=auth-BTgoWhoI.js.map