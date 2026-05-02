import { PATHS } from "./paths-Cla6y5eD.js";
import { HTTPError, getCopilotUsage, getGitHubApiBaseUrl, getOauthAppConfig, getOauthUrls, githubHeaders, githubUserHeaders, isOpencodeOauthApp, sleep, state } from "./utils-Caw-6iPt.js";
import consola from "consola";
import fs from "node:fs/promises";
import { setTimeout } from "node:timers/promises";

//#region src/services/github/get-copilot-token.ts
const getCopilotToken = async () => {
	const response = await fetch(`${getGitHubApiBaseUrl()}/copilot_internal/v2/token`, { headers: githubHeaders(state) });
	if (!response.ok) {
		const errorText = await response.clone().text();
		consola.error("Failed to get Copilot token response body", errorText);
		throw new HTTPError("Failed to get Copilot token", response);
	}
	return await response.json();
};

//#endregion
//#region src/services/github/get-device-code.ts
async function getDeviceCode() {
	const { clientId, headers, scope } = getOauthAppConfig();
	const { deviceCodeUrl } = getOauthUrls();
	const response = await fetch(deviceCodeUrl, {
		method: "POST",
		headers,
		body: JSON.stringify({
			client_id: clientId,
			scope
		})
	});
	if (!response.ok) throw new HTTPError("Failed to get device code", response);
	return await response.json();
}

//#endregion
//#region src/services/github/get-user.ts
async function getGitHubUser(githubToken) {
	const resolvedGithubToken = githubToken ?? state.githubToken;
	if (!resolvedGithubToken) throw new Error("GitHub token not found");
	const authState = {
		...state,
		githubToken: resolvedGithubToken
	};
	const response = await fetch(`${getGitHubApiBaseUrl()}/user`, { headers: githubUserHeaders(authState) });
	if (!response.ok) throw new HTTPError("Failed to get GitHub user", response);
	return await response.json();
}

//#endregion
//#region src/services/github/poll-access-token.ts
async function pollAccessToken(deviceCode) {
	const { clientId, headers } = getOauthAppConfig();
	const { accessTokenUrl } = getOauthUrls();
	const sleepDuration = (deviceCode.interval + 1) * 1e3;
	consola.debug(`Polling access token with interval of ${sleepDuration}ms`);
	while (true) {
		const response = await fetch(accessTokenUrl, {
			method: "POST",
			headers,
			body: JSON.stringify({
				client_id: clientId,
				device_code: deviceCode.device_code,
				grant_type: "urn:ietf:params:oauth:grant-type:device_code"
			})
		});
		if (!response.ok) {
			await sleep(sleepDuration);
			consola.error("Failed to poll access token:", await response.text());
			continue;
		}
		const json = await response.json();
		consola.debug("Polling access token response:", json);
		const { access_token } = json;
		if (access_token) return access_token;
		else await sleep(sleepDuration);
	}
}

//#endregion
//#region src/lib/token.ts
let copilotRefreshLoopController = null;
const stopCopilotRefreshLoop = () => {
	if (!copilotRefreshLoopController) return;
	copilotRefreshLoopController.abort();
	copilotRefreshLoopController = null;
};
const readGithubToken = () => fs.readFile(PATHS.GITHUB_TOKEN_PATH, "utf8");
const writeGithubToken = (token) => fs.writeFile(PATHS.GITHUB_TOKEN_PATH, token);
const setupCopilotToken = async () => {
	if (isOpencodeOauthApp()) {
		if (!state.githubToken) throw new Error(`opencode token not found`);
		state.copilotToken = state.githubToken;
		consola.debug("GitHub Copilot token set from opencode auth token");
		if (state.showToken) consola.info("Copilot token:", state.copilotToken);
		stopCopilotRefreshLoop();
		return;
	}
	const { token, refresh_in } = await getCopilotToken();
	state.copilotToken = token;
	consola.debug("GitHub Copilot Token fetched successfully!");
	if (state.showToken) consola.info("Copilot token:", token);
	stopCopilotRefreshLoop();
	const controller = new AbortController();
	copilotRefreshLoopController = controller;
	runCopilotRefreshLoop(refresh_in, controller.signal).catch(() => {
		consola.warn("Copilot token refresh loop stopped");
	}).finally(() => {
		if (copilotRefreshLoopController === controller) copilotRefreshLoopController = null;
	});
};
const REFRESH_POLL_INTERVAL_MS = 15e3;
const EARLY_REFRESH_BUFFER_MS = 6e4;
const RETRY_REFRESH_DELAY_MS = 15e3;
const MIN_REFRESH_DELAY_MS = 1e3;
const getRefreshDeadlineMs = (refreshIn, nowMs = Date.now()) => nowMs + Math.max(refreshIn * 1e3 - EARLY_REFRESH_BUFFER_MS, MIN_REFRESH_DELAY_MS);
const getRefreshPollDelayMs = (refreshAtMs, nowMs = Date.now()) => Math.min(Math.max(refreshAtMs - nowMs, 0), REFRESH_POLL_INTERVAL_MS);
const runCopilotRefreshLoop = async (refreshIn, signal) => {
	let refreshAtMs = getRefreshDeadlineMs(refreshIn);
	while (!signal.aborted) {
		const nextDelayMs = getRefreshPollDelayMs(refreshAtMs);
		if (nextDelayMs > 0) {
			await setTimeout(nextDelayMs, void 0, { signal });
			continue;
		}
		consola.debug("Refreshing Copilot token");
		try {
			const { token, refresh_in } = await getCopilotToken();
			state.copilotToken = token;
			refreshAtMs = getRefreshDeadlineMs(refresh_in);
			consola.debug("Copilot token refreshed");
			if (state.showToken) consola.info("Refreshed Copilot token:", token);
		} catch (error) {
			consola.error("Failed to refresh Copilot token:", error);
			refreshAtMs = Date.now() + RETRY_REFRESH_DELAY_MS;
			consola.warn(`Retrying Copilot token refresh in ${RETRY_REFRESH_DELAY_MS / 1e3}s`);
		}
	}
};
async function setupGitHubToken(options) {
	try {
		const githubToken = await readGithubToken();
		if (githubToken && !options?.force) {
			state.githubToken = githubToken;
			if (state.showToken) consola.info("GitHub token:", githubToken);
			await logUser();
			return;
		}
		consola.info("Not logged in, getting new access token");
		const response = await getDeviceCode();
		consola.debug("Device code response:", response);
		consola.info(`Please enter the code "${response.user_code}" in ${response.verification_uri}`);
		const token = await pollAccessToken(response);
		await writeGithubToken(token);
		state.githubToken = token;
		if (state.showToken) consola.info("GitHub token:", token);
		await logUser();
	} catch (error) {
		if (error instanceof HTTPError) {
			consola.error("Failed to get GitHub token:", await error.response.json());
			throw error;
		}
		consola.error("Failed to get GitHub token:", error);
		throw error;
	}
}
async function logUser() {
	const user = await getGitHubUser();
	state.userName = user.login;
	consola.info(`Logged in as ${user.login}`);
	state.copilotApiUrl = (await getCopilotUsage()).endpoints.api;
}

//#endregion
export { logUser, setupCopilotToken, setupGitHubToken };
//# sourceMappingURL=token-CyrFVmrS.js.map