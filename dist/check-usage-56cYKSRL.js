import { n as ensurePaths } from "./paths-RsZHsmRX.js";
import { h as getCopilotUsage } from "./utils-CBc0KiDM.js";
import { r as setupGitHubToken } from "./token-TbOrtoLs.js";
import { defineCommand } from "citty";
import consola from "consola";

//#region src/check-usage.ts
const checkUsage = defineCommand({
	meta: {
		name: "check-usage",
		description: "Show current GitHub Copilot usage/quota information"
	},
	async run() {
		await ensurePaths();
		await setupGitHubToken();
		try {
			const usage = await getCopilotUsage();
			const premium = usage.quota_snapshots.premium_interactions;
			const premiumTotal = premium.entitlement;
			const premiumUsed = premiumTotal - premium.remaining;
			const premiumPercentUsed = premiumTotal > 0 ? premiumUsed / premiumTotal * 100 : 0;
			const premiumPercentRemaining = premium.percent_remaining;
			function summarizeQuota(name, snap) {
				if (!snap) return `${name}: N/A`;
				const total = snap.entitlement;
				const used = total - snap.remaining;
				const percentUsed = total > 0 ? used / total * 100 : 0;
				const percentRemaining = snap.percent_remaining;
				return `${name}: ${used}/${total} used (${percentUsed.toFixed(1)}% used, ${percentRemaining.toFixed(1)}% remaining)`;
			}
			const premiumLine = `Premium: ${premiumUsed}/${premiumTotal} used (${premiumPercentUsed.toFixed(1)}% used, ${premiumPercentRemaining.toFixed(1)}% remaining)`;
			const chatLine = summarizeQuota("Chat", usage.quota_snapshots.chat);
			const completionsLine = summarizeQuota("Completions", usage.quota_snapshots.completions);
			consola.box(`Copilot Usage (plan: ${usage.copilot_plan})\nQuota resets: ${usage.quota_reset_date}\n\nQuotas:\n  ${premiumLine}\n  ${chatLine}\n  ${completionsLine}`);
		} catch (err) {
			consola.error("Failed to fetch Copilot usage:", err);
			process.exit(1);
		}
	}
});

//#endregion
export { checkUsage };
//# sourceMappingURL=check-usage-56cYKSRL.js.map