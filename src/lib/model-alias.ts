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
export const UPSTREAM_TO_ALIAS: Readonly<Record<string, string>> = {
  "claude-opus-4.7-1m-internal": "claude-opus-4.7-internal[1m]",
}

/** Reverse: client-facing alias ID -> real upstream Copilot model ID. */
export const ALIAS_TO_UPSTREAM: Readonly<Record<string, string>> =
  Object.fromEntries(
    Object.entries(UPSTREAM_TO_ALIAS).map(([upstream, alias]) => [
      alias,
      upstream,
    ]),
  )

/** Returns the alias for a given upstream id, or undefined if none. */
export const getAliasForUpstream = (upstreamId: string): string | undefined =>
  UPSTREAM_TO_ALIAS[upstreamId]

/** Returns the real upstream id for a given alias, or undefined if not an alias. */
export const getUpstreamForAlias = (aliasId: string): string | undefined =>
  ALIAS_TO_UPSTREAM[aliasId]

/** Convenience: replace alias -> upstream if applicable, else identity. */
export const resolveToUpstream = (id: string): string =>
  ALIAS_TO_UPSTREAM[id] ?? id

/** Convenience: replace upstream -> alias if applicable, else identity. */
export const exposeAlias = (id: string): string => UPSTREAM_TO_ALIAS[id] ?? id
