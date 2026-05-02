/**
 * Model ID aliases.
 *
 * Some upstream Copilot model IDs use names that Claude Code does not
 * recognize as 1M-context models. Claude Code's heuristic for enabling the
 * 1M-context code path requires the model ID to *end* with `-1m` (e.g.
 * `claude-opus-4.6-1m`, `claude-sonnet-4-5-1m`). IDs like
 * `claude-opus-4.7-1m-internal` have the `-1m` in the middle and therefore
 * fall back to the default 200k window even though the underlying model
 * supports 1M tokens.
 *
 * To work around this we expose an *alias* ID to the client (Claude Code)
 * that follows the `-1m` convention, and translate it back to the real
 * upstream ID before forwarding requests to GitHub Copilot. The alias is
 * surfaced in:
 *   1. `/v1/models` (so the picker / IDE can see it)
 *   2. The `--claude-code` env-script generator (so the env vars use it)
 * And translated back in:
 *   3. `findEndpointModel` (the central resolver used by every route)
 *
 * Add new entries to {@link UPSTREAM_TO_ALIAS} as needed.
 */

/** Map of real upstream Copilot model ID -> client-facing alias ID. */
export const UPSTREAM_TO_ALIAS: Readonly<Record<string, string>> = {
  "claude-opus-4.7-1m-internal": "claude-opus-4-7-internal-1m",
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
