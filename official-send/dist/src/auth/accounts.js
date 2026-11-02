// Minimal local stub for standalone send test, NOT part of official plugin.
// Official code reads bot_agent / routeTag from OpenClaw config; standalone
// run has no OpenClaw config, so defaults are used: bot_agent "OpenClaw", no routeTag.
export function loadConfigBotAgent() { return undefined; }
export function loadConfigRouteTag() { return undefined; }
