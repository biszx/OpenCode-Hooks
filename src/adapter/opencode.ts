import type { Hooks, PluginInput } from "@opencode-ai/plugin"

import { createHooksRuntime } from "../core/runtime.js"

/**
 * Structural subset of `@opencode/plugin` v2's setup context — only the
 * domains this plugin uses, so the package gains no runtime dependency.
 */
export interface V2Context {
  readonly location: { readonly directory: string }
  readonly session: V2SessionDomain
  readonly tool: V2ToolDomain
  readonly event: { subscribe(): AsyncIterable<V2Event> }
}

export interface V2SessionDomain {
  command(input: { sessionID: string; name: { name: string; text: string } }): Promise<unknown>
  prompt(input: { sessionID: string; text: string }): Promise<unknown>
  interrupt(input: { sessionID: string }): Promise<unknown>
  get(input: { sessionID: string }): Promise<unknown>
}

export interface V2ToolDomain {
  hook(
    name: "execute.before" | "execute.after",
    callback: (payload: V2ToolHookPayload) => Promise<unknown>,
  ): Promise<unknown>
}

export interface V2ToolHookPayload {
  readonly tool: string
  readonly sessionID: string
  readonly id: string
  input: unknown
  readonly status?: "completed" | "error"
  readonly result?: unknown
  readonly error?: unknown
}

export interface V2Event {
  readonly id: string
  readonly type: string
  readonly data?: Record<string, unknown>
  readonly location?: { readonly directory?: string }
}

export async function setup(context: V2Context): Promise<void> {
  const directory = context.location.directory
  const hooks = await createHooksRuntime(createV1Input(context))
  await registerToolHooks(hooks, context)
  startEventPump(hooks, context, directory)
}

function createV1Input(context: V2Context): PluginInput {
  // The hooks runtime consumes the v1 PluginInput surface: client + directory.
  return {
    client: createSessionClient(context),
    directory: context.location.directory,
  } as unknown as PluginInput
}

function createSessionClient(context: V2Context) {
  return {
    session: {
      async command(request: { path: { id: string }; body?: { command?: string; arguments?: string } }) {
        await context.session.command({
          sessionID: request.path.id,
          name: { name: String(request.body?.command ?? ""), text: String(request.body?.arguments ?? "") },
        })
        return { data: {} }
      },
      async prompt(request: { path: { id: string }; body?: { parts?: unknown } }) {
        await context.session.prompt({ sessionID: request.path.id, text: textParts(request.body?.parts) })
        return { data: {} }
      },
      async abort(request: { path: { id: string } }) {
        await context.session.interrupt({ sessionID: request.path.id })
        return {}
      },
      async get(request: { path: { id: string } }) {
        return { data: { info: await context.session.get({ sessionID: request.path.id }) } }
      },
    },
  }
}

function textParts(parts: unknown): string {
  if (!Array.isArray(parts)) {
    return ""
  }

  return parts
    .map((part) => {
      const record = part as { type?: unknown; text?: unknown } | null
      return record && record.type === "text" && typeof record.text === "string" ? record.text : ""
    })
    .filter(Boolean)
    .join("\n")
}

async function registerToolHooks(hooks: Hooks, context: V2Context): Promise<void> {
  const before = hooks["tool.execute.before"]
  if (before) {
    await context.tool.hook("execute.before", async (payload) => {
      const output = { args: payload.input }
      // The runtime signals a blocked tool call by throwing; let it propagate.
      await before({ tool: payload.tool, sessionID: payload.sessionID, callID: payload.id }, output)
      if (output.args !== payload.input) {
        payload.input = output.args
      }
    })
  }

  const after = hooks["tool.execute.after"]
  if (after) {
    await context.tool.hook("execute.after", async (payload) => {
      // The runtime reads only the input side of an after hook; metadata keeps the result reachable.
      await after(
        { tool: payload.tool, sessionID: payload.sessionID, callID: payload.id, args: payload.input },
        { title: "", output: "", metadata: payload.status === "completed" ? payload.result : payload.error },
      )
    })
  }
}

function startEventPump(hooks: Hooks, context: V2Context, directory: string): void {
  const event = hooks.event
  if (!event) {
    return
  }

  void (async () => {
    try {
      for await (const ev of context.event.subscribe()) {
        if (ev.location?.directory && ev.location.directory !== directory) {
          continue
        }

        const properties =
          ev.type === "session.created"
            ? { info: { id: ev.data?.sessionID, parentID: ev.data?.parentID } }
            : ev.type === "session.deleted"
              ? { info: { id: ev.data?.sessionID } }
              : ev.data

        try {
          // v2 events carry dynamic type/data; the runtime dispatches them loosely.
          await event({ event: { id: ev.id, type: ev.type, properties } } as Parameters<typeof event>[0])
        } catch (error) {
          console.error(`[opencode-yaml-hooks] event hook failed: ${error}`)
        }
      }
    } catch {
      // Subscription closed (plugin unload) — stop pumping.
    }
  })()
}
