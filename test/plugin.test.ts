import { describe, expect, it, vi } from "vitest"

import type { V2Context } from "../src/adapter/opencode.ts"
import plugin from "../src/index.ts"

function createFakeContext() {
  const registered: Array<{ name: string; callback: (payload: never) => Promise<unknown> }> = []
  const context = {
    location: { directory: "/repo/project" },
    session: {
      command: vi.fn(async () => undefined),
      prompt: vi.fn(async () => undefined),
      interrupt: vi.fn(async () => undefined),
      get: vi.fn(async () => ({ id: "s1" })),
    },
    tool: {
      hook: vi.fn(async (name: string, callback: (payload: never) => Promise<unknown>) => {
        registered.push({ name, callback })
        return { unregister: vi.fn() }
      }),
    },
    event: {
      subscribe: () =>
        (async function* () {
          // no events in tests
        })(),
    },
  }
  return { context, registered }
}

describe("plugin", () => {
  it("exports a v2 plugin definition with id and setup", () => {
    expect(plugin.id).toBe("opencode-yaml-hooks")
    expect(typeof plugin.setup).toBe("function")
  })

  it("registers tool hooks during setup", async () => {
    const { context, registered } = createFakeContext()
    await plugin.setup(context as unknown as V2Context)
    expect(registered.map((entry) => entry.name)).toEqual(["execute.before", "execute.after"])
  })
})
