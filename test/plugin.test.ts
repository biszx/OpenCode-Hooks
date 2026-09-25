import { mkdirSync, readFileSync, writeFileSync } from "node:fs"
import os from "node:os"
import path from "node:path"

import { describe, expect, it, vi } from "vitest"

import type { V2Context } from "../src/adapter/opencode.ts"
import plugin from "../src/index.ts"

function createProjectDir(): string {
  const projectDir = path.join(os.tmpdir(), `opencode-yaml-hooks-${Date.now()}-${Math.random().toString(16).slice(2)}`)
  mkdirSync(path.join(projectDir, ".opencode", "hook"), { recursive: true })
  return projectDir
}

function writeHooks(projectDir: string, yaml: string): void {
  writeFileSync(path.join(projectDir, ".opencode", "hook", "hooks.yaml"), yaml, "utf8")
}

async function readFileWhenPresent(filePath: string): Promise<string> {
  const deadline = Date.now() + 4000
  for (;;) {
    try {
      return readFileSync(filePath, "utf8")
    } catch {
      if (Date.now() >= deadline) {
        throw new Error(`timed out waiting for ${filePath}`)
      }
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
  }
}

function createFakeContext(directory = "/repo/project") {
  const registered: Array<{ name: string; callback: (payload: never) => Promise<unknown> }> = []
  const context = {
    location: { directory },
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

  it("propagates a blocked tool call from a tool.before hook", async () => {
    const projectDir = createProjectDir()
    writeHooks(
      projectDir,
      `hooks:
  - event: tool.before.*
    actions:
      - bash: 'echo "blocked:adapter-test" >&2; exit 2'
`,
    )

    const { context, registered } = createFakeContext(projectDir)
    await plugin.setup(context as unknown as V2Context)
    const before = registered.find((entry) => entry.name === "execute.before")

    await expect(
      before?.callback({ tool: "greet", sessionID: "session-1", id: "call-1", input: {} } as never),
    ).rejects.toThrow("blocked:adapter-test")
  })

  it("fires session.created hooks only for events in the plugin directory", async () => {
    const projectDir = createProjectDir()
    writeHooks(
      projectDir,
      `hooks:
  - event: session.created
    actions:
      - bash: 'echo "$OPENCODE_SESSION_ID" >> "${projectDir}/fired.log"'
`,
    )

    const { context, registered } = createFakeContext(projectDir)
    context.event.subscribe = () =>
      (async function* () {
        yield {
          id: "e1",
          type: "session.created",
          data: { sessionID: "mismatched" },
          location: { directory: "/other/project" },
        }
        yield {
          id: "e2",
          type: "session.created",
          data: { sessionID: "matched", parentID: "parent-1" },
          location: { directory: projectDir },
        }
      })()

    await plugin.setup(context as unknown as V2Context)
    expect(registered.map((entry) => entry.name)).toEqual(["execute.before", "execute.after"])

    const content = await readFileWhenPresent(path.join(projectDir, "fired.log"))
    expect(content.trim().split("\n")).toEqual(["matched"])
  })

  it("logs when the event stream ends with an error", async () => {
    const projectDir = createProjectDir()
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {})
    const { context } = createFakeContext(projectDir)
    context.event.subscribe = () =>
      (async function* () {
        throw new Error("stream broke")
      })()

    await plugin.setup(context as unknown as V2Context)

    await vi.waitFor(() => {
      expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("event stream ended: Error: stream broke"))
    })
    errorSpy.mockRestore()
  })
})
