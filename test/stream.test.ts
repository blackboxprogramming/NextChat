/**
 * @jest-environment node
 */
import assert from "node:assert/strict";

import { fetch as streamFetch } from "../app/utils/stream";

type Metadata = {
  request_id: number;
  status: number;
  status_text: string;
  headers: Record<string, string>;
};

type Payload = {
  request_id: number;
  status?: number;
  chunk?: number[];
};

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

const metadata: Metadata = {
  request_id: 7,
  status: 200,
  status_text: "OK",
  headers: { "Content-Type": "text/plain" },
};

class NativeBridge {
  registration = deferred<void>();
  response = deferred<Metadata>();
  listeners = new Set<(event: { payload: Payload }) => void>();
  unlistens = 0;
  invocations: unknown[][] = [];
  onInvoke = () => this.response.resolve(metadata);

  constructor() {
    this.registration.resolve();
  }

  event = {
    listen: async (
      _name: string,
      callback: (event: { payload: Payload }) => void,
    ) => {
      await this.registration.promise;
      this.listeners.add(callback);
      return () => {
        this.unlistens += 1;
        this.listeners.delete(callback);
      };
    },
  };

  invoke = (...args: unknown[]) => {
    this.invocations.push(args);
    this.onInvoke();
    return this.response.promise;
  };

  emit(payload: Payload) {
    this.listeners.forEach((callback) => callback({ payload }));
  }
}

async function within<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("stream did not settle")),
          1000,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function tick() {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

async function withWindow(
  value: unknown,
  run: () => Promise<void>,
): Promise<void> {
  const windowDescriptor = Object.getOwnPropertyDescriptor(
    globalThis,
    "window",
  );
  const navigatorDescriptor = Object.getOwnPropertyDescriptor(
    globalThis,
    "navigator",
  );
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value,
  });
  Object.defineProperty(globalThis, "navigator", {
    configurable: true,
    value: { userAgent: "native-stream-test" },
  });
  try {
    await run();
  } finally {
    if (windowDescriptor) {
      Object.defineProperty(globalThis, "window", windowDescriptor);
    } else {
      Reflect.deleteProperty(globalThis, "window");
    }
    if (navigatorDescriptor) {
      Object.defineProperty(globalThis, "navigator", navigatorDescriptor);
    } else {
      Reflect.deleteProperty(globalThis, "navigator");
    }
  }
}

async function withNative(
  run: (bridge: NativeBridge) => Promise<void>,
): Promise<void> {
  const bridge = new NativeBridge();
  await withWindow({ __TAURI__: bridge }, async () => {
    try {
      await run(bridge);
    } finally {
      bridge.registration.resolve();
      bridge.response.resolve(metadata);
      bridge.emit({ request_id: 7, status: 0 });
      await tick();
    }
  });
}

describe("native streaming fetch lifecycle", () => {
  test("browser fetch preserves arguments and response", async () => {
    const options = { method: "POST", body: "payload" };
    const expected = new Response("browser");
    let received: unknown[] = [];
    await withWindow(
      {
        fetch: (...args: unknown[]) => {
          received = args;
          return Promise.resolve(expected);
        },
      },
      async () => {
        assert.equal(await streamFetch("/browser", options), expected);
        assert.deepEqual(received, ["/browser", options]);
      },
    );
  });

  test("pre-aborted fetch starts no native work", async () => {
    await withNative(async (bridge) => {
      const controller = new AbortController();
      const reason = new Error("already stopped");
      controller.abort(reason);
      await assert.rejects(
        within(streamFetch("/native", { signal: controller.signal })),
        (error) => error === reason,
      );
      assert.equal(bridge.listeners.size, 0);
      assert.equal(bridge.invocations.length, 0);
    });
  });

  test("default aborts keep their AbortError reason", async () => {
    await withNative(async (bridge) => {
      const controller = new AbortController();
      controller.abort();
      await assert.rejects(
        streamFetch("/native", { signal: controller.signal }),
        (error) => error === controller.signal.reason,
      );
      assert.equal(bridge.invocations.length, 0);
    });
  });

  test("an explicit null abort reason stays null", async () => {
    await withNative(async () => {
      const controller = new AbortController();
      controller.abort(null);
      await assert.rejects(
        streamFetch("/native", { signal: controller.signal }),
        (error) => error === null,
      );
    });
  });

  test("registration precedes invoke and retains queued bytes", async () => {
    await withNative(async (bridge) => {
      bridge.onInvoke = () => {
        assert.equal(bridge.listeners.size, 1);
        bridge.emit({ request_id: 999, chunk: [88] });
        bridge.emit({ request_id: 7, chunk: [65, 226] });
        bridge.emit({ request_id: 7, chunk: [] });
        bridge.emit({ request_id: 7, chunk: [130, 172] });
        bridge.emit({ request_id: 7, status: 0 });
        bridge.response.resolve(metadata);
      };
      const response = await within(streamFetch("/native"));
      assert.equal(await within(response.text()), "A€");
      assert.equal(bridge.unlistens, 1);
      assert.equal(bridge.listeners.size, 0);
    });
  });

  test("invoke failure returns 599 and removes the listener", async () => {
    await withNative(async (bridge) => {
      bridge.onInvoke = () => {
        bridge.response.reject(new Error("native failed"));
      };
      const response = await within(streamFetch("/native"));
      assert.equal(response.status, 599);
      assert.equal(await response.text(), "");
      assert.equal(bridge.unlistens, 1);
      assert.equal(bridge.listeners.size, 0);
    });
  });

  test("listener failure returns 599 without invoking", async () => {
    await withNative(async (bridge) => {
      bridge.registration = deferred<void>();
      const responsePromise = streamFetch("/native");
      bridge.registration.reject(new Error("listener failed"));
      const response = await within(responsePromise);
      assert.equal(response.status, 599);
      assert.equal(bridge.invocations.length, 0);
    });
  });

  test("abort handles a listener that registers late", async () => {
    await withNative(async (bridge) => {
      bridge.registration = deferred<void>();
      const controller = new AbortController();
      const reason = new Error("stopped during registration");
      const pending = streamFetch("/native", { signal: controller.signal });
      controller.abort(reason);
      await assert.rejects(within(pending), (error) => error === reason);
      bridge.registration.resolve();
      await tick();
      assert.equal(bridge.invocations.length, 0);
      assert.equal(bridge.unlistens, 1);
      assert.equal(bridge.listeners.size, 0);
    });
  });

  test("abort before headers rejects and removes the listener", async () => {
    await withNative(async (bridge) => {
      bridge.onInvoke = () => {};
      const controller = new AbortController();
      const reason = new Error("stopped before headers");
      const pending = streamFetch("/native", { signal: controller.signal });
      await tick();
      assert.equal(bridge.invocations.length, 1);
      controller.abort(reason);
      await assert.rejects(within(pending), (error) => error === reason);
      assert.equal(bridge.unlistens, 1);
      bridge.response.resolve(metadata);
      await tick();
      assert.equal(bridge.unlistens, 1);
    });
  });

  test("abort after headers errors the response body", async () => {
    await withNative(async (bridge) => {
      const controller = new AbortController();
      const response = await streamFetch("/native", {
        signal: controller.signal,
      });
      const reading = response.text();
      const reason = new Error("stopped while reading");
      controller.abort(reason);
      await assert.rejects(within(reading), (error) => error === reason);
      assert.equal(bridge.unlistens, 1);
      bridge.emit({ request_id: 7, chunk: [88] });
      assert.equal(bridge.listeners.size, 0);
    });
  });

  test("cancelling the response body releases the listener", async () => {
    await withNative(async (bridge) => {
      const response = await streamFetch("/native");
      await response.body!.cancel();
      assert.equal(bridge.unlistens, 1);
      assert.equal(bridge.listeners.size, 0);
    });
  });

  test("HTTP error status and body survive until native EOF", async () => {
    await withNative(async (bridge) => {
      bridge.onInvoke = () => {
        bridge.response.resolve({ ...metadata, status: 503 });
      };
      const response = await streamFetch("/native");
      bridge.emit({ request_id: 7, chunk: [98, 117, 115, 121] });
      bridge.emit({ request_id: 7, status: 0 });
      assert.equal(response.status, 503);
      assert.equal(await within(response.text()), "busy");
      assert.equal(bridge.unlistens, 1);
    });
  });

  test("native request encoding preserves headers and UTF-8", async () => {
    await withNative(async (bridge) => {
      const response = await streamFetch("/native", {
        method: "post",
        headers: { Authorization: "test-only-value" },
        body: "€",
      });
      const [command, payload] = bridge.invocations[0] as [
        string,
        { method: string; body: number[]; headers: Record<string, string> },
      ];
      assert.equal(command, "stream_fetch");
      assert.equal(payload.method, "POST");
      assert.deepEqual(payload.body, [226, 130, 172]);
      assert.equal(payload.headers.authorization, "test-only-value");
      bridge.emit({ request_id: 7, status: 0 });
      assert.equal(await within(response.text()), "");
    });
  });

  test("normal completion removes the abort listener", async () => {
    await withNative(async (bridge) => {
      const controller = new AbortController();
      let removals = 0;
      const remove = controller.signal.removeEventListener.bind(
        controller.signal,
      );
      controller.signal.removeEventListener = (
        type: string,
        listener: EventListenerOrEventListenerObject | null,
        options?: boolean | EventListenerOptions,
      ) => {
        if (type === "abort") removals += 1;
        remove(type, listener, options);
      };
      const response = await streamFetch("/native", {
        signal: controller.signal,
      });
      bridge.emit({ request_id: 7, status: 0 });
      assert.equal(await within(response.text()), "");
      assert.equal(removals, 1);
      controller.abort();
      assert.equal(bridge.unlistens, 1);
    });
  });
});
