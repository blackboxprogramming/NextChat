// using tauri command to send request
// see src-tauri/src/stream.rs, and src-tauri/src/main.rs
// 1. invoke('stream_fetch', {url, method, headers, body}), get response with headers.
// 2. listen event: `stream-response` multi times to get body

type ResponseEvent = {
  id: number;
  payload: {
    request_id: number;
    status?: number;
    chunk?: number[];
  };
};

type StreamResponse = {
  request_id: number;
  status: number;
  status_text: string;
  headers: Record<string, string>;
};

export function fetch(url: string, options?: RequestInit): Promise<Response> {
  const tauri = window.__TAURI__;
  if (!tauri) {
    return window.fetch(url, options);
  }

  const {
    signal,
    method = "GET",
    headers: _headers = {},
    body = [],
  } = options || {};
  const abortReason = () =>
    signal?.reason !== undefined
      ? signal.reason
      : new DOMException("The request was aborted", "AbortError");
  if (signal?.aborted) {
    return Promise.reject(abortReason());
  }

  let unlisten: (() => void) | undefined;
  let setRequestId!: (requestId: number | undefined) => void;
  const requestIdPromise = new Promise<number | undefined>((resolve) => {
    setRequestId = resolve;
  });
  let bodyController!: ReadableStreamDefaultController<Uint8Array>;
  let closed = false;
  const cleanup = () => {
    unlisten?.();
    unlisten = undefined;
    signal?.removeEventListener("abort", onAbort);
    // Release events waiting for headers if invocation fails or is aborted.
    setRequestId(undefined);
  };
  const close = () => {
    if (closed) return;
    closed = true;
    cleanup();
    bodyController.close();
  };
  const fail = (reason: unknown) => {
    if (closed) return;
    closed = true;
    cleanup();
    bodyController.error(reason);
  };
  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      bodyController = controller;
    },
    cancel() {
      if (closed) return;
      closed = true;
      cleanup();
    },
  });

  let rejectAbort!: (reason: unknown) => void;
  const aborted = new Promise<never>((_, reject) => {
    rejectAbort = reject;
  });
  function onAbort() {
    const reason = abortReason();
    fail(reason);
    rejectAbort(reason);
  }
  signal?.addEventListener("abort", onAbort, { once: true });

  const request = async () => {
    try {
      // Native chunks can arrive before invoke resolves with the request ID.
      // Attach the listener first, then defer delivery until that ID is known.
      // @ts-ignore event is supplied by the native Tauri bridge.
      const stopListening: () => void = await tauri.event.listen(
        "stream-response",
        (event: ResponseEvent) => {
          void requestIdPromise.then((requestId) => {
            const { request_id, chunk, status } = event.payload || {};
            if (closed || requestId === undefined || requestId !== request_id) {
              return;
            }
            if (chunk) {
              bodyController.enqueue(new Uint8Array(chunk));
            } else if (status === 0) {
              close();
            }
          });
        },
      );
      if (closed) {
        stopListening();
        throw abortReason();
      }
      unlisten = stopListening;

      const headers: Record<string, string> = {
        Accept: "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        "User-Agent": navigator.userAgent,
      };
      for (const item of new Headers(_headers || {})) {
        headers[item[0]] = item[1];
      }
      const result: StreamResponse = await tauri.invoke("stream_fetch", {
        method: method.toUpperCase(),
        url,
        headers,
        // TODO FormData
        body:
          typeof body === "string"
            ? Array.from(new TextEncoder().encode(body))
            : [],
      });
      if (signal?.aborted) {
        throw abortReason();
      }
      const { request_id, status, status_text: statusText } = result;
      setRequestId(request_id);
      return new Response(readable, {
        status,
        statusText,
        headers: result.headers,
      });
    } catch (error) {
      if (signal?.aborted) {
        throw abortReason();
      }
      close();
      console.error("stream error", error);
      return new Response("", { status: 599 });
    }
  };

  return Promise.race([request(), aborted]);
}
