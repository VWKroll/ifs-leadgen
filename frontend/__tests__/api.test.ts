import { describe, expect, it, vi, beforeEach } from "vitest";

// Mock fetch globally
const fetchMock = vi.fn();
vi.stubGlobal("fetch", fetchMock);

describe("streamChatResponse", () => {
  beforeEach(() => {
    fetchMock.mockReset();
  });

  it("calls fetch with correct URL and method", async () => {
    const { streamChatResponse } = await import("@/lib/api");

    // Create a stream that completes immediately
    const stream = new ReadableStream({
      start(controller) {
        const encoder = new TextEncoder();
        controller.enqueue(encoder.encode("event: response\ndata: {\"response_id\":\"r1\",\"message\":{\"role\":\"assistant\",\"content\":\"hi\",\"citations\":[]}}\n\n"));
        controller.close();
      },
    });

    fetchMock.mockResolvedValue({
      ok: true,
      body: stream,
      json: vi.fn(),
    });

    const onResponse = vi.fn();
    await streamChatResponse(
      { message: "hello", scope: "all" } as any,
      { onResponse },
    );

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toContain("/chat/responses/stream");
    expect(options.method).toBe("POST");
  });

  it("calls onDelta for delta events", async () => {
    const { streamChatResponse } = await import("@/lib/api");

    const stream = new ReadableStream({
      start(controller) {
        const encoder = new TextEncoder();
        controller.enqueue(encoder.encode("event: delta\ndata: {\"text\":\"Hello \"}\n\n"));
        controller.enqueue(encoder.encode("event: delta\ndata: {\"text\":\"world\"}\n\n"));
        controller.close();
      },
    });

    fetchMock.mockResolvedValue({ ok: true, body: stream, json: vi.fn() });

    const onDelta = vi.fn();
    await streamChatResponse({ message: "test", scope: "all" } as any, { onDelta });

    expect(onDelta).toHaveBeenCalledWith("Hello ");
    expect(onDelta).toHaveBeenCalledWith("world");
  });

  it("retries on 5xx errors", async () => {
    const { streamChatResponse } = await import("@/lib/api");

    // First call: 500 error, second call: success
    const stream = new ReadableStream({
      start(controller) {
        const encoder = new TextEncoder();
        controller.enqueue(encoder.encode("event: response\ndata: {\"response_id\":\"r1\",\"message\":{\"role\":\"assistant\",\"content\":\"ok\",\"citations\":[]}}\n\n"));
        controller.close();
      },
    });

    fetchMock
      .mockResolvedValueOnce({ ok: false, status: 502, body: null, json: vi.fn().mockResolvedValue({ detail: "bad gateway" }) })
      .mockResolvedValueOnce({ ok: true, body: stream, json: vi.fn() });

    const onResponse = vi.fn();
    await streamChatResponse({ message: "test", scope: "all" } as any, { onResponse });

    // Should have been called twice (1 retry)
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(onResponse).toHaveBeenCalledTimes(1);
  });

  it("throws on non-retryable 4xx errors", async () => {
    const { streamChatResponse } = await import("@/lib/api");

    fetchMock.mockResolvedValue({
      ok: false,
      status: 403,
      body: null,
      json: vi.fn().mockResolvedValue({ detail: "Forbidden" }),
    });

    await expect(
      streamChatResponse({ message: "test", scope: "all" } as any, {}),
    ).rejects.toThrow("Forbidden");
  });
});
