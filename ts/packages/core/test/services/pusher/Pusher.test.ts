import { afterEach, describe, expect, it, vi } from 'vitest';
import { PusherService } from '../../../src/services/pusher/Pusher';
import type { PusherClient } from '../../../src/types/pusher.types';

type EventCallback = (data: Record<string, unknown>) => void;

const bindChunkedEvent = () => {
  const handlers = new Map<string, EventCallback>();
  const channel = {
    bind: vi.fn((event: string, callback: EventCallback) => {
      handlers.set(event, callback);
    }),
  } as unknown as PusherClient;
  const callback = vi.fn();
  const service = Object.create(PusherService.prototype) as PusherService;

  const cleanup = (
    service as unknown as {
      bindWithChunking: (
        channel: PusherClient,
        event: string,
        callback: EventCallback
      ) => () => void;
    }
  ).bindWithChunking(channel, 'trigger_to_client', callback);

  return {
    callback,
    cleanup,
    emitChunk: handlers.get('chunked-trigger_to_client')!,
  };
};

describe('PusherService chunk reassembly', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('expires an incomplete event instead of retaining it indefinitely', () => {
    vi.useFakeTimers();
    const { callback, cleanup, emitChunk } = bindChunkedEvent();

    emitChunk({ id: 'evt-1', index: 0, chunk: '{"ok":', final: false });
    emitChunk({ id: 'evt-1', index: 2, chunk: '}', final: true });

    vi.advanceTimersByTime(60_001);
    emitChunk({ id: 'evt-1', index: 1, chunk: 'true', final: false });

    expect(callback).not.toHaveBeenCalled();
    cleanup();
  });

  it('waits for every chunk through the final index before dispatching', () => {
    const { callback, cleanup, emitChunk } = bindChunkedEvent();

    emitChunk({ id: 'evt-1', index: 0, chunk: '{"ok":', final: false });
    emitChunk({ id: 'evt-1', index: 2, chunk: '}', final: true });

    expect(callback).not.toHaveBeenCalled();

    emitChunk({ id: 'evt-1', index: 1, chunk: 'true', final: false });

    expect(callback).toHaveBeenCalledOnce();
    expect(callback).toHaveBeenCalledWith({ ok: true });
    cleanup();
  });

  it('rejects out-of-range indices without poisoning a later valid event', () => {
    const { callback, cleanup, emitChunk } = bindChunkedEvent();

    emitChunk({ id: 'evt-1', index: 1001, chunk: 'ignored', final: false });
    emitChunk({ id: 'evt-1', index: 0, chunk: '{"ok":true}', final: true });

    expect(callback).toHaveBeenCalledOnce();
    expect(callback).toHaveBeenCalledWith({ ok: true });
    cleanup();
  });

  it('evicts the oldest event when the pending-event cap is reached', () => {
    const { callback, cleanup, emitChunk } = bindChunkedEvent();

    emitChunk({ id: 'oldest', index: 0, chunk: '{"ok":', final: false });
    for (let index = 0; index < 100; index += 1) {
      emitChunk({ id: `evt-${index}`, index: 0, chunk: 'pending', final: false });
    }
    emitChunk({ id: 'oldest', index: 1, chunk: 'true}', final: true });

    expect(callback).not.toHaveBeenCalled();
    cleanup();
  });
});
