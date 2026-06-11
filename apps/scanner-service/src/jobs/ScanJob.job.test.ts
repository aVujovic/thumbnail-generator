import { describe, it, expect, vi } from 'vitest';
import { FakeLogger } from '@thumbnailer/domain/fakes';
import type { Signals } from '@thumbnailer/core';
import type { FileInfo } from '@thumbnailer/domain';
import { ScanJob } from './ScanJob.job.js';
import type { FileScanner } from '../scanner/FileScanner.js';
import type { JobProducer } from '../producer/JobProducer.js';

const info: FileInfo = { name: 'v.mp4', isDirectory: false, size: 1, permissions: 0o644 };

/** A FileScanner stand-in that yields `count` videos lazily. */
function fakeScanner(count: number): FileScanner {
  return {
    async *walk() {
      for (let i = 0; i < count; i++) yield { path: `/v${i}.mp4`, info };
    },
  } as unknown as FileScanner;
}

/** Captures the onTerm handler so the test can fire shutdown deterministically. */
function fakeSignals() {
  let handler: (() => void | Promise<void>) | undefined;
  const signals = { onTerm: (h: () => void) => void (handler = h) } as unknown as Signals;
  return { signals, fire: () => handler?.() };
}

describe('ScanJob cancellation', () => {
  it('produces every video when not interrupted', async () => {
    const produce = vi.fn().mockResolvedValue(undefined);
    const { signals } = fakeSignals();
    const job = new ScanJob({
      logger: new FakeLogger(),
      config: { topic: 't', scanRoot: '/' } as never,
      fileScanner: fakeScanner(3),
      jobProducer: { produce } as unknown as JobProducer,
      signals,
    });

    await job.run();
    expect(produce).toHaveBeenCalledTimes(3);
  });

  it('stops producing after a shutdown signal mid-scan', async () => {
    const { signals, fire } = fakeSignals();
    let calls = 0;
    const produce = vi.fn().mockImplementation(async () => {
      calls++;
      if (calls === 2) fire(); // request shutdown after the 2nd produce
    });

    const job = new ScanJob({
      logger: new FakeLogger(),
      config: { topic: 't', scanRoot: '/' } as never,
      fileScanner: fakeScanner(10),
      jobProducer: { produce } as unknown as JobProducer,
      signals,
    });

    await job.run();
    // Loop checks `stopping` at the top of the next iteration → stops at 2.
    expect(produce).toHaveBeenCalledTimes(2);
  });

  it('swallows a producer error that races the disconnect while stopping', async () => {
    const { signals, fire } = fakeSignals();
    const produce = vi.fn().mockImplementation(async () => {
      fire(); // shutting down…
      throw new Error('The producer is disconnected'); // …and the send loses the race
    });

    const job = new ScanJob({
      logger: new FakeLogger(),
      config: { topic: 't', scanRoot: '/' } as never,
      fileScanner: fakeScanner(5),
      jobProducer: { produce } as unknown as JobProducer,
      signals,
    });

    // Must NOT throw — the disconnect-while-stopping is expected.
    await expect(job.run()).resolves.toBeUndefined();
  });

  it('still surfaces a genuine producer error when not stopping', async () => {
    const { signals } = fakeSignals();
    const produce = vi.fn().mockRejectedValue(new Error('broker exploded'));

    const job = new ScanJob({
      logger: new FakeLogger(),
      config: { topic: 't', scanRoot: '/' } as never,
      fileScanner: fakeScanner(5),
      jobProducer: { produce } as unknown as JobProducer,
      signals,
    });

    await expect(job.run()).rejects.toThrow('broker exploded');
  });
});
