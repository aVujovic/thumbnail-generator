import { describe, it, expect, vi } from 'vitest';
import { FakeLogger } from '@thumbnailer/domain/fakes';
import { serializeVideoJob, type ThumbnailResult, type VideoJob } from '@thumbnailer/contracts';
import { KafkaUtils, type KafkaConnection, type Signals } from '@thumbnailer/core';
import type { EachMessagePayload } from 'kafkajs';
import { VideoJobConsumer } from './VideoJobConsumer.job.js';
import type { ThumbnailGenerator, GenerateResult } from '../thumbnail/ThumbnailGenerator.js';
import type { IResultRepository } from '../repository/ResultRepository.js';

const job: VideoJob = { path: '/data/clip.mp4', size: 100, discoveredAt: 1 };

/**
 * Drives a VideoJobConsumer far enough to capture the eachMessage handler it
 * registers, so we can feed it messages and assert commit behaviour — no real
 * Kafka. Returns the handler plus spies on the underlying consumer.
 */
async function harness(generateResult: GenerateResult | Error) {
  const commitOffsets = vi.fn().mockResolvedValue(undefined);
  let handler!: (p: EachMessagePayload) => Promise<void>;

  const consumer = {
    subscribe: vi.fn().mockResolvedValue(undefined),
    stop: vi.fn().mockResolvedValue(undefined),
    run: vi.fn(async (opts: { eachMessage: (p: EachMessagePayload) => Promise<void> }) => {
      handler = opts.eachMessage;
    }),
    commitOffsets,
  };

  const kafka = { consumer } as unknown as KafkaConnection;
  const logger = new FakeLogger();
  const signals = { onTerm: vi.fn() } as unknown as Signals;

  const generator = {
    generate: vi.fn(async () => {
      if (generateResult instanceof Error) throw generateResult;
      return generateResult;
    }),
  } as unknown as ThumbnailGenerator;

  const saved: ThumbnailResult[] = [];
  const resultRepository: IResultRepository = {
    save: vi.fn(async (r: ThumbnailResult) => void saved.push(r)),
  };

  const consumerJob = new VideoJobConsumer(
    {
      logger,
      config: {
        topic: 'video-jobs',
        outputRoot: '/out',
        thumbnailFormat: 'jpg',
        kafka: { consumer: { groupId: 'g' } },
      } as never,
      kafka,
      kafkaUtils: new KafkaUtils({ logger }),
      signals,
      thumbnailGenerator: generator,
      resultRepository,
    },
    () => 999, // fixed clock for processedAt
  );

  await consumerJob.start();
  return { handler, commitOffsets, generator, logger, signals, saved };
}

function payload(value: Buffer | null, offset = '5'): EachMessagePayload {
  return {
    topic: 'video-jobs',
    partition: 0,
    message: { value, offset },
  } as unknown as EachMessagePayload;
}

describe('VideoJobConsumer commit semantics', () => {
  it('commits offset+1 after a successful generation (write-ahead)', async () => {
    const { handler, commitOffsets, generator } = await harness({
      status: 'generated',
      outputPath: '/out/data/clip.mp4.jpg',
      attempts: 1,
    });

    await handler(payload(serializeVideoJob(job), '5'));

    expect(generator.generate).toHaveBeenCalledOnce();
    expect(commitOffsets).toHaveBeenCalledWith([
      { topic: 'video-jobs', partition: 0, offset: '6' }, // offset + 1
    ]);
  });

  it('commits when generation is skipped (idempotent no-op)', async () => {
    const { handler, commitOffsets } = await harness({
      status: 'skipped',
      outputPath: '/out/data/clip.mp4.jpg',
      reason: 'exists',
    });

    await handler(payload(serializeVideoJob(job)));
    expect(commitOffsets).toHaveBeenCalledOnce();
  });

  it('commits after an exhausted failure to avoid a redelivery loop', async () => {
    const { handler, commitOffsets, logger } = await harness({
      status: 'failed',
      outputPath: '/out/data/clip.mp4.jpg',
      attempts: 3,
      reason: 'exited with code 1',
    });

    await handler(payload(serializeVideoJob(job)));

    expect(commitOffsets).toHaveBeenCalledOnce();
    expect(logger.lines.some((l) => l.message?.includes('after retries'))).toBe(true);
  });

  it('skips and commits a poison message without invoking the generator', async () => {
    const { handler, commitOffsets, generator } = await harness({
      status: 'generated',
      outputPath: '/x',
      attempts: 1,
    });

    await handler(payload(Buffer.from('not json'))); // poison

    expect(generator.generate).not.toHaveBeenCalled();
    expect(commitOffsets).toHaveBeenCalledOnce(); // committed so we don't re-read it
  });

  it('does NOT commit when generation throws (crash → redeliver)', async () => {
    const { handler, commitOffsets } = await harness(new Error('unexpected crash'));

    await expect(handler(payload(serializeVideoJob(job)))).rejects.toThrow('unexpected crash');
    expect(commitOffsets).not.toHaveBeenCalled(); // offset stays → message redelivered
  });

  it('registers a graceful-shutdown teardown via signals', async () => {
    const { signals } = await harness({ status: 'skipped', outputPath: '/x', reason: 'exists' });
    expect(signals.onTerm).toHaveBeenCalledOnce();
  });
});

describe('VideoJobConsumer result recording', () => {
  it('records a full result row on success', async () => {
    const { handler, saved } = await harness({
      status: 'generated',
      outputPath: '/out/data/clip.mp4.jpg',
      attempts: 2,
    });

    await handler(payload(serializeVideoJob(job)));

    expect(saved).toHaveLength(1);
    expect(saved[0]).toEqual({
      videoPath: '/data/clip.mp4',
      outputPath: '/out/data/clip.mp4.jpg',
      size: 100,
      discoveredAt: 1,
      format: 'jpg',
      status: 'generated',
      attempts: 2,
      processedAt: 999,
    });
  });

  it('records a skipped result with attempts=1 and no error', async () => {
    const { handler, saved } = await harness({
      status: 'skipped',
      outputPath: '/out/data/clip.mp4.jpg',
      reason: 'exists',
    });

    await handler(payload(serializeVideoJob(job)));

    expect(saved[0]).toMatchObject({ status: 'skipped', attempts: 1 });
    expect(saved[0]).not.toHaveProperty('error');
  });

  it('records a failed result including the error reason', async () => {
    const { handler, saved } = await harness({
      status: 'failed',
      outputPath: '/out/data/clip.mp4.jpg',
      attempts: 3,
      reason: 'exited with code 234',
    });

    await handler(payload(serializeVideoJob(job)));

    expect(saved[0]).toMatchObject({
      status: 'failed',
      attempts: 3,
      error: 'exited with code 234',
    });
  });

  it('does not record a result for a poison message', async () => {
    const { handler, saved } = await harness({ status: 'generated', outputPath: '/x', attempts: 1 });
    await handler(payload(Buffer.from('not json')));
    expect(saved).toHaveLength(0);
  });
});
