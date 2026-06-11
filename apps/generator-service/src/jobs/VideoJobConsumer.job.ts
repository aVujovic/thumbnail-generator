import { parseVideoJob, type ThumbnailResult, type VideoJob } from '@thumbnailer/contracts';
import type { ILogger } from '@thumbnailer/domain';
import type { KafkaConnection, KafkaUtils, Signals } from '@thumbnailer/core';
import type { GeneratorConfig } from '../config.schema.js';
import type { ThumbnailGenerator } from '../thumbnail/ThumbnailGenerator.js';
import type { IResultRepository } from '../repository/ResultRepository.js';

/**
 * Long-running consumer. Subscribes to the video-jobs topic and processes one
 * job at a time with the write-ahead commit pattern: the offset is committed
 * only AFTER the work reaches a terminal state, so a crash mid-work redelivers
 * the message.
 *
 * Collaborators arrive via constructor injection, so the job is testable with a
 * fake consumer + fake generator, no real Kafka.
 */
export class VideoJobConsumer {
  private readonly logger: ILogger;
  private readonly config: GeneratorConfig;
  private readonly kafka: KafkaConnection;
  private readonly kafkaUtils: KafkaUtils;
  private readonly signals: Signals;
  private readonly generator: ThumbnailGenerator;
  private readonly results: IResultRepository;
  private readonly now: () => number;

  private readonly pendingOffsets = new Map<string, string>();
  private running = false;

  constructor(
    deps: {
      logger: ILogger;
      config: GeneratorConfig;
      kafka: KafkaConnection;
      kafkaUtils: KafkaUtils;
      signals: Signals;
      thumbnailGenerator: ThumbnailGenerator;
      resultRepository: IResultRepository;
    },
    now: () => number = Date.now,
  ) {
    this.logger = deps.logger;
    this.config = deps.config;
    this.kafka = deps.kafka;
    this.kafkaUtils = deps.kafkaUtils;
    this.signals = deps.signals;
    this.generator = deps.thumbnailGenerator;
    this.results = deps.resultRepository;
    this.now = now;
  }

  async start(): Promise<void> {
    if (this.running) return;
    const consumer = this.kafka.consumer!;

    await consumer.subscribe({
      topic: this.config.topic,
      fromBeginning: this.config.fromBeginning,
    });

    // Register graceful teardown: stop the consumer on SIGINT/SIGTERM, in the
    // central ordered registry (core's signals provider).
    this.signals.onTerm(async () => {
      await consumer.stop();
      this.logger.info('🛑 consumer stopped');
    });

    this.logger.info(
      {
        topic: this.config.topic,
        groupId: this.config.kafka.consumer.groupId,
        outputRoot: this.config.outputRoot,
      },
      'generator-service started — waiting for jobs',
    );

    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        const offset = message.offset;
        this.kafkaUtils.trackOffset(this.pendingOffsets, topic, partition, offset);

        const job = parseVideoJob(
          this.kafkaUtils.parseJson(message, { topic, partition, offset }),
        );
        if (!job) {
          // poison message — already warned; commit so we don't re-read it.
          await this.kafkaUtils.commitPending(consumer, this.pendingOffsets);
          return;
        }

        await this.process(job, { partition, offset });

        // Write-ahead commit: only AFTER process() reaches a terminal state
        // (generated / skipped / failed-after-retries). A crash before this
        // line never commits, so the message is redelivered.
        await this.kafkaUtils.commitPending(consumer, this.pendingOffsets);
      },
    });

    this.running = true;
  }

  /**
   * Process a single job: generate its thumbnail, record the outcome to the
   * result repository (audit trail), and let the result decide what gets logged.
   * All three outcomes (generated / skipped / failed) are terminal and lead to a
   * commit by the caller — retries already happened inside the generator, so
   * redelivering a permanently-broken video would just loop.
   */
  async process(job: VideoJob, ctx: { partition: number; offset: string }): Promise<void> {
    const result = await this.generator.generate(job);

    const record: ThumbnailResult = {
      videoPath: job.path,
      outputPath: result.outputPath,
      size: job.size,
      discoveredAt: job.discoveredAt,
      format: this.config.thumbnailFormat,
      status: result.status,
      attempts: result.status === 'skipped' ? 1 : result.attempts,
      ...(result.status === 'failed' ? { error: result.reason } : {}),
      processedAt: this.now(),
    };
    await this.results.save(record);

    if (result.status === 'failed') {
      this.logger.error(
        { path: job.path, ...ctx, reason: result.reason },
        'job failed after retries — committing to avoid a redelivery loop',
      );
    }
  }
}

export default VideoJobConsumer;
