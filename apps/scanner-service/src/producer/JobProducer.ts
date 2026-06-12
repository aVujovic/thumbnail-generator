import { serializeVideoJob, type VideoJob } from '@thumbnailer/contracts';
import type { FileInfo, ILogger } from '@thumbnailer/domain';
import type { KafkaConnection } from '@thumbnailer/core';

interface ProducerConfig {
  topic: string;
}

/**
 * Maps a discovered video to a VideoJob and produces it onto the topic. The
 * file path is used as the message key so jobs for the same file land on the
 * same partition (per-file ordering, and even spread across partitions).
 *
 * Thin and injectable: the consumer of this class (ScanJob) doesn't know about
 * Kafka, and this class doesn't know about walking — single responsibility.
 */
export class JobProducer {
  private readonly kafka: KafkaConnection;
  private readonly logger: ILogger;
  private readonly config: ProducerConfig;
  private readonly now: () => number;

  /**
   * @param deps  Injected by awilix (kafka, logger, config).
   * @param now   Injectable clock for deterministic tests; not from the cradle,
   *              so awilix never tries to resolve it. Defaults to Date.now.
   */
  constructor(
    deps: { kafka: KafkaConnection; logger: ILogger; config: ProducerConfig },
    now: () => number = Date.now,
  ) {
    this.kafka = deps.kafka;
    this.logger = deps.logger;
    this.config = deps.config;
    this.now = now;
  }

  async produce(
    path: string,
    info: FileInfo,
    ctx: { scanId?: string; tenantId?: string } = {},
  ): Promise<void> {
    const job: VideoJob = {
      path,
      size: info.size,
      discoveredAt: this.now(),
      ...(ctx.scanId ? { scanId: ctx.scanId } : {}),
      ...(ctx.tenantId ? { tenantId: ctx.tenantId } : {}),
    };
    await this.kafka.producer!.send({
      topic: this.config.topic,
      messages: [{ key: job.path, value: serializeVideoJob(job) }],
    });
    this.logger.debug({ job }, 'produced video job');
  }
}

export default JobProducer;
