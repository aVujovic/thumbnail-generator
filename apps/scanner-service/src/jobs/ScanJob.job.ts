import type { ILogger } from '@thumbnailer/domain';
import type { Signals } from '@thumbnailer/core';
import type { ScannerConfig } from '../config.schema.js';
import type { FileScanner } from '../scanner/FileScanner.js';
import type { JobProducer } from '../producer/JobProducer.js';

/**
 * The scanner's unit of work: walk the file system (FileScanner) and produce
 * one VideoJob per discovered video (JobProducer). This class only orchestrates
 * — it owns no walking or Kafka logic, so it stays small and the pieces are
 * independently testable.
 *
 * Videos are produced one at a time as the walk yields them (streaming): memory
 * stays flat and the generator service can start working before the scan ends.
 *
 * The scan is cancellable: on SIGINT/SIGTERM a `stopping` flag is flipped, the
 * walk loop breaks at the next iteration, and we stop producing before the
 * Kafka producer is torn down — so we never `send()` on a disconnected client.
 */
export class ScanJob {
  private readonly logger: ILogger;
  private readonly config: ScannerConfig;
  private readonly fileScanner: FileScanner;
  private readonly jobProducer: JobProducer;

  private stopping = false;

  constructor(deps: {
    logger: ILogger;
    config: ScannerConfig;
    fileScanner: FileScanner;
    jobProducer: JobProducer;
    signals: Signals;
  }) {
    this.logger = deps.logger;
    this.config = deps.config;
    this.fileScanner = deps.fileScanner;
    this.jobProducer = deps.jobProducer;

    // Stop the walk before the producer teardown runs. Registered first, so it
    // runs first in the ordered shutdown — flips the flag, then the producer
    // disconnects, by which point run()'s loop has already broken out.
    deps.signals.onTerm(() => {
      this.stopping = true;
      this.logger.info('scan cancellation requested — stopping');
    });
  }

  async run(): Promise<void> {
    const { logger, config } = this;
    logger.info({ topic: config.topic, scanRoot: config.scanRoot }, 'scan started');

    let produced = 0;
    for await (const { path, info } of this.fileScanner.walk()) {
      if (this.stopping) break;
      try {
        await this.jobProducer.produce(path, info);
        produced++;
      } catch (err) {
        // If we're shutting down, a send() that loses the race with the
        // producer disconnect is expected — swallow it and stop. Otherwise it's
        // a real producer error and should surface.
        if (this.stopping) break;
        throw err;
      }
    }

    logger.info({ produced, cancelled: this.stopping }, 'scan complete');
  }
}

export default ScanJob;
