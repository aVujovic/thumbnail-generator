import { createService } from '@thumbnailer/core';
import { asClass } from 'awilix';

import container from './container.js';
import { ConfigSchema } from './config.schema.js';
import LocalFileSystem from './fs/LocalFileSystem.js';
import VideoDetector from './scanner/videoDetector.js';
import FileScanner from './scanner/FileScanner.js';
import JobProducer from './producer/JobProducer.js';
import ScanJob from './jobs/ScanJob.job.js';

/**
 * Entry point / composition root. No business logic lives here — it wires the
 * infrastructure (core) and the service's own classes into the container, then
 * hands control to them. Mirrors the @gamerstake/payment service.ts shape.
 */
const main = async (): Promise<void> => {
  createService({ container, configSchema: ConfigSchema });

  container.register({
    // The production IFileSystem is assumed to exist; LocalFileSystem is the
    // local stand-in so the demo runs against real files.
    fs: asClass(LocalFileSystem).singleton(),
    videoDetector: asClass(VideoDetector).singleton(),
    fileScanner: asClass(FileScanner).singleton(),
    jobProducer: asClass(JobProducer).singleton(),
    scanJob: asClass(ScanJob).singleton(),
  });

  // Resolving the cradle constructs scanJob, which registers its cancellation
  // teardown. This happens before commands.start() registers the producer
  // disconnect — so on shutdown the ordered registry flips scanJob's stop flag
  // first, halting the walk before the producer is torn down.
  const { commands, scanJob } = container.cradle;
  await commands.start(); // connect producer + register Kafka teardown
  await scanJob.run(); // walk → detect → produce
  await commands.stop(0); // one-shot: flush + graceful shutdown via signals
};

void main();
