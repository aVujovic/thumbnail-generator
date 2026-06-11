import { z } from 'zod';
import { booleanish, resolvePath } from '@thumbnailer/core';

/**
 * Scanner config, validated against process.env at startup by core's config
 * provider. The `kafka` block matches the shape core's kafka provider expects.
 */
export const ConfigSchema = z
  .object({
    KAFKA_BROKERS: z.string().min(1),
    TOPIC_VIDEO_JOBS: z.string().min(1).default('video-jobs'),
    SCAN_ROOT: z.string().min(1).default('/data'),
    SCAN_MAX_DEPTH: z.coerce.number().int().positive().default(64),
    // Emit an info-level progress line every N entries scanned (0 = off).
    SCAN_PROGRESS_EVERY: z.coerce.number().int().nonnegative().default(1000),
    VIDEO_EXTENSIONS: z
      .string()
      .default('mp4,mov,mkv,avi,webm,m4v,mpg,mpeg,wmv,flv'),
    KAFKA_CLIENT_ID: z.string().min(1).default('scanner-service'),
    LOG_LEVEL: z.string().default('info'),
    LOG_PRETTY: booleanish.optional(),
    NODE_ENV: z.string().default('development'),
  })
  .transform((env) => ({
    name: 'scanner-service',
    env: env.NODE_ENV,
    logLevel: env.LOG_LEVEL,
    logPretty: env.LOG_PRETTY,
    scanRoot: resolvePath(env.SCAN_ROOT),
    maxDepth: env.SCAN_MAX_DEPTH,
    progressEvery: env.SCAN_PROGRESS_EVERY,
    videoExtensions: env.VIDEO_EXTENSIONS.split(',')
      .map((e) => e.trim().toLowerCase().replace(/^\./, ''))
      .filter(Boolean),
    topic: env.TOPIC_VIDEO_JOBS,
    kafka: {
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
      clientId: env.KAFKA_CLIENT_ID,
      mode: 'producer' as const,
    },
  }));

export type ScannerConfig = z.infer<typeof ConfigSchema>;
