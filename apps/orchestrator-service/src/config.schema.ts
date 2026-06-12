import { z } from 'zod';

/** Orchestrator config — it only needs to produce onto the commands topic. */
export const ConfigSchema = z
  .object({
    KAFKA_BROKERS: z.string().min(1),
    TOPIC_SCAN_COMMANDS: z.string().min(1).default('scan-commands'),
    LOG_LEVEL: z.string().default('info'),
    NODE_ENV: z.string().default('development'),
  })
  .transform((env) => ({
    name: 'orchestrator',
    env: env.NODE_ENV,
    logLevel: env.LOG_LEVEL,
    commandsTopic: env.TOPIC_SCAN_COMMANDS,
    kafka: {
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
      clientId: 'orchestrator',
      mode: 'producer' as const,
    },
  }));

export type OrchestratorConfig = z.infer<typeof ConfigSchema>;
