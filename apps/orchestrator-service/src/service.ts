import { randomUUID } from 'node:crypto';
import { createService } from '@thumbnailer/core';
import { asClass } from 'awilix';

import container from './container.js';
import { ConfigSchema } from './config.schema.js';
import CommandPublisher from './CommandPublisher.js';
import { parseArgs } from './parseArgs.js';

/**
 * One-shot CLI: parse the sub-command, connect the producer, publish one
 * ScanCommand, and exit. The scanner/generator daemons react to it.
 *
 *   pnpm scan:start --root ~/Movies     -> StartScan
 *   pnpm scan:stop  [--id <scanId>]     -> StopScan
 *   pnpm gen:pause                       -> PauseGenerator
 *   pnpm gen:resume                      -> ResumeGenerator
 */
const main = async (): Promise<void> => {
  const command = parseArgs(process.argv.slice(2), randomUUID);

  createService({ container, configSchema: ConfigSchema });
  container.register({ commandPublisher: asClass(CommandPublisher).singleton() });

  const { commands, commandPublisher } = container.cradle;
  await commands.start(); // connect producer + register teardown
  await commandPublisher.publish(command);
  await commands.stop(0); // flush + graceful exit
};

main().catch((err: unknown) => {
  console.error(`orchestrator: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
