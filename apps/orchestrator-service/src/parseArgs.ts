import { homedir } from 'node:os';
import { resolve } from 'node:path';
import { scanCommands, type ScanCommand } from '@thumbnailer/contracts';

/** Expand `~` and resolve to an absolute path (same rules as the services). */
function resolvePath(p: string): string {
  if (p === '~') return homedir();
  if (p.startsWith('~/')) return resolve(homedir(), p.slice(2));
  return resolve(p);
}

function flag(args: string[], name: string): string | undefined {
  const i = args.indexOf(`--${name}`);
  return i !== -1 ? args[i + 1] : undefined;
}

/**
 * Turn CLI args into a ScanCommand. Pure (no I/O) so it's unit-testable. The
 * `newId` factory makes scanId generation injectable for deterministic tests.
 *
 * Usage:
 *   start  --root <path> [--id <scanId>]   StartScan
 *   stop   [--id <scanId>]                 StopScan
 *   pause                                  PauseGenerator
 *   resume                                 ResumeGenerator
 */
export function parseArgs(argv: string[], newId: () => string): ScanCommand {
  const [sub, ...rest] = argv;

  switch (sub) {
    case 'start': {
      const root = flag(rest, 'root');
      if (!root) throw new Error('start requires --root <path>');
      return scanCommands.startScan(flag(rest, 'id') ?? newId(), resolvePath(root));
    }
    case 'stop':
      return scanCommands.stopScan(flag(rest, 'id'));
    case 'pause':
      return scanCommands.pauseGenerator();
    case 'resume':
      return scanCommands.resumeGenerator();
    default:
      throw new Error(
        `unknown command '${sub ?? ''}'. Use: start --root <path> | stop [--id <id>] | pause | resume`,
      );
  }
}
