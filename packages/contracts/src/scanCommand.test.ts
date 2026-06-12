import { describe, it, expect } from 'vitest';
import { parseScanCommand, serializeScanCommand, type ScanCommand } from './scanCommand.js';

describe('scanCommand', () => {
  it('round-trips each command type', () => {
    const cmds: ScanCommand[] = [
      { type: 'StartScan', scanId: 'a', scanRoot: '/x' },
      { type: 'StopScan', scanId: 'a' },
      { type: 'StopScan' },
      { type: 'PauseGenerator' },
      { type: 'ResumeGenerator' },
    ];
    for (const c of cmds) {
      const parsed = parseScanCommand(JSON.parse(serializeScanCommand(c).toString('utf-8')));
      expect(parsed).toEqual(c);
    }
  });

  it('rejects unknown / malformed commands', () => {
    expect(parseScanCommand({ type: 'Nope' })).toBeNull();
    expect(parseScanCommand({ type: 'StartScan' })).toBeNull(); // missing scanId/scanRoot
    expect(parseScanCommand({ type: 'StartScan', scanId: '', scanRoot: '/x' })).toBeNull();
    expect(parseScanCommand(null)).toBeNull();
  });
});
