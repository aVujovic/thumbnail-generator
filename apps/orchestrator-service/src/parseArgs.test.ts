import { describe, it, expect } from 'vitest';
import { homedir } from 'node:os';
import { parseArgs } from './parseArgs.js';

const id = () => 'generated-id';

describe('parseArgs', () => {
  it('builds StartScan with an explicit id and resolved root', () => {
    expect(parseArgs(['start', '--root', '/data/movies', '--id', 'abc'], id)).toEqual({
      type: 'StartScan',
      scanId: 'abc',
      scanRoot: '/data/movies',
    });
  });

  it('generates a scanId when none is given', () => {
    const cmd = parseArgs(['start', '--root', '/x'], id);
    expect(cmd).toMatchObject({ type: 'StartScan', scanId: 'generated-id' });
  });

  it('expands ~ in the root', () => {
    const cmd = parseArgs(['start', '--root', '~/Movies'], id);
    expect(cmd).toMatchObject({ scanRoot: `${homedir()}/Movies` });
  });

  it('requires --root for start', () => {
    expect(() => parseArgs(['start'], id)).toThrow(/--root/);
  });

  it('builds StopScan with and without an id', () => {
    expect(parseArgs(['stop', '--id', 'abc'], id)).toEqual({ type: 'StopScan', scanId: 'abc' });
    expect(parseArgs(['stop'], id)).toEqual({ type: 'StopScan' });
  });

  it('builds Pause/Resume', () => {
    expect(parseArgs(['pause'], id)).toEqual({ type: 'PauseGenerator' });
    expect(parseArgs(['resume'], id)).toEqual({ type: 'ResumeGenerator' });
  });

  it('rejects unknown commands', () => {
    expect(() => parseArgs(['frobnicate'], id)).toThrow(/unknown command/);
  });
});
