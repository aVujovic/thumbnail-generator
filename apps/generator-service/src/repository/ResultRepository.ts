import { appendFileSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { serializeResultLine, type ThumbnailResult } from '@thumbnailer/contracts';

/**
 * Sink for per-video processing results. Abstracted so the generator doesn't
 * care whether results land in a file, Postgres, or MySQL — in production this
 * is a DB repository; here it's an append-only JSONL file. `save()` models a
 * single row INSERT.
 */
export interface IResultRepository {
  save(result: ThumbnailResult): Promise<void>;
}

/**
 * Append-only JSONL implementation: one JSON object per line. Append is the
 * right primitive for many concurrent consumers — each `save` is an atomic
 * append, so writers never clobber each other (unlike rewriting a JSON array),
 * and a crash can't corrupt earlier rows.
 */
export class JsonlResultRepository implements IResultRepository {
  private readonly path: string;
  private dirEnsured = false;

  constructor(deps: { config: { resultsPath: string } }) {
    this.path = deps.config.resultsPath;
  }

  async save(result: ThumbnailResult): Promise<void> {
    if (!this.dirEnsured) {
      mkdirSync(dirname(this.path), { recursive: true });
      this.dirEnsured = true;
    }
    appendFileSync(this.path, serializeResultLine(result));
  }
}

export default JsonlResultRepository;
