import type { FileInfo, IFileSystem, ILogger } from '@thumbnailer/domain';
import type { VideoDetector } from './videoDetector.js';

/** A discovered video file: its path plus the FileInfo we already stat'd. */
export interface DiscoveredVideo {
  path: string;
  info: FileInfo;
}

interface ScannerConfig {
  scanRoot: string;
  maxDepth: number;
  /** Emit a progress log every N entries scanned (0 disables). */
  progressEvery: number;
}

/**
 * Walks an IFileSystem tree and yields every video file it finds.
 *
 * Design choices (see PLAN):
 *  - **Iterative** (explicit stack), never recursive — a deep tree can't blow
 *    the call stack.
 *  - **Async generator** — yields each video as soon as it's found, so the
 *    producer streams jobs and memory stays flat even on a huge tree.
 *  - **max-depth guard** — cheap protection against symlink cycles (we have no
 *    inode info to do full cycle detection).
 *  - **Resilient** — a failing listFiles/openFile (permission denied, or a file
 *    that vanished between list and stat — TOCTOU) is logged and skipped; one
 *    bad entry never aborts the whole scan.
 */
export class FileScanner {
  private readonly fs: IFileSystem;
  private readonly logger: ILogger;
  private readonly detector: VideoDetector;
  private readonly config: ScannerConfig;

  constructor(deps: {
    fs: IFileSystem;
    logger: ILogger;
    videoDetector: VideoDetector;
    config: ScannerConfig;
  }) {
    this.fs = deps.fs;
    this.logger = deps.logger;
    this.detector = deps.videoDetector;
    this.config = deps.config;
  }

  /** Yield every video under `scanRoot`, depth-first, lazily. */
  async *walk(): AsyncGenerator<DiscoveredVideo> {
    const stack: Array<{ path: string; depth: number }> = [
      { path: this.config.scanRoot, depth: 0 },
    ];

    // Heartbeat so a long scan over a large tree shows it's alive — most
    // entries (non-videos, broken links) are otherwise silent at info level.
    const progressEvery = this.config.progressEvery;
    let scanned = 0;
    let videos = 0;

    while (stack.length > 0) {
      const { path, depth } = stack.pop()!;

      if (depth > this.config.maxDepth) {
        this.logger.warn({ path, depth }, 'max depth exceeded — skipping subtree');
        continue;
      }

      let childPaths: string[];
      try {
        childPaths = this.fs.listFiles(path);
      } catch (err) {
        // permission denied on a directory, or `path` isn't listable.
        this.logger.warn({ path, err: errMsg(err) }, 'listFiles failed — skipping');
        continue;
      }

      for (const childPath of childPaths) {
        let info: FileInfo;
        try {
          info = this.fs.openFile(childPath);
        } catch (err) {
          // ENOENT here is expected and benign: a broken symlink (listed by the
          // parent but its target is gone) or a file removed between list and
          // stat (TOCTOU). Log it at debug so it doesn't drown the output on a
          // real filesystem; anything else is a genuine anomaly → warn.
          const msg = errMsg(err);
          const expected = msg.includes('ENOENT');
          const log = expected ? this.logger.debug : this.logger.warn;
          log.call(this.logger, { path: childPath, err: msg }, 'openFile failed — skipping');
          continue;
        }

        scanned++;
        if (progressEvery > 0 && scanned % progressEvery === 0) {
          this.logger.info({ scanned, videos, queued: stack.length }, 'scanning…');
        }

        if (info.isDirectory) {
          stack.push({ path: childPath, depth: depth + 1 });
          continue;
        }

        if (this.detector.isVideo(info)) {
          videos++;
          yield { path: childPath, info };
        }
      }
    }
  }
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export default FileScanner;
