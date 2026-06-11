import { readdirSync, statSync } from 'node:fs';
import { join, basename } from 'node:path';
import type { FileInfo, IFileSystem } from '@thumbnailer/domain';

/**
 * A POSIX-backed IFileSystem over the local disk.
 *
 * The task states a production IFileSystem implementation is assumed (local or
 * remote) — this is the local stand-in so the demo runs against real files.
 * In production this is swapped for the real adapter; nothing else changes,
 * because everything depends on the IFileSystem interface, not this class.
 */
export class LocalFileSystem implements IFileSystem {
  listFiles(path: string): string[] {
    return readdirSync(path).map((name) => join(path, name));
  }

  openFile(path: string): FileInfo {
    const st = statSync(path);
    return {
      name: basename(path),
      isDirectory: st.isDirectory(),
      size: st.size,
      permissions: st.mode & 0o777,
    };
  }
}

export default LocalFileSystem;
