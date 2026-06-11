import { posix } from 'node:path';

/**
 * Maps a source video path to its thumbnail output path, mirroring the source
 * tree under `outputRoot`. Keeps the original file name (extension included) and
 * appends `.{format}`, so two different videos can never collide:
 *
 *   outputRoot=/out, video=/data/movies/clip.mp4, format=jpg
 *     ->  /out/data/movies/clip.mp4.jpg
 *
 * The video path is normalized and stripped of any leading `/` or `..` segments
 * so a malicious/odd path can't escape `outputRoot` (path traversal guard).
 */
export function thumbnailOutputPath(
  outputRoot: string,
  videoPath: string,
  format = 'jpg',
): string {
  const normalized = posix.normalize(videoPath);
  // Drop leading slashes and any `..`/`.` segments — everything stays inside root.
  const safeRelative = normalized
    .split('/')
    .filter((seg) => seg && seg !== '..' && seg !== '.')
    .join('/');
  const ext = format.toLowerCase().replace(/^\./, '');
  return posix.join(outputRoot, `${safeRelative}.${ext}`);
}
