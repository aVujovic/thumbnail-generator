import { describe, it, expect } from 'vitest';
import { thumbnailOutputPath } from './outputPath.js';

describe('thumbnailOutputPath', () => {
  it('mirrors the source tree under the output root and appends .jpg', () => {
    expect(thumbnailOutputPath('/out', '/data/movies/clip.mp4')).toBe(
      '/out/data/movies/clip.mp4.jpg',
    );
  });

  it('keeps the extension so different videos with the same stem do not collide', () => {
    expect(thumbnailOutputPath('/out', '/a/clip.mp4')).toBe('/out/a/clip.mp4.jpg');
    expect(thumbnailOutputPath('/out', '/a/clip.mov')).toBe('/out/a/clip.mov.jpg');
  });

  it('handles a relative output root', () => {
    expect(thumbnailOutputPath('./output', '/data/x.mp4')).toBe('output/data/x.mp4.jpg');
  });

  it('strips path-traversal segments so output stays inside the root', () => {
    expect(thumbnailOutputPath('/out', '/../../etc/passwd.mp4')).toBe('/out/etc/passwd.mp4.jpg');
    expect(thumbnailOutputPath('/out', 'a/../../b/v.mp4')).toBe('/out/b/v.mp4.jpg');
  });
});
