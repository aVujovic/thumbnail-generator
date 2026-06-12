import { z } from 'zod';

/**
 * The outcome of processing one video — what the generator records to its
 * result sink. In production this is a row in a Postgres/MySQL table; here it's
 * one line in a JSONL file. The shape is the same either way, which is why it
 * lives in contracts (the "table schema").
 */
export const ThumbnailResultSchema = z.object({
  /** The scan this result belongs to (if the job carried one). */
  scanId: z.string().min(1).optional(),
  /** The tenant this result belongs to (if the job carried one). */
  tenantId: z.string().min(1).optional(),
  /** Source video path. */
  videoPath: z.string().min(1),
  /** Where the thumbnail was (or would be) written. */
  outputPath: z.string().min(1),
  /** Video size in bytes (from the scan). */
  size: z.number().int().nonnegative(),
  /** Epoch millis the scanner discovered the file. */
  discoveredAt: z.number().int().nonnegative(),
  /** Thumbnail image format (jpg, png, ...). */
  format: z.string().min(1),
  /** Terminal outcome of generation. */
  status: z.enum(['generated', 'skipped', 'failed']),
  /** Attempts made (1 for skip; 1..maxRetries+1 otherwise). */
  attempts: z.number().int().positive(),
  /** Failure reason — present only when status is 'failed'. */
  error: z.string().optional(),
  /** Epoch millis the result was recorded. */
  processedAt: z.number().int().nonnegative(),
});

export type ThumbnailResult = z.infer<typeof ThumbnailResultSchema>;

/** Serialize one result as a single JSONL line (newline included). */
export function serializeResultLine(result: ThumbnailResult): string {
  return `${JSON.stringify(result)}\n`;
}
