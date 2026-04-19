import { crc32 } from 'node:zlib';

/**
 * Map an arbitrary string to a deterministic shard id, formatted as a
 * zero-padded hex number in the range `[0, shardCount)`.
 *
 * Uses Node's built-in `zlib.crc32`, which is the standard CRC-32
 * (IEEE 802.3 polynomial 0xEDB88320) and benefits from whatever
 * hardware acceleration the runtime provides. CRC32 is not a
 * cryptographic hash — it's used here only because we need a fast,
 * deterministic, uniform mapping of an input to a shard bucket.
 *
 * String inputs are UTF-8 encoded before hashing (the standard
 * `zlib.crc32` behaviour), so two strings with identical bytes produce
 * identical shard ids regardless of the JS string representation.
 */
export function crcShard(input: string, shardCount: number): string {
	// Using `!(x >= 1)` instead of `x < 1` so NaN takes the clamped path
	// (`NaN < 1` is false, which would otherwise let NaN escape).
	shardCount = !(shardCount >= 1) ? 1 : Math.trunc(shardCount);
	const padLength = (shardCount - 1).toString(16).length;
	return (crc32(input) % shardCount).toString(16).padStart(padLength, '0');
}
