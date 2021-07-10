import CRC32 from 'crc-32';

/**
 * Assign any arbitrary string to a "random" hex group using CRC32.
 *
 * CRC32 is not a cryptographic hash but it's incredibly fast.
 *
 * This operation is consistent. Given a the same string
 * and the same group size you willa always get the same
 * group returned
 *
 * Both of these make this function a great tool for
 * distributing keys across nodes or partitions
 */
export function crcShard(string: string, shardCount: number) {
	shardCount = shardCount < 1 ? 1 : ~~shardCount;

	/**
	 * Shards are represented as hex numbers. To have the shard
	 * names be consistent in length we need to see how many digits
	 * we need to pad with.
	 *
	 * A shard count of 16 will only result in values from 0x0 to 0xF
	 * so we only need one digit to display 0 to F
	 *
	 * But a shard count of 512 will have values of 0x0 to 0x1FF
	 * so we need 3 digits to represent all possible groups
	 */
	const padLength = (shardCount - 1).toString(16).length;

	return ((CRC32.bstr(string) >>> 1) % shardCount)
		.toString(16)
		.padStart(padLength, '0');
}
