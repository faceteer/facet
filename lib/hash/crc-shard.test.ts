import { describe, expect, test } from 'vitest';
import { crcShard } from './crc-shard.js';

describe('crcShard', () => {
	test('produces the same shard for the same input', () => {
		const a = crcShard('some-post-id', 16);
		const b = crcShard('some-post-id', 16);
		expect(a).toBe(b);
	});

	test('returns a shard id within [0, shardCount)', () => {
		const shardCount = 16;
		for (let i = 0; i < 100; i++) {
			const shard = parseInt(crcShard(`key-${i}`, shardCount), 16);
			expect(shard).toBeGreaterThanOrEqual(0);
			expect(shard).toBeLessThan(shardCount);
		}
	});

	test('pads shard ids to a consistent hex width', () => {
		// count=16 → padLength 1 (max value f).
		expect(crcShard('x', 16)).toMatch(/^[0-9a-f]$/);
		// count=256 → padLength 2 (max value ff).
		expect(crcShard('x', 256)).toMatch(/^[0-9a-f]{2}$/);
		// count=512 → padLength 3 (max value 1ff).
		expect(crcShard('x', 512)).toMatch(/^[0-9a-f]{3}$/);
	});

	test('distributes inputs approximately uniformly', () => {
		const shardCount = 8;
		const buckets = new Array<number>(shardCount).fill(0);
		const samples = 10_000;
		for (let i = 0; i < samples; i++) {
			buckets[parseInt(crcShard(`sample-${i}`, shardCount), 16)]++;
		}
		const expected = samples / shardCount;
		for (const count of buckets) {
			// Each bucket should be within 10% of the expected mean.
			expect(count).toBeGreaterThan(expected * 0.9);
			expect(count).toBeLessThan(expected * 1.1);
		}
	});

	test('clamps invalid shardCount to 1', () => {
		expect(crcShard('x', 0)).toBe('0');
		expect(crcShard('x', -5)).toBe('0');
		expect(crcShard('x', NaN)).toBe('0');
	});

	test('truncates fractional shardCount', () => {
		// 3.7 → 3. All outputs must be in [0, 3).
		for (let i = 0; i < 50; i++) {
			const v = parseInt(crcShard(`k-${i}`, 3.7), 16);
			expect(v).toBeGreaterThanOrEqual(0);
			expect(v).toBeLessThan(3);
		}
	});

	test('UTF-8 encodes the input, matching node:zlib.crc32 semantics', () => {
		// The canonical CRC-32 test vector: "123456789" → 0xcbf43926.
		// With shardCount = 0x10000, the shard id is the low 16 bits:
		// 0xcbf43926 & 0xffff = 0x3926.
		expect(crcShard('123456789', 0x10000)).toBe('3926');
	});
});
