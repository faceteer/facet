import { describe, expect, test } from 'vitest';
import { decodeCursor, encodeCursor } from './cursor';

describe('cursor', () => {
	test('round-trips a base-table cursor', () => {
		const key = {
			PK: { S: '#ORG_alpha' },
			SK: { S: '#PAGE_42' },
		};
		const cursor = encodeCursor(key);
		expect(decodeCursor(cursor)).toEqual(key);
	});

	test('round-trips a GSI cursor with four entries', () => {
		const key = {
			PK: { S: '#ORG_alpha' },
			SK: { S: '#POST_01' },
			GSI3PK: { S: '#STATUS_queued' },
			GSI3SK: { S: '#STATUS_2024-05-01T00:00:00.000Z' },
		};
		const cursor = encodeCursor(key);
		expect(decodeCursor(cursor)).toEqual(key);
	});

	test('round-trips long key values past the single-byte varint boundary', () => {
		const key = {
			PK: { S: 'x'.repeat(500) },
			SK: { S: 'y'.repeat(2000) },
		};
		expect(decodeCursor(encodeCursor(key))).toEqual(key);
	});

	test('round-trips unicode key values', () => {
		const key = {
			PK: { S: 'café☕' },
			SK: { S: '🚀\u0000\uFEFFedge' },
		};
		expect(decodeCursor(encodeCursor(key))).toEqual(key);
	});

	test('round-trips empty string values', () => {
		const key = {
			PK: { S: '' },
			SK: { S: '' },
		};
		expect(decodeCursor(encodeCursor(key))).toEqual(key);
	});

	test('encodes using the url-safe base64 alphabet', () => {
		const key = {
			PK: { S: '??>>>>>>>>' },
			SK: { S: '////++++++' },
		};
		const cursor = encodeCursor(key);
		expect(cursor).not.toMatch(/[+/=]/);
	});

	test('empty or undefined cursor decodes to an empty object', () => {
		expect(decodeCursor()).toEqual({});
		expect(decodeCursor('')).toEqual({});
	});

	test('supports every GSI slot', () => {
		for (let n = 1; n <= 20; n++) {
			const key = {
				PK: { S: 'p' },
				SK: { S: 's' },
				[`GSI${n}PK`]: { S: `gp${n}` },
				[`GSI${n}SK`]: { S: `gs${n}` },
			};
			expect(decodeCursor(encodeCursor(key))).toEqual(key);
		}
	});

	test('rejects an unknown attribute name on encode', () => {
		expect(() => encodeCursor({ HELLO: { S: 'x' } })).toThrow(
			/unexpected attribute name/,
		);
	});

	test('rejects a non-string AttributeValue on encode', () => {
		expect(() => encodeCursor({ PK: { N: '42' } })).toThrow(
			/expected string value/,
		);
	});

	test('rejects a cursor with an invalid attribute code', () => {
		// Code 200 isn't in the table (max valid is 41 = GSI20SK).
		const poisoned = Buffer.from([200, 1, 0x61]).toString('base64url');
		expect(() => decodeCursor(poisoned)).toThrow(/invalid attribute code/);
	});

	test('rejects a cursor whose declared length runs past the buffer', () => {
		// Code 0 (PK), length 100, but only 2 bytes follow.
		const poisoned = Buffer.from([0, 100, 0x61, 0x62]).toString('base64url');
		expect(() => decodeCursor(poisoned)).toThrow(/exceeds buffer/);
	});

	test('rejects a cursor with a truncated varint', () => {
		// Code 0, then a single continuation byte with the high bit set
		// and nothing after it.
		const poisoned = Buffer.from([0, 0x80]).toString('base64url');
		expect(() => decodeCursor(poisoned)).toThrow(/truncated varint/);
	});

	test('rejects a cursor with a varint that overflows MAX_SAFE_INTEGER', () => {
		// Code 0, then nine 0xff continuation bytes followed by 0x7f.
		// That would decode to well beyond 2^53.
		const poisoned = Buffer.from([
			0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
		]).toString('base64url');
		expect(() => decodeCursor(poisoned)).toThrow(/varint overflow/);
	});

	test('rejects a cursor whose input contains no base64url characters', () => {
		// "!!!" has no valid base64url alphabet characters, so Node
		// decodes it to an empty buffer. Without the guard this would
		// silently return {}.
		expect(() => decodeCursor('!!!')).toThrow(/empty buffer/);
	});

	test('round-trips a value at DynamoDB SK size limit', () => {
		const key = {
			PK: { S: 'p' },
			SK: { S: 'x'.repeat(2048) },
		};
		expect(decodeCursor(encodeCursor(key))).toEqual(key);
	});
});
