import type { AttributeValue } from '@aws-sdk/client-dynamodb';

/**
 * Encodes a DynamoDB `LastEvaluatedKey` as an opaque cursor string.
 *
 * The cursor is a base64url-encoded byte sequence of
 * `(code:u8)(len:varint)(bytes:utf-8)` tuples, one per attribute.
 * This exploits two invariants the library maintains:
 *
 * 1. Every key value is a string. `buildKey` only emits strings, and
 *    DynamoDB returns back the same shape it received.
 * 2. Attribute names are drawn from a fixed 42-name set: `PK`, `SK`,
 *    and `GSI1PK..GSI20SK`. Each name encodes to a single byte.
 *
 * On a typical 4-entry GSI cursor the wire form is ~30% smaller than
 * base64url'd JSON and ~20% smaller than the previous CBOR encoding,
 * with no runtime dependencies.
 */

const nameToCode = new Map<string, number>([
	['PK', 0],
	['SK', 1],
]);
const codeToName: string[] = ['PK', 'SK'];
for (let n = 1; n <= 20; n++) {
	nameToCode.set(`GSI${n}PK`, 2 * n);
	nameToCode.set(`GSI${n}SK`, 2 * n + 1);
	codeToName[2 * n] = `GSI${n}PK`;
	codeToName[2 * n + 1] = `GSI${n}SK`;
}

export function encodeCursor(lastKey: {
	[key: string]: AttributeValue;
}): string {
	const chunks: Buffer[] = [];
	for (const [name, value] of Object.entries(lastKey)) {
		const code = nameToCode.get(name);
		if (code === undefined) {
			throw new Error(
				`encodeCursor: unexpected attribute name ${JSON.stringify(name)}`,
			);
		}
		if (typeof value.S !== 'string') {
			throw new Error(
				`encodeCursor: expected string value for ${name}, got ${JSON.stringify(value)}`,
			);
		}
		const utf8 = Buffer.from(value.S, 'utf-8');
		chunks.push(Buffer.from([code]));
		chunks.push(encodeVarint(utf8.length));
		chunks.push(utf8);
	}
	return Buffer.concat(chunks).toString('base64url');
}

export function decodeCursor(cursor?: string): {
	[key: string]: AttributeValue;
} {
	if (!cursor) {
		return {};
	}
	const buf = Buffer.from(cursor, 'base64url');
	if (buf.length === 0) {
		throw new Error('decodeCursor: cursor decoded to an empty buffer');
	}
	const result: { [key: string]: AttributeValue } = {};
	let i = 0;
	while (i < buf.length) {
		const code = buf[i++];
		const name = codeToName[code];
		if (!name) {
			throw new Error(`decodeCursor: invalid attribute code ${code}`);
		}
		const [length, next] = decodeVarint(buf, i);
		i = next + length;
		if (i > buf.length) {
			throw new Error(`decodeCursor: value length exceeds buffer`);
		}
		result[name] = { S: buf.subarray(next, i).toString('utf-8') };
	}
	return result;
}

/**
 * Unsigned 7-bit varint (protobuf-style, little-endian). One byte for
 * values up to 127, which covers every realistic key size; widens as
 * needed. Uses `Number` arithmetic instead of 32-bit bitwise shifts so
 * values above 2^31 don't wrap silently.
 */
function encodeVarint(value: number): Buffer {
	const bytes: number[] = [];
	while (value >= 0x80) {
		bytes.push((value & 0x7f) | 0x80);
		value = Math.floor(value / 0x80);
	}
	bytes.push(value & 0x7f);
	return Buffer.from(bytes);
}

function decodeVarint(buf: Buffer, start: number): [number, number] {
	let value = 0;
	let multiplier = 1;
	let i = start;
	while (i < buf.length) {
		const byte = buf[i++];
		value += (byte & 0x7f) * multiplier;
		if (value > Number.MAX_SAFE_INTEGER) {
			throw new Error('decodeCursor: varint overflow');
		}
		if ((byte & 0x80) === 0) return [value, i];
		multiplier *= 0x80;
	}
	throw new Error('decodeCursor: truncated varint');
}
