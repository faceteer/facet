import type { DynamoDB } from 'aws-sdk';
import cbor from 'cbor';

export function encodeCursor(lastKey: DynamoDB.Key): string {
	return cbor.encode(lastKey).toString('base64');
}

export function decodeCursor(cursor?: string): DynamoDB.Key {
	if (!cursor) {
		return {};
	}
	return cbor.decode(Buffer.from(cursor, 'base64'));
}
