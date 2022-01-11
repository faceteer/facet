import type { AttributeValue } from '@aws-sdk/client-dynamodb';
import cbor from 'cbor';

export function encodeCursor(lastKey: {
	[key: string]: AttributeValue;
}): string {
	return cbor.encode(lastKey).toString('base64');
}

export function decodeCursor(cursor?: string): {
	[key: string]: AttributeValue;
} {
	if (!cursor) {
		return {};
	}
	return cbor.decode(Buffer.from(cursor, 'base64'));
}
