import { DynamoDB, type QueryCommandOutput } from '@aws-sdk/client-dynamodb';
import { describe, expect, test, vi } from 'vitest';
import { Facet } from './facet.js';

interface Item {
	pk: string;
	sk: string;
}

const TABLE_NAME = 'TEST';

describe('PartitionQuery', () => {
	test('a response with no Items yields an empty result', async () => {
		const ddb = new DynamoDB({
			region: 'us-east-1',
			endpoint: 'http://localhost:8000',
		});
		// The SDK types `Items` as optional; an absent array must read as
		// zero records, not a crash.
		vi.spyOn(ddb, 'query').mockImplementation(
			async (): Promise<QueryCommandOutput> => ({
				$metadata: {},
			}),
		);

		const facet = new Facet<Item, 'pk', 'sk'>({
			name: 'Item',
			PK: { keys: ['pk'], prefix: 'PK' },
			SK: { keys: ['sk'], prefix: 'SK' },
			validator: (input) => input as Item,
			connection: {
				dynamoDb: ddb,
				tableName: TABLE_NAME,
			},
		});

		const result = await facet.query({ pk: 'x' }).list();
		expect(result.records).toEqual([]);
		expect(result.cursor).toBeUndefined();
	});
});
