import {
	DynamoDB,
	type QueryCommandInput,
	type QueryCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { describe, expect, test, vi } from 'vitest';
import { Facet } from './facet.js';
import { Index } from './keys.js';

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

describe('consistent reads', () => {
	function buildQueryFacet() {
		const calls: QueryCommandInput[] = [];
		const ddb = new DynamoDB({
			region: 'us-east-1',
			endpoint: 'http://localhost:8000',
		});
		vi.spyOn(ddb, 'query').mockImplementation(
			async (input: QueryCommandInput): Promise<QueryCommandOutput> => {
				calls.push(input);
				return { Items: [], $metadata: {} };
			},
		);

		const facet = new Facet<Item, 'pk', 'sk'>({
			name: 'Item',
			PK: { keys: ['pk'], prefix: 'PK' },
			SK: { keys: ['sk'], prefix: 'SK' },
			validator: (input) => input as Item,
			pickValidator: (keys) => (input) => {
				const record = input as Record<string, unknown>;
				const picked: Record<string, unknown> = {};
				for (const key of keys) {
					picked[key as string] = record[key as string];
				}
				return picked as Pick<Item, (typeof keys)[number]>;
			},
			connection: {
				dynamoDb: ddb,
				tableName: TABLE_NAME,
			},
		}).addIndex({
			index: Index.GSI1,
			PK: { keys: ['sk'], prefix: 'GPK' },
			SK: { keys: ['pk'], prefix: 'GSK' },
		});

		return { facet, calls };
	}

	test('a base-table query passes ConsistentRead when the option is set', async () => {
		const { facet, calls } = buildQueryFacet();
		await facet.query({ pk: 'x' }).list({ consistentRead: true });
		expect(calls[0].ConsistentRead).toBe(true);
	});

	test('a base-table query defaults to an eventually consistent read', async () => {
		const { facet, calls } = buildQueryFacet();
		await facet.query({ pk: 'x' }).list();
		expect(calls[0].ConsistentRead).toBeUndefined();
	});

	test('a projected query combines select with ConsistentRead', async () => {
		const { facet, calls } = buildQueryFacet();
		await facet
			.query({ pk: 'x' })
			.list({ select: ['pk'], consistentRead: true });
		expect(calls[0].ConsistentRead).toBe(true);
		expect(calls[0].ProjectionExpression).toBeDefined();
	});

	test('an index query with consistentRead forced past the types throws', async () => {
		const { facet, calls } = buildQueryFacet();
		await expect(
			facet.GSI1.query({ sk: 'x' }).list({ consistentRead: true as never }),
		).rejects.toThrow(
			'Consistent reads are not supported on global secondary indexes',
		);
		expect(calls).toHaveLength(0);
	});
});
