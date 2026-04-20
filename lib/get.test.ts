import {
	DynamoDB,
	type BatchGetItemCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { describe, expect, test, vi } from 'vitest';
import { Facet } from './facet.js';
import { wait } from './wait.js';

interface Item {
	pk: string;
	sk: string;
}

const TABLE_NAME = 'TEST';

function buildConcurrencyFacet() {
	let inFlight = 0;
	let peak = 0;
	const ddb = new DynamoDB({
		region: 'us-east-1',
		endpoint: 'http://localhost:8000',
	});
	vi.spyOn(ddb, 'batchGetItem').mockImplementation(
		async (): Promise<BatchGetItemCommandOutput> => {
			inFlight += 1;
			peak = Math.max(peak, inFlight);
			await wait(5);
			inFlight -= 1;
			return {
				Responses: { [TABLE_NAME]: [] },
				UnprocessedKeys: {},
				$metadata: {},
			};
		},
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
	return { facet, getPeak: () => peak };
}

function buildQueries(count: number): Item[] {
	return Array.from({ length: count }, (_, i) => ({
		pk: `pk-${i}`,
		sk: `sk-${i}`,
	}));
}

describe('getBatchItems fan-out concurrency', () => {
	test('caps in-flight requests at the default (8)', async () => {
		const { facet, getPeak } = buildConcurrencyFacet();
		// 100 items per batchGetItem ⇒ 50 batches for 5000 queries.
		await facet.get(buildQueries(50 * 100));
		expect(getPeak()).toBe(8);
	});

	test('honours a caller-supplied concurrency override', async () => {
		const { facet, getPeak } = buildConcurrencyFacet();
		await facet.get(buildQueries(20 * 100), { concurrency: 2 });
		expect(getPeak()).toBe(2);
	});
});
