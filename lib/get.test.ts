import {
	DynamoDB,
	type AttributeValue,
	type BatchGetItemCommandInput,
	type BatchGetItemCommandOutput,
	type GetItemCommandInput,
	type GetItemCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { describe, expect, test, vi } from 'vitest';
import { Facet } from './facet.js';
import { wait } from './wait.js';

interface Item {
	pk: string;
	sk: string;
}

const TABLE_NAME = 'TEST';

function buildItemFacet(ddb: DynamoDB) {
	return new Facet<Item, 'pk', 'sk'>({
		name: 'Item',
		PK: { keys: ['pk'], prefix: 'PK' },
		SK: { keys: ['sk'], prefix: 'SK' },
		validator: (input) => input as Item,
		connection: {
			dynamoDb: ddb,
			tableName: TABLE_NAME,
		},
	});
}

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

	return { facet: buildItemFacet(ddb), getPeak: () => peak };
}

function buildRetryFacet(queueResponses: BatchGetItemCommandOutput[]) {
	const calls: BatchGetItemCommandInput[] = [];
	const ddb = new DynamoDB({
		region: 'us-east-1',
		endpoint: 'http://localhost:8000',
	});
	vi.spyOn(ddb, 'batchGetItem').mockImplementation(
		async (input: BatchGetItemCommandInput) => {
			calls.push(input);
			const next = queueResponses.shift();
			if (!next) {
				throw new Error('batchGetItem called more times than expected');
			}
			return next;
		},
	);
	return { facet: buildItemFacet(ddb), calls };
}

function storedItem(id: string): Record<string, AttributeValue> {
	return {
		PK: { S: `PK_${id}` },
		SK: { S: `SK_${id}` },
		pk: { S: id },
		sk: { S: id },
		facet: { S: 'Item' },
	};
}

function keyFor(id: string): Record<string, AttributeValue> {
	return { PK: { S: `PK_${id}` }, SK: { S: `SK_${id}` } };
}

function batchResponse(
	items: Record<string, AttributeValue>[] | null,
	unprocessed?: Record<string, AttributeValue>[],
): BatchGetItemCommandOutput {
	return {
		Responses: items ? { [TABLE_NAME]: items } : {},
		UnprocessedKeys: unprocessed ? { [TABLE_NAME]: { Keys: unprocessed } } : {},
		$metadata: {},
	};
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

describe('getBatchItems unprocessed-key retries', () => {
	test('unprocessed keys are retried and their records returned', async () => {
		const { facet, calls } = buildRetryFacet([
			batchResponse([storedItem('a')], [keyFor('b')]),
			batchResponse([storedItem('b')]),
		]);

		const records = await facet.get([
			{ pk: 'a', sk: 'a' },
			{ pk: 'b', sk: 'b' },
		]);

		expect(records).toEqual([
			{ pk: 'a', sk: 'a' },
			{ pk: 'b', sk: 'b' },
		]);
		expect(calls).toHaveLength(2);
		// Only the unprocessed key is re-requested.
		expect(calls[1].RequestItems?.[TABLE_NAME].Keys).toEqual([keyFor('b')]);
	});

	test('keys that stay unprocessed are retried until they resolve', async () => {
		const { facet, calls } = buildRetryFacet([
			batchResponse([storedItem('a')], [keyFor('b')]),
			// A retry can itself return nothing and re-report the key.
			batchResponse(null, [keyFor('b')]),
			batchResponse([storedItem('b')]),
		]);

		const records = await facet.get([
			{ pk: 'a', sk: 'a' },
			{ pk: 'b', sk: 'b' },
		]);

		expect(records).toEqual([
			{ pk: 'a', sk: 'a' },
			{ pk: 'b', sk: 'b' },
		]);
		expect(calls).toHaveLength(3);
	});

	test('gives up after 10 retry attempts and returns what it fetched', async () => {
		vi.useFakeTimers();
		try {
			const { facet, calls } = buildRetryFacet([
				batchResponse([storedItem('a')], [keyFor('b')]),
				...Array.from({ length: 10 }, () => batchResponse(null, [keyFor('b')])),
			]);

			const pending = facet.get([
				{ pk: 'a', sk: 'a' },
				{ pk: 'b', sk: 'b' },
			]);
			await vi.runAllTimersAsync();
			const records = await pending;

			// Best-effort: the resolved record comes back; the stuck key is
			// dropped rather than retried forever.
			expect(records).toEqual([{ pk: 'a', sk: 'a' }]);
			expect(calls).toHaveLength(11);
		} finally {
			vi.useRealTimers();
		}
	});

	test('an unprocessed entry without a Keys array is treated as empty', async () => {
		// `UnprocessedKeys` can name the table while `Keys` is absent; that
		// must read as "nothing left to retry", both on the initial response
		// and on a retry response.
		const initial = buildRetryFacet([
			{
				Responses: { [TABLE_NAME]: [storedItem('a')] },
				UnprocessedKeys: { [TABLE_NAME]: { Keys: undefined } },
				$metadata: {},
			},
		]);
		expect(await initial.facet.get([{ pk: 'a', sk: 'a' }])).toEqual([
			{ pk: 'a', sk: 'a' },
		]);
		expect(initial.calls).toHaveLength(1);

		const onRetry = buildRetryFacet([
			batchResponse([storedItem('a')], [keyFor('b')]),
			{
				Responses: { [TABLE_NAME]: [storedItem('b')] },
				UnprocessedKeys: { [TABLE_NAME]: { Keys: undefined } },
				$metadata: {},
			},
		]);
		expect(
			await onRetry.facet.get([
				{ pk: 'a', sk: 'a' },
				{ pk: 'b', sk: 'b' },
			]),
		).toEqual([
			{ pk: 'a', sk: 'a' },
			{ pk: 'b', sk: 'b' },
		]);
		expect(onRetry.calls).toHaveLength(2);
	});

	test('retried requests keep the consistentRead flag', async () => {
		const { facet, calls } = buildRetryFacet([
			batchResponse([storedItem('a')], [keyFor('b')]),
			batchResponse([storedItem('b')]),
		]);

		await facet.get(
			[
				{ pk: 'a', sk: 'a' },
				{ pk: 'b', sk: 'b' },
			],
			{ consistentRead: true },
		);

		expect(calls).toHaveLength(2);
		expect(calls[0].RequestItems?.[TABLE_NAME].ConsistentRead).toBe(true);
		expect(calls[1].RequestItems?.[TABLE_NAME].ConsistentRead).toBe(true);
	});

	test('an error from the batch get call is propagated', async () => {
		const ddb = new DynamoDB({
			region: 'us-east-1',
			endpoint: 'http://localhost:8000',
		});
		vi.spyOn(ddb, 'batchGetItem').mockImplementation(async () => {
			throw new Error('throughput exceeded');
		});
		const facet = buildItemFacet(ddb);

		await expect(facet.get([{ pk: 'a', sk: 'a' }])).rejects.toThrow(
			'throughput exceeded',
		);
	});
});

describe('consistent reads', () => {
	function buildGetItemFacet() {
		const calls: GetItemCommandInput[] = [];
		const ddb = new DynamoDB({
			region: 'us-east-1',
			endpoint: 'http://localhost:8000',
		});
		vi.spyOn(ddb, 'getItem').mockImplementation(
			async (input: GetItemCommandInput): Promise<GetItemCommandOutput> => {
				calls.push(input);
				return { Item: storedItem('a'), $metadata: {} };
			},
		);
		return { facet: buildItemFacet(ddb), calls };
	}

	test('a single get passes ConsistentRead when the option is set', async () => {
		const { facet, calls } = buildGetItemFacet();
		await facet.get({ pk: 'a', sk: 'a' }, { consistentRead: true });
		expect(calls[0].ConsistentRead).toBe(true);
	});

	test('a single get defaults to an eventually consistent read', async () => {
		const { facet, calls } = buildGetItemFacet();
		await facet.get({ pk: 'a', sk: 'a' });
		expect(calls[0].ConsistentRead).toBeUndefined();
	});

	test('a batch get passes ConsistentRead in the table request', async () => {
		const { facet, calls } = buildRetryFacet([
			batchResponse([storedItem('a')]),
		]);
		await facet.get([{ pk: 'a', sk: 'a' }], { consistentRead: true });
		expect(calls[0].RequestItems?.[TABLE_NAME].ConsistentRead).toBe(true);
	});

	test('a batch get defaults to an eventually consistent read', async () => {
		const { facet, calls } = buildRetryFacet([
			batchResponse([storedItem('a')]),
		]);
		await facet.get([{ pk: 'a', sk: 'a' }]);
		expect(calls[0].RequestItems?.[TABLE_NAME].ConsistentRead).toBeUndefined();
	});
});
