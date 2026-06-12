import {
	DynamoDB,
	type BatchWriteItemCommandInput,
	type BatchWriteItemCommandOutput,
	type WriteRequest,
} from '@aws-sdk/client-dynamodb';
import { vi } from 'vitest';
import { Facet } from './facet.js';
import { wait } from './wait.js';

interface Item {
	pk: string;
	sk: string;
	value?: string;
}

const TABLE_NAME = 'TEST';

function emptyOutput(): BatchWriteItemCommandOutput {
	return {
		UnprocessedItems: {},
		$metadata: {},
	};
}

function unprocessed(requests: WriteRequest[]): BatchWriteItemCommandOutput {
	return {
		UnprocessedItems: { [TABLE_NAME]: requests },
		$metadata: {},
	};
}

function buildFacet(queueResponses: BatchWriteItemCommandOutput[]) {
	const calls: BatchWriteItemCommandInput[] = [];
	const ddb = new DynamoDB({
		region: 'us-east-1',
		endpoint: 'http://localhost:8000',
	});
	vi.spyOn(ddb, 'batchWriteItem').mockImplementation(
		async (input: BatchWriteItemCommandInput) => {
			calls.push(input);
			const next = queueResponses.shift();
			if (!next) {
				throw new Error('batchWriteItem called more times than expected');
			}
			return next;
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
	return { facet, calls };
}

/**
 * Mirrors `buildFacet` but makes every `batchWriteItem` call resolve
 * after a short delay so concurrent callers can overlap, letting the
 * peak-in-flight counter see values > 1.
 */
function buildConcurrencyFacet() {
	let inFlight = 0;
	let peak = 0;
	const ddb = new DynamoDB({
		region: 'us-east-1',
		endpoint: 'http://localhost:8000',
	});
	vi.spyOn(ddb, 'batchWriteItem').mockImplementation(async () => {
		inFlight += 1;
		peak = Math.max(peak, inFlight);
		await wait(5);
		inFlight -= 1;
		return emptyOutput();
	});

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

function buildRecords(count: number): Item[] {
	return Array.from({ length: count }, (_, i) => ({
		pk: `pk-${i}`,
		sk: `sk-${i}`,
	}));
}

describe('batchWriteWithRetry', () => {
	test('put: reports items that succeed on retry as successful', async () => {
		const { facet, calls } = buildFacet([
			// Initial call: one of the three items comes back unprocessed.
			unprocessed([
				{
					PutRequest: {
						Item: {
							PK: { S: 'PK_a' },
							SK: { S: 'SK_a' },
							pk: { S: 'a' },
							sk: { S: 'a' },
							facet: { S: 'Item' },
						},
					},
				},
			]),
			// Retry clears the unprocessed queue.
			emptyOutput(),
		]);

		const result = await facet.put([
			{ pk: 'a', sk: 'a' },
			{ pk: 'b', sk: 'b' },
			{ pk: 'c', sk: 'c' },
		]);

		expect(result.hasFailures).toBe(false);
		expect(result.failed).toEqual([]);
		expect(result.put).toHaveLength(3);
		expect(calls).toHaveLength(2);
	});

	test('put: reports items still unprocessed after retries as failed', async () => {
		const failing: WriteRequest = {
			PutRequest: {
				Item: {
					PK: { S: 'PK_a' },
					SK: { S: 'SK_a' },
					pk: { S: 'a' },
					sk: { S: 'a' },
					facet: { S: 'Item' },
				},
			},
		};
		const { facet, calls } = buildFacet([
			unprocessed([failing]),
			unprocessed([failing]),
			unprocessed([failing]),
			unprocessed([failing]),
			unprocessed([failing]),
			unprocessed([failing]),
		]);

		const result = await facet.put([
			{ pk: 'a', sk: 'a' },
			{ pk: 'b', sk: 'b' },
			{ pk: 'c', sk: 'c' },
		]);

		expect(result.hasFailures).toBe(true);
		expect(result.failed).toHaveLength(1);
		expect(result.failed[0].record).toEqual({ pk: 'a', sk: 'a' });
		expect(result.put).toHaveLength(2);
		expect(calls).toHaveLength(6);
	});

	test('delete: reports items that succeed on retry as deleted', async () => {
		const { facet, calls } = buildFacet([
			unprocessed([
				{
					DeleteRequest: {
						Key: {
							PK: { S: 'PK_a' },
							SK: { S: 'SK_a' },
						},
					},
				},
			]),
			emptyOutput(),
		]);

		const result = await facet.delete([
			{ pk: 'a', sk: 'a' },
			{ pk: 'b', sk: 'b' },
			{ pk: 'c', sk: 'c' },
		]);

		expect(result.hasFailures).toBe(false);
		expect(result.failed).toEqual([]);
		expect(result.deleted).toHaveLength(3);
		expect(calls).toHaveLength(2);
	});

	test('delete: reports items still unprocessed after retries as failed', async () => {
		const failing: WriteRequest = {
			DeleteRequest: {
				Key: {
					PK: { S: 'PK_a' },
					SK: { S: 'SK_a' },
				},
			},
		};
		const { facet, calls } = buildFacet([
			unprocessed([failing]),
			unprocessed([failing]),
			unprocessed([failing]),
			unprocessed([failing]),
			unprocessed([failing]),
			unprocessed([failing]),
		]);

		const result = await facet.delete([
			{ pk: 'a', sk: 'a' },
			{ pk: 'b', sk: 'b' },
			{ pk: 'c', sk: 'c' },
		]);

		expect(result.hasFailures).toBe(true);
		expect(result.failed).toHaveLength(1);
		expect(result.failed[0].record).toEqual({ pk: 'a', sk: 'a' });
		expect(result.deleted).toHaveLength(2);
		expect(calls).toHaveLength(6);
	});

	test('put: unprocessed entries that match nothing sent are ignored', async () => {
		// DynamoDB should only echo back requests we sent, but a malformed or
		// unknown entry must not crash the batch or mark good items as failed.
		const garbage: WriteRequest[] = [
			{},
			{ PutRequest: { Item: { PK: { N: '1' }, SK: { S: 'SK_x' } } } },
			{
				PutRequest: {
					Item: { PK: { S: 'PK_ghost' }, SK: { S: 'SK_ghost' } },
				},
			},
		];
		const { facet, calls } = buildFacet(
			Array.from({ length: 6 }, () => unprocessed(garbage)),
		);

		const result = await facet.put([{ pk: 'a', sk: 'a' }]);

		expect(result.hasFailures).toBe(false);
		expect(result.failed).toEqual([]);
		expect(result.put).toEqual([{ pk: 'a', sk: 'a' }]);
		expect(calls).toHaveLength(6);
	});

	test('delete: unprocessed entries that match nothing sent are ignored', async () => {
		const garbage: WriteRequest[] = [
			// A put request inside a delete batch's unprocessed list.
			{ PutRequest: { Item: { PK: { S: 'PK_x' }, SK: { S: 'SK_x' } } } },
			{ DeleteRequest: { Key: { PK: { N: '1' }, SK: { S: 'SK_x' } } } },
			{
				DeleteRequest: {
					Key: { PK: { S: 'PK_ghost' }, SK: { S: 'SK_ghost' } },
				},
			},
		];
		const { facet, calls } = buildFacet(
			Array.from({ length: 6 }, () => unprocessed(garbage)),
		);

		const result = await facet.delete([{ pk: 'a', sk: 'a' }]);

		expect(result.hasFailures).toBe(false);
		expect(result.failed).toEqual([]);
		expect(result.deleted).toEqual([{ pk: 'a', sk: 'a' }]);
		expect(calls).toHaveLength(6);
	});
});

describe('batch write call failures', () => {
	/**
	 * Rejects any batch containing the `pk-0` item; other batches succeed.
	 * Lets a test prove that a thrown call fails only its own batch.
	 */
	function buildThrowingFacet(error: Error) {
		const ddb = new DynamoDB({
			region: 'us-east-1',
			endpoint: 'http://localhost:8000',
		});
		vi.spyOn(ddb, 'batchWriteItem').mockImplementation(
			async (input: BatchWriteItemCommandInput) => {
				const requests = input.RequestItems?.[TABLE_NAME] ?? [];
				const hasPoisonItem = requests.some(
					(request) =>
						request.PutRequest?.Item?.pk.S === 'pk-0' ||
						request.DeleteRequest?.Key?.PK.S === 'PK_pk-0',
				);
				if (hasPoisonItem) {
					throw error;
				}
				return emptyOutput();
			},
		);

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

	test('put: an error from one batch fails only that batch', async () => {
		const error = new Error('socket hang up');
		const facet = buildThrowingFacet(error);
		// pk-0..pk-24 land in the first batch, pk-25 in the second.
		const records = buildRecords(26);

		const result = await facet.put(records);

		expect(result.hasFailures).toBe(true);
		expect(result.failed.map((f) => f.record.pk)).toEqual(
			records.slice(0, 25).map((r) => r.pk),
		);
		for (const failure of result.failed) {
			expect(failure.error).toBe(error);
		}
		expect(result.put.map((r) => r.pk)).toEqual(['pk-25']);
	});

	test('delete: an error from one batch fails only that batch', async () => {
		const error = new Error('socket hang up');
		const facet = buildThrowingFacet(error);
		const records = buildRecords(26);

		const result = await facet.delete(records);

		expect(result.hasFailures).toBe(true);
		expect(result.failed.map((f) => f.record.pk)).toEqual(
			records.slice(0, 25).map((r) => r.pk),
		);
		for (const failure of result.failed) {
			expect(failure.error).toBe(error);
		}
		expect(result.deleted.map((r) => r.pk)).toEqual(['pk-25']);
	});
});

describe('batch fan-out concurrency', () => {
	test('putItems caps in-flight requests at the default (8)', async () => {
		const { facet, getPeak } = buildConcurrencyFacet();
		// 50 batches (25 items each) ⇒ plenty of room to exceed the cap if unbounded.
		const result = await facet.put(buildRecords(50 * 25));
		expect(result.hasFailures).toBe(false);
		expect(getPeak()).toBe(8);
	});

	test('putItems honours a caller-supplied concurrency override', async () => {
		const { facet, getPeak } = buildConcurrencyFacet();
		await facet.put(buildRecords(20 * 25), { concurrency: 3 });
		expect(getPeak()).toBe(3);
	});

	test('deleteItems caps in-flight requests at the default (8)', async () => {
		const { facet, getPeak } = buildConcurrencyFacet();
		const result = await facet.delete(buildRecords(50 * 25));
		expect(result.hasFailures).toBe(false);
		expect(getPeak()).toBe(8);
	});

	test('deleteItems honours a caller-supplied concurrency override', async () => {
		const { facet, getPeak } = buildConcurrencyFacet();
		await facet.delete(buildRecords(20 * 25), { concurrency: 2 });
		expect(getPeak()).toBe(2);
	});
});
