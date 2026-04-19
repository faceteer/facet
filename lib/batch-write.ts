import type { DynamoDB, WriteRequest } from '@aws-sdk/client-dynamodb';
import { wait } from './wait';

/**
 * Adapts a domain-specific record (put payload or delete key) to the
 * DynamoDB `WriteRequest` shape and back, so `batchWriteWithRetry`
 * can stay generic across put and delete batches.
 */
export interface BatchWriteAdapter<U, O> {
	/**
	 * Translate one input item into the WriteRequest to send to DynamoDB,
	 * the dedup/lookup key used to attribute failures, and the success
	 * output shape that the caller expects.
	 */
	prepare(item: U): { request: WriteRequest; key: string; output: O };

	/**
	 * Extract the dedup/lookup key from a WriteRequest echoed back in
	 * `UnprocessedItems`. Return `undefined` if the request does not
	 * belong to this adapter.
	 */
	keyForRequest(request: WriteRequest): string | undefined;
}

export interface BatchFailure<U> {
	record: U;
	error: unknown;
}

export interface BatchWriteResult<U, O> {
	ok: O[];
	failed: BatchFailure<U>[];
}

export interface BatchWriteConnection {
	tableName: string;
	dynamoDb: Pick<DynamoDB, 'batchWriteItem'>;
}

const MAX_RETRIES = 5;
const BASE_BACKOFF_MS = 10;

/**
 * Send up to 25 writes as a single DynamoDB `BatchWriteItem` call,
 * retry any `UnprocessedItems` with exponential backoff, and report
 * whichever items could not be written after all retries.
 *
 * Duplicate input items (same composite key) are collapsed; the last
 * one wins, matching the behaviour callers relied on before this
 * helper was extracted.
 */
export async function batchWriteWithRetry<U, O>(
	connection: BatchWriteConnection,
	items: U[],
	adapter: BatchWriteAdapter<U, O>,
): Promise<BatchWriteResult<U, O>> {
	const { tableName, dynamoDb } = connection;

	const itemsByKey = new Map<string, U>();
	const outputsByKey = new Map<string, O>();
	const requestsByKey = new Map<string, WriteRequest>();

	for (const item of items) {
		const { request, key, output } = adapter.prepare(item);
		itemsByKey.set(key, item);
		outputsByKey.set(key, output);
		requestsByKey.set(key, request);
	}

	const initialResult = await dynamoDb.batchWriteItem({
		RequestItems: {
			[tableName]: Array.from(requestsByKey.values()),
		},
	});

	let unprocessed: WriteRequest[] = [
		...(initialResult.UnprocessedItems?.[tableName] ?? []),
	];

	for (
		let retries = 0;
		unprocessed.length > 0 && retries < MAX_RETRIES;
		retries += 1
	) {
		await wait(BASE_BACKOFF_MS * 2 ** (retries + 1));
		const retryResult = await dynamoDb.batchWriteItem({
			RequestItems: { [tableName]: unprocessed },
		});
		unprocessed = [...(retryResult.UnprocessedItems?.[tableName] ?? [])];
	}

	const failed: BatchFailure<U>[] = [];
	for (const request of unprocessed) {
		const key = adapter.keyForRequest(request);
		if (key === undefined) {
			continue;
		}
		const failedItem = itemsByKey.get(key);
		if (failedItem !== undefined) {
			failed.push({
				record: failedItem,
				error: new Error('Item was not processed'),
			});
			outputsByKey.delete(key);
		}
	}

	return {
		ok: Array.from(outputsByKey.values()),
		failed,
	};
}
