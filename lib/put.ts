import type { PutItemInput } from '@aws-sdk/client-dynamodb';
import type { Facet, WithoutReservedAttributes } from './facet';
import type { Keys } from './keys';
import { Converter } from '@faceteer/converter';
import type { ConditionExpression } from '@faceteer/expression-builder';
import { batchWriteWithRetry, type BatchWriteAdapter } from './batch-write';
import { applyCondition } from './condition';

export interface PutOptions<T> {
	condition?: ConditionExpression<T>;
}

/**
 * A failed put request
 */
export interface PutFailure<T> {
	/**
	 * The record that failed to be put into Dynamo DB
	 */
	record: T;
	/**
	 * The error from trying to put the item
	 */
	error: unknown;
}

/**
 * A response from a put request to a Facet
 */
export interface PutResponse<T> {
	/**
	 * If the request had any failures
	 */
	hasFailures: boolean;
	/**
	 * The records that were successfully put into
	 * Dynamo DB
	 */
	put: T[];
	/**
	 * The records that failed to be put into Dynamo DB
	 */
	failed: PutFailure<T>[];
}

/**
 * Response when we put a single a single item
 */
export interface PutSingleItemResponse<T> {
	/**
	 * If the request had any failures
	 */
	wasSuccessful: boolean;
	record: T;
	error?: unknown;
}

export async function putSingleItem<
	T extends WithoutReservedAttributes<T>,
	PK extends Keys<T>,
	SK extends Keys<T>,
>(
	facet: Facet<T, PK, SK>,
	record: T,
	options: PutOptions<T> = {},
): Promise<PutSingleItemResponse<T>> {
	try {
		const item = facet.in(record);
		const putInput: PutItemInput = {
			Item: item,
			TableName: facet.connection.tableName,
		};

		if (options.condition) {
			applyCondition(putInput, options.condition);
		}

		await facet.connection.dynamoDb.putItem(putInput);

		return {
			record: facet.out(item),
			wasSuccessful: true,
		};
	} catch (error) {
		return {
			record,
			wasSuccessful: false,
			error: error,
		};
	}
}

/**
 * Put records into the Dynamo DB table
 * @param records
 */
export async function putItems<
	T extends WithoutReservedAttributes<T>,
	PK extends Keys<T>,
	SK extends Keys<T>,
>(facet: Facet<T, PK, SK>, records: T[]): Promise<PutResponse<T>> {
	const recordsToBatch: T[] = [...records];
	const putResponse: PutResponse<T> = {
		failed: [],
		hasFailures: false,
		put: [],
	};
	/**
	 * Dynamo DB only allows 25 items in a write batch
	 * request so we will break this down into batches
	 */
	const batches: T[][] = [];

	while (recordsToBatch.length >= 1) {
		batches.push(recordsToBatch.splice(0, 25));
	}

	const adapter = putAdapter(facet);
	const batchResults = await Promise.allSettled(
		batches.map((batch) =>
			batchWriteWithRetry(facet.connection, batch, adapter),
		),
	);
	for (const [index, result] of batchResults.entries()) {
		if (result.status === 'rejected') {
			const failedBatch = batches[index];
			const error: unknown = result.reason;
			putResponse.failed.push(
				...failedBatch.map((failedItem) => ({
					record: failedItem,
					error,
				})),
			);
		} else {
			putResponse.put.push(...result.value.ok);
			putResponse.failed.push(...result.value.failed);
		}
	}

	putResponse.hasFailures = putResponse.failed.length > 0;

	return putResponse;
}

function putAdapter<
	T extends WithoutReservedAttributes<T>,
	PK extends Keys<T>,
	SK extends Keys<T>,
>(facet: Facet<T, PK, SK>): BatchWriteAdapter<T, T> {
	return {
		prepare(record) {
			const item = facet.in(record);
			return {
				request: { PutRequest: { Item: item } },
				key: facet.pk(record) + facet.sk(record),
				output: facet.out(item),
			};
		},
		keyForRequest(request) {
			if (!request.PutRequest?.Item) {
				return undefined;
			}
			const item = Converter.unmarshall(request.PutRequest.Item) as Record<
				string,
				unknown
			>;
			if (typeof item.PK !== 'string' || typeof item.SK !== 'string') {
				return undefined;
			}
			return item.PK + item.SK;
		},
	};
}
