import type { DeleteItemInput } from '@aws-sdk/client-dynamodb';
import type { Facet } from './facet';
import expressionBuilder from '@faceteer/expression-builder';
import { PK, SK, Keys } from './keys';
import {
	batchWriteWithRetry,
	type BatchWriteAdapter,
} from './batch-write';

export interface DeleteOptions<T> {
	condition?: expressionBuilder.ConditionExpression<T>;
}

/**
 * A failed delete request
 */
export interface DeleteFailure<T> {
	/**
	 * The record that failed to be deleted
	 */
	record: T;
	/**
	 * The error from trying to delete the item
	 */
	error: unknown;
}

/**
 * A response from a delete request to a Facet
 */
export interface DeleteResponse<T> {
	/**
	 * If the request had any failures
	 */
	hasFailures: boolean;
	/**
	 * The records that were successfully deleted
	 */
	deleted: T[];
	/**
	 * The records that failed to be deleted
	 */
	failed: DeleteFailure<T>[];
}

export async function deleteSingleItem<
	T,
	PK extends Keys<T>,
	SK extends Keys<T>,
	U extends Partial<T> = Pick<T, PK | SK> & Partial<T>,
>(
	facet: Facet<T, PK, SK>,
	record: U,
	options: DeleteOptions<T> = {},
): Promise<DeleteResponse<U>> {
	try {
		const deleteInput: DeleteItemInput = {
			TableName: facet.connection.tableName,
			Key: {
				[PK]: {
					S: facet.pk(record),
				},
				[SK]: {
					S: facet.sk(record),
				},
			},
		};

		if (options.condition) {
			const expression = expressionBuilder.condition(options.condition);
			deleteInput.ConditionExpression = expression.expression;
			deleteInput.ExpressionAttributeNames = expression.names;
			deleteInput.ExpressionAttributeValues = expression.values;
		}

		await facet.connection.dynamoDb.deleteItem(deleteInput);

		return {
			failed: [],
			hasFailures: false,
			deleted: [record],
		};
	} catch (error) {
		return {
			failed: [
				{
					error: error,
					record,
				},
			],
			hasFailures: true,
			deleted: [],
		};
	}
}

/**
 * Delete records from the from Dynamo DB
 * @param records
 */
export async function deleteItems<
	T,
	PK extends Keys<T>,
	SK extends Keys<T>,
	U extends Partial<T> = Pick<T, PK | SK> & Partial<T>,
>(facet: Facet<T, PK, SK>, records: U[]): Promise<DeleteResponse<U>> {
	const recordsToBatch: U[] = [...records];
	const deleteResponse: DeleteResponse<U> = {
		failed: [],
		hasFailures: false,
		deleted: [],
	};
	/**
	 * Dynamo DB only allows 25 items in a write batch
	 * request so we will break this down into batches
	 */
	const batches: U[][] = [];

	while (recordsToBatch.length >= 1) {
		batches.push(recordsToBatch.splice(0, 25));
	}

	const adapter = deleteAdapter<T, PK, SK, U>(facet);
	const batchResults = await Promise.allSettled(
		batches.map((batch) =>
			batchWriteWithRetry(facet.connection, batch, adapter),
		),
	);
	for (const [index, result] of batchResults.entries()) {
		if (result.status === 'rejected') {
			const failedBatch = batches[index];
			deleteResponse.failed.push(
				...failedBatch.map((failedItem) => ({
					record: failedItem,
					error: result.reason,
				})),
			);
		} else {
			deleteResponse.deleted.push(...result.value.ok);
			deleteResponse.failed.push(...result.value.failed);
		}
	}

	deleteResponse.hasFailures = deleteResponse.failed.length > 0;

	return deleteResponse;
}

function deleteAdapter<
	T,
	PartitionKey extends Keys<T>,
	SortKey extends Keys<T>,
	U extends Partial<T>,
>(facet: Facet<T, PartitionKey, SortKey>): BatchWriteAdapter<U, U> {
	return {
		prepare(record) {
			const pk = facet.pk(record);
			const sk = facet.sk(record);
			return {
				request: {
					DeleteRequest: {
						Key: {
							[PK]: { S: pk },
							[SK]: { S: sk },
						},
					},
				},
				key: pk + sk,
				output: record,
			};
		},
		keyForRequest(request) {
			const key = request.DeleteRequest?.Key;
			if (!key) {
				return undefined;
			}
			const pk = key[PK]?.S;
			const sk = key[SK]?.S;
			if (pk === undefined || sk === undefined) {
				return undefined;
			}
			return pk + sk;
		},
	};
}
