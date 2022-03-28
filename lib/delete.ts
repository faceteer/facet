import type { WriteRequest, DeleteItemInput } from '@aws-sdk/client-dynamodb';
import type { Facet } from './facet';
import { wait } from './wait';
import { Converter } from '@faceteer/converter';
import expressionBuilder from '@faceteer/expression-builder';
import { PK, SK, Keys } from './keys';

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
	U = Pick<T, PK | SK> & Partial<T>,
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
	U = Pick<T, PK | SK> & Partial<T>,
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

	const batchPromises = batches.map((batch) => deleteBatch(facet, batch));
	const putResults = await Promise.allSettled(batchPromises);
	for (const [index, result] of putResults.entries()) {
		if (result.status === 'rejected') {
			const failedBatch = batches[index];
			deleteResponse.failed.push(
				...failedBatch.map((failedItem) => {
					return {
						record: failedItem,
						error: result.reason,
					};
				}),
			);
		} else {
			deleteResponse.failed.push(...result.value.failed);
			deleteResponse.deleted.push(...result.value.deleted);
		}
	}

	deleteResponse.hasFailures = deleteResponse.failed.length > 0;

	return deleteResponse;
}

/**
 * Delete a batch of records into Dynamo DB.
 *
 * This function expects the batch to be 25
 * records or less
 * @param batchToDelete
 */
async function deleteBatch<
	T,
	PK extends Keys<T>,
	SK extends Keys<T>,
	U = Pick<T, PK | SK> & Partial<T>,
>(facet: Facet<T, PK, SK>, batchToDelete: U[]): Promise<DeleteResponse<U>> {
	const deleteRequests: Record<string, WriteRequest> = {};
	const deleteResponse: DeleteResponse<U> = {
		failed: [],
		hasFailures: false,
		deleted: [],
	};

	/**
	 * We keep track of the items by their key so
	 * we can return any failed requests
	 */
	const itemsByKey: Record<string, U> = {};

	/**
	 * We can't have duplicate items in a batch so we extract
	 * the SK and PK to make sure the batch only has unique items
	 */
	for (const batchItem of batchToDelete) {
		const key = facet.pk(batchItem) + facet.sk(batchItem);
		deleteRequests[key] = {
			DeleteRequest: {
				Key: {
					[PK]: {
						S: facet.pk(batchItem),
					},
					[SK]: {
						S: facet.sk(batchItem),
					},
				},
			},
		};
		itemsByKey[key] = batchItem;
	}

	const result = await facet.connection.dynamoDb.batchWriteItem({
		RequestItems: {
			[facet.connection.tableName]: Object.values(deleteRequests),
		},
	});

	/**
	 * Attempt to put any unprocessed items into the database
	 */
	if (
		result.UnprocessedItems &&
		result.UnprocessedItems[facet.connection.tableName]
	) {
		const unprocessed = [
			...result.UnprocessedItems[facet.connection.tableName],
		];
		let retries = 0;

		while (unprocessed.length > 0 && retries < 5) {
			retries += 1;

			/**
			 * Wait a short bit before retrying
			 */
			await wait(10 * 2 ** retries);

			const retryResult = await facet.connection.dynamoDb.batchWriteItem({
				RequestItems: {
					[facet.connection.tableName]: unprocessed.splice(0),
				},
			});

			if (
				retryResult.UnprocessedItems &&
				retryResult.UnprocessedItems[facet.connection.tableName]
			) {
				unprocessed.push(
					...retryResult.UnprocessedItems[facet.connection.tableName],
				);
			}
		}
	}

	/**
	 * If we still have unprocessed items it means that we weren't able
	 * to successfully retry them.
	 */
	if (
		result.UnprocessedItems &&
		result.UnprocessedItems[facet.connection.tableName]
	) {
		for (const unprocessedRequest of result.UnprocessedItems[
			facet.connection.tableName
		]) {
			if (unprocessedRequest.PutRequest?.Item) {
				// We use the PK and SK of the delete request in order
				// to rebuild the primary key and get the original item
				const item = Converter.unmarshall(unprocessedRequest.PutRequest.Item);
				const failedItem = itemsByKey[item.PK + item.SK];
				if (failedItem) {
					deleteResponse.failed.push({
						error: new Error('Item was not processed'),
						record: failedItem,
					});
					// We delete the item from itemsByKey since we use
					// that to generate the successful items later
					delete itemsByKey[item.PK + item.SK];
				}
			}
		}
	}

	deleteResponse.deleted = [...Object.values(itemsByKey)];
	deleteResponse.hasFailures = deleteResponse.failed.length > 0;
	return deleteResponse;
}
