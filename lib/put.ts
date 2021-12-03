import type { PutItemInput, WriteRequest } from 'aws-sdk/clients/dynamodb';
import type { Facet } from './facet';
import { wait } from './wait';
import { Converter } from '@faceteer/converter';
import { condition, ConditionExpression } from '@faceteer/expression-builder';

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

export async function putSingleItem<T, PK extends keyof T, SK extends keyof T>(
	facet: Facet<T, PK, SK>,
	record: T,
	options: PutOptions<T> = {},
): Promise<PutSingleItemResponse<T>> {
	try {
		const putInput: PutItemInput = {
			Item: facet.in(record),
			TableName: facet.connection.tableName,
		};

		if (options.condition) {
			const expression = condition(options.condition);
			putInput.ConditionExpression = expression.expression;
			putInput.ExpressionAttributeNames = expression.names;
			putInput.ExpressionAttributeValues = expression.values;
		}

		const response = await facet.connection.dynamoDb
			.putItem(putInput)
			.promise();

		if (response.$response.error) {
			return {
				record,
				wasSuccessful: false,
				error: response.$response.error,
			};
		}

		return {
			record: facet.out(putInput.Item),
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
export async function putItems<T, PK extends keyof T, SK extends keyof T>(
	facet: Facet<T, PK, SK>,
	records: T[],
): Promise<PutResponse<T>> {
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

	const batchPromises = batches.map((batch) => putBatch(facet, batch));
	const putResults = await Promise.allSettled(batchPromises);
	for (const [index, result] of putResults.entries()) {
		if (result.status === 'rejected') {
			const failedBatch = batches[index];
			putResponse.failed.push(
				...failedBatch.map((failedItem) => {
					return {
						record: failedItem,
						error: result.reason,
					};
				}),
			);
		} else {
			putResponse.failed.push(...result.value.failed);
			putResponse.put.push(...result.value.put);
		}
	}

	putResponse.hasFailures = putResponse.failed.length > 0;

	return putResponse;
}

/**
 * Put a batch of records into Dynamo DB.
 *
 * This function expects the batch to be 25
 * records or less
 * @param batchToPut
 */
async function putBatch<T, PK extends keyof T, SK extends keyof T>(
	facet: Facet<T, PK, SK>,
	batchToPut: T[],
): Promise<PutResponse<T>> {
	const writeRequests: Record<string, WriteRequest> = {};
	const putResponse: PutResponse<T> = {
		failed: [],
		hasFailures: false,
		put: [],
	};

	/**
	 * We keep track of the items by their key so
	 * we can return any failed requests
	 */
	const itemsByKey: Record<string, T> = {};

	/**
	 * We can't have duplicate items in a batch so we extract
	 * the SK and PK to make sure the batch only has unique items
	 */
	for (const batchItem of batchToPut) {
		const item = facet.in(batchItem);
		const key = facet.pk(batchItem) + facet.sk(batchItem);
		writeRequests[key] = {
			PutRequest: {
				Item: item,
			},
		};
		itemsByKey[key] = facet.out(item);
	}

	const result = await facet.connection.dynamoDb
		.batchWriteItem({
			RequestItems: {
				[facet.connection.tableName]: Object.values(writeRequests),
			},
		})
		.promise();

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

			const retryResult = await facet.connection.dynamoDb
				.batchWriteItem({
					RequestItems: {
						[facet.connection.tableName]: unprocessed.splice(0),
					},
				})
				.promise();

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
			if (unprocessedRequest.PutRequest) {
				// We use the PK and SK of the put request in order
				// to rebuild the primary key and get the original item
				const item = Converter.unmarshall(unprocessedRequest.PutRequest.Item);
				const failedItem = itemsByKey[item.PK + item.SK];
				if (failedItem) {
					putResponse.failed.push({
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

	putResponse.put = [...Object.values(itemsByKey)];
	putResponse.hasFailures = putResponse.failed.length > 0;
	return putResponse;
}
