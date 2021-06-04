import type { DynamoDB } from 'aws-sdk';
import type { Facet } from './facet';
import { PK, SK } from './keys';

export async function getSingleItem<T>(
	facet: Facet<T>,
	query: Partial<T>,
	dynamoDb: DynamoDB,
	tableName: string,
) {
	const result = await dynamoDb
		.getItem({
			TableName: tableName,
			Key: {
				[PK]: {
					S: facet.pk(query),
				},
				[SK]: {
					S: facet.sk(query),
				},
			},
		})
		.promise();

	/**
	 * Throw if we get an error
	 */
	if (result.$response.error) {
		throw result.$response.error;
	}

	/**
	 * If we got the record, return it
	 */
	if (result.Item) {
		return facet.out(result.Item);
	}

	/**
	 * Return nothing if we didn't get the item
	 */
	return null;
}

/**
 * Get a batch of items from Dynamo DB.
 *
 * This function should be called after we've made sure that
 * the batches only have a maximum of 100 items
 * @param queries
 */
export async function getBatch<T>(
	facet: Facet<T>,
	queries: Partial<T>[],
	dynamoDb: DynamoDB,
	tableName: string,
): Promise<T[]> {
	/**
	 * An array of all the items we found
	 */
	const items: T[] = [];

	/**
	 * Function to gather items from a batch response
	 */
	const gatherItems = (batchResponse?: DynamoDB.BatchGetResponseMap) => {
		if (batchResponse && batchResponse[tableName]) {
			const itemsFromResponse = batchResponse[tableName].map((item) =>
				facet.out(item),
			);
			items.push(...itemsFromResponse);
		}
	};

	const keysToGet: DynamoDB.KeyList = queries.map((query) => {
		return {
			[PK]: {
				S: facet.pk(query),
			},
			[SK]: {
				S: facet.sk(query),
			},
		};
	});

	const results = await getBatchKeys(keysToGet, dynamoDb, tableName);
	const { Responses, UnprocessedKeys } = results;
	/**
	 * Collect all of the responses
	 */
	gatherItems(Responses);

	/**
	 * Retry any unprocessed keys
	 */
	let attempts = 0;
	if (UnprocessedKeys && UnprocessedKeys[tableName]) {
		/**
		 * We will keep putting unprocessed items into this array
		 * until we don't have any unprocessed items left
		 */
		const unprocessed = [...UnprocessedKeys[tableName].Keys];

		while (unprocessed.length > 0 && attempts < 10) {
			attempts += 1;
			/**
			 * Retry the unprocessed keys
			 */
			const { Responses: RetriedResponses, UnprocessedKeys: StillUnprocessed } =
				await getBatchKeys(unprocessed.splice(0), dynamoDb, tableName);

			/**
			 * Gather any results
			 */
			gatherItems(RetriedResponses);

			/**
			 * If we have any items that are still unprocessed we'll
			 * add them back to the unprocessed array so we can retry them
			 */
			if (StillUnprocessed && StillUnprocessed[tableName]) {
				unprocessed.push(...StillUnprocessed[tableName].Keys);
			}
		}
	}

	return items;
}

/**
 * Get any number of items from Dynamo DB.
 *
 * This function will split the items into
 * batches of 100 if needed
 * @param queries
 */
export async function getBatchItems<T>(
	facet: Facet<T>,
	queries: Partial<T>[],
	dynamoDb: DynamoDB,
	tableName: string,
): Promise<T[]> {
	const queriesToBatch = [...queries];
	/**
	 * Dynamo DB only allows 100 items in a batch request
	 * so we will break this down into batches
	 */
	const batches: Partial<T>[][] = [];

	while (queriesToBatch.length >= 1) {
		batches.push(queriesToBatch.splice(0, 100));
	}

	/**
	 * Create all of the promises for every batch
	 */
	const batchPromises = batches.map((batch) =>
		getBatch(facet, batch, dynamoDb, tableName),
	);

	/**
	 * Collect all of the batch results
	 */
	const batchResults = await Promise.all(batchPromises);
	return batchResults.flat(1);
}

/**
 * Make a batch request to Dynamo DB to get
 * specific keys
 * @param keys
 * @returns
 */
async function getBatchKeys(
	keys: DynamoDB.KeyList,
	dynamoDb: DynamoDB,
	tableName: string,
) {
	return dynamoDb
		.batchGetItem({
			RequestItems: {
				[tableName]: {
					Keys: keys,
				},
			},
		})
		.promise();
}
