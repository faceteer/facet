import type {
	KeysAndAttributes,
	BatchGetItemOutput,
} from '@aws-sdk/client-dynamodb';
import type { Facet } from './facet';
import { PK, SK } from './keys';
import { wait } from './wait';

export async function getSingleItem<T, PK extends keyof T, SK extends keyof T>(
	facet: Facet<T, PK, SK>,
	query: Partial<T>,
) {
	const result = await facet.connection.dynamoDb.getItem({
		TableName: facet.connection.tableName,
		Key: {
			[PK]: {
				S: facet.pk(query),
			},
			[SK]: {
				S: facet.sk(query),
			},
		},
	});

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
export async function getBatch<T, PK extends keyof T, SK extends keyof T>(
	facet: Facet<T, PK, SK>,
	queries: Partial<T>[],
): Promise<T[]> {
	/**
	 * An array of all the items we found
	 */
	const items: T[] = [];

	/**
	 * Function to gather items from a batch response
	 */
	const gatherItems = (batchResponse?: BatchGetItemOutput['Responses']) => {
		if (batchResponse && batchResponse[facet.connection.tableName]) {
			const itemsFromResponse = batchResponse[facet.connection.tableName].map(
				(item) => facet.out(item),
			);
			items.push(...itemsFromResponse);
		}
	};

	const keysToGet: KeysAndAttributes['Keys'] = queries.map((query) => {
		return {
			[PK]: {
				S: facet.pk(query),
			},
			[SK]: {
				S: facet.sk(query),
			},
		};
	});

	const results = await getBatchKeys(keysToGet, facet);
	const { Responses, UnprocessedKeys } = results;
	/**
	 * Collect all of the responses
	 */
	gatherItems(Responses);

	/**
	 * Retry any unprocessed keys
	 */
	let attempts = 0;
	if (UnprocessedKeys && UnprocessedKeys[facet.connection.tableName]) {
		/**
		 * We will keep putting unprocessed items into this array
		 * until we don't have any unprocessed items left
		 */
		const unprocessed = [
			...(UnprocessedKeys[facet.connection.tableName].Keys ?? []),
		];

		while (unprocessed.length > 0 && attempts < 10) {
			attempts += 1;

			/**
			 * Wait a short bit before retrying
			 */
			await wait(10 * 2 ** attempts);

			/**
			 * Retry the unprocessed keys
			 */
			const { Responses: RetriedResponses, UnprocessedKeys: StillUnprocessed } =
				await getBatchKeys(unprocessed.splice(0), facet);

			/**
			 * Gather any results
			 */
			gatherItems(RetriedResponses);

			/**
			 * If we have any items that are still unprocessed we'll
			 * add them back to the unprocessed array so we can retry them
			 */
			if (StillUnprocessed && StillUnprocessed[facet.connection.tableName]) {
				unprocessed.push(
					...(StillUnprocessed[facet.connection.tableName].Keys ?? []),
				);
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
export async function getBatchItems<T, PK extends keyof T, SK extends keyof T>(
	facet: Facet<T, PK, SK>,
	queries: Partial<T>[],
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
	const batchPromises = batches.map((batch) => getBatch(facet, batch));

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
async function getBatchKeys<T, PK extends keyof T, SK extends keyof T>(
	keys: KeysAndAttributes['Keys'],
	facet: Facet<T, PK, SK>,
) {
	return facet.connection.dynamoDb.batchGetItem({
		RequestItems: {
			[facet.connection.tableName]: {
				Keys: keys,
			},
		},
	});
}
