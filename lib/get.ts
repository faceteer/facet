import type {
	GetItemInput,
	KeysAndAttributes,
	BatchGetItemOutput,
} from '@aws-sdk/client-dynamodb';
import type { Facet, WithoutReservedAttributes } from './facet';
import { PK, SK, Keys } from './keys';
import { buildProjectionExpression } from './projection';
import { wait } from './wait';

export interface GetOptions<T, K extends keyof T = keyof T> {
	/**
	 * Restrict the read to a subset of attributes via a
	 * Dynamo DB `ProjectionExpression`. The facet's PK and SK fields are
	 * always included in the result (they are load-bearing for identity),
	 * even if omitted from `select`.
	 *
	 * Requires `pickValidator` on the facet; throws otherwise.
	 */
	select?: readonly [K, ...K[]];
}

export async function getSingleItem<
	T extends WithoutReservedAttributes<T>,
	PartitionKey extends Keys<T>,
	SortKey extends Keys<T>,
	K extends keyof T = keyof T,
>(
	facet: Facet<T, PartitionKey, SortKey>,
	query: Partial<T>,
	options: GetOptions<T, K> = {},
): Promise<T | Pick<T, K | PartitionKey | SortKey> | null> {
	const input: GetItemInput = {
		TableName: facet.connection.tableName,
		Key: {
			[PK]: {
				S: facet.pk(query),
			},
			[SK]: {
				S: facet.sk(query),
			},
		},
	};

	const projectedKeys = options.select
		? ([...options.select, ...facet.keyFields] as readonly (
				| K
				| PartitionKey
				| SortKey
			)[])
		: undefined;

	if (projectedKeys) {
		const projection = buildProjectionExpression(projectedKeys);
		input.ProjectionExpression = projection.expression;
		input.ExpressionAttributeNames = projection.names;
	}

	const result = await facet.connection.dynamoDb.getItem(input);

	if (!result.Item) {
		return null;
	}

	if (projectedKeys) {
		return facet.pick(result.Item, projectedKeys);
	}
	return facet.out(result.Item);
}

/**
 * Get a batch of items from Dynamo DB.
 *
 * This function should be called after we've made sure that
 * the batches only have a maximum of 100 items
 * @param queries
 */
export async function getBatch<
	T extends WithoutReservedAttributes<T>,
	PartitionKey extends Keys<T>,
	SortKey extends Keys<T>,
	K extends keyof T = keyof T,
>(
	facet: Facet<T, PartitionKey, SortKey>,
	queries: Partial<T>[],
	options: GetOptions<T, K> = {},
): Promise<(T | Pick<T, K | PartitionKey | SortKey>)[]> {
	const items: (T | Pick<T, K | PartitionKey | SortKey>)[] = [];

	const projectedKeys = options.select
		? ([...options.select, ...facet.keyFields] as readonly (
				| K
				| PartitionKey
				| SortKey
			)[])
		: undefined;

	const gatherItems = (batchResponse?: BatchGetItemOutput['Responses']) => {
		if (batchResponse?.[facet.connection.tableName]) {
			const itemsFromResponse = batchResponse[facet.connection.tableName].map(
				(item) =>
					projectedKeys ? facet.pick(item, projectedKeys) : facet.out(item),
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

	const results = await getBatchKeys(keysToGet, facet, projectedKeys);
	const { Responses, UnprocessedKeys } = results;
	gatherItems(Responses);

	let attempts = 0;
	if (UnprocessedKeys?.[facet.connection.tableName]) {
		const unprocessed = [
			...(UnprocessedKeys[facet.connection.tableName].Keys ?? []),
		];

		while (unprocessed.length > 0 && attempts < 10) {
			attempts += 1;

			await wait(10 * 2 ** attempts);

			const { Responses: RetriedResponses, UnprocessedKeys: StillUnprocessed } =
				await getBatchKeys(unprocessed.splice(0), facet, projectedKeys);

			gatherItems(RetriedResponses);

			if (StillUnprocessed?.[facet.connection.tableName]) {
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
export async function getBatchItems<
	T extends WithoutReservedAttributes<T>,
	PartitionKey extends Keys<T>,
	SortKey extends Keys<T>,
	K extends keyof T = keyof T,
>(
	facet: Facet<T, PartitionKey, SortKey>,
	queries: Partial<T>[],
	options: GetOptions<T, K> = {},
): Promise<(T | Pick<T, K | PartitionKey | SortKey>)[]> {
	const queriesToBatch = [...queries];
	const batches: Partial<T>[][] = [];

	while (queriesToBatch.length >= 1) {
		batches.push(queriesToBatch.splice(0, 100));
	}

	const batchPromises = batches.map((batch) => getBatch(facet, batch, options));

	const batchResults = await Promise.all(batchPromises);
	return batchResults.flat(1);
}

/**
 * Make a batch request to Dynamo DB to get
 * specific keys
 */
async function getBatchKeys<
	T extends WithoutReservedAttributes<T>,
	PartitionKey extends Keys<T>,
	SortKey extends Keys<T>,
>(
	keys: KeysAndAttributes['Keys'],
	facet: Facet<T, PartitionKey, SortKey>,
	projectedKeys?: readonly PropertyKey[],
) {
	const tableRequest: KeysAndAttributes = {
		Keys: keys,
	};
	if (projectedKeys) {
		const projection = buildProjectionExpression(projectedKeys);
		tableRequest.ProjectionExpression = projection.expression;
		tableRequest.ExpressionAttributeNames = projection.names;
	}
	return facet.connection.dynamoDb.batchGetItem({
		RequestItems: {
			[facet.connection.tableName]: tableRequest,
		},
	});
}
