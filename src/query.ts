import { decodeCursor, encodeCursor } from './cursor';
import { Facet, FacetIndex } from './facet';
import { IndexKeyNameMap, PK, SK } from './keys';

export interface PartitionQueryOptions<T> {
	facet: Facet<T>;
	partitionIdentifier: Partial<T>;
	index?: FacetIndex<T>;
	shard?: number;
}

enum Comparison {
	Greater = '>',
	GreaterOrEqual = '>=',
	Less = '<',
	LessOrEqual = '<=',
}

export interface QueryResult<T> {
	cursor?: string;
	records: T[];
}

export interface QueryOptions {
	/**
	 * The primary key of the first record that this operation will evaluate.
	 * Use the value that was returned for `cursor` in the previous operation.
	 */
	cursor?: string;
	/**
	 * Specifies the order for index traversal: If true (default),
	 * the traversal is performed in ascending order; if false, the
	 * traversal is performed in descending order.
	 */
	scanForward?: boolean;
	/**
	 * The maximum number of records to evaluate (not necessarily the number of matching records).
	 * If DynamoDB processes the number of records up to the limit while processing the results,
	 * it stops the operation and returns the matching values up to that point, and a cursor
	 * to apply in a subsequent operation, so that you can pick up where you left off.
	 *
	 * Also, if the processed dataset size exceeds 1 MB before DynamoDB reaches this limit,
	 * it stops the operation and returns the matching values up to the limit, and a cursor
	 * to apply in a subsequent operation to continue the operation.
	 */
	limit?: number;
	/**
	 * If a key has a distribution configuration this option will specify
	 * which group to get.
	 */
	shard?: number;
}

export class PartitionQuery<T> {
	#facet: Facet<T>;
	#index?: FacetIndex<T>;
	#PK: string;
	#SK: string;
	partition: string;

	constructor({
		facet,
		partitionIdentifier,
		index,
		shard,
	}: PartitionQueryOptions<T>) {
		this.#facet = facet;
		this.#index = index;

		if (this.#index) {
			const IndexKeys = IndexKeyNameMap[this.#index.name];
			this.#PK = IndexKeys.PK;
			this.#SK = IndexKeys.SK;
			this.partition = this.#index.pk(partitionIdentifier, shard);
		} else {
			this.#PK = PK;
			this.#SK = SK;
			this.partition = this.#facet.pk(partitionIdentifier, shard);
		}
	}

	/**
	 * Get records from a partition by a comparison with the sort key.
	 */
	private async compare(
		comparison: Comparison,
		sort: Partial<T> | string,
		{ cursor, limit, scanForward = true, shard }: QueryOptions,
	) {
		const { dynamoDb, tableName } = this.#facet.connection;

		const queryResult: QueryResult<T> = {
			records: [],
		};

		let sortKey: string;
		/**
		 * If we were given a string we'll use it, otherwise we'll
		 * create the sort key using the getKey function of the facet
		 */
		if (typeof sort === 'string') {
			sortKey = sort;
		} else {
			sortKey = this.#index
				? this.#index.sk(sort)
				: this.#facet.sk(sort, shard);
		}

		const lastEvaluatedKey = decodeCursor(cursor);

		const results = await dynamoDb
			.query({
				TableName: tableName,
				IndexName: this.#index?.name,
				KeyConditionExpression: `#PK = :partition AND #SK ${comparison} :sort`,
				ExpressionAttributeNames: {
					'#PK': this.#PK,
					'#SK': this.#SK,
				},
				ExpressionAttributeValues: {
					':partition': {
						S: this.partition,
					},
					':sort': {
						S: sortKey,
					},
				},
				Limit: limit,
				ScanIndexForward: scanForward,
				ExclusiveStartKey: lastEvaluatedKey,
			})
			.promise();

		/**
		 * Gather any items that were returned
		 */
		if (results.Items) {
			results.Items.forEach((item) => {
				queryResult.records.push(this.#facet.out(item));
			});
		}

		/**
		 * Attach the cursor if needed
		 */
		if (results.LastEvaluatedKey) {
			queryResult.cursor = encodeCursor(results.LastEvaluatedKey);
		}

		return queryResult;
	}

	greaterThan() {}
}
