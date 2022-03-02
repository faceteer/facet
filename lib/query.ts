import { decodeCursor, encodeCursor } from './cursor';
import { Facet, FacetIndex } from './facet';
import * as expressionBuilder from '@faceteer/expression-builder';
import { IndexKeyNameMap, PK, SK } from './keys';
import type { QueryInput } from '@aws-sdk/client-dynamodb';

export interface PartitionQueryOptions<
	T,
	PK extends keyof T,
	SK extends keyof T,
	GSIPK extends keyof T,
	GSISK extends keyof T,
> {
	facet: Facet<T, PK, SK>;
	partitionIdentifier: Partial<T>;
	index?: FacetIndex<T, PK, SK, GSIPK, GSISK>;
	shard?: number;
}

enum Comparison {
	Equals = '=',
	Greater = '>',
	GreaterOrEqual = '>=',
	Less = '<',
	LessOrEqual = '<=',
}

export interface QueryResult<T> {
	cursor?: string;
	records: T[];
}

export interface QueryOptions<T, PK extends keyof T, SK extends keyof T> {
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

	filter?: expressionBuilder.FilterConditionExpression<Omit<T, PK | SK>>;
}

export class PartitionQuery<
	T,
	PK extends keyof T,
	SK extends keyof T,
	GSIPK extends keyof T = never,
	GSISK extends keyof T = never,
> {
	#facet: Facet<T, PK, SK>;
	#index?: FacetIndex<T, PK, SK, GSIPK, GSISK>;
	#PK: string;
	#SK: string;
	#partition: string;

	constructor({
		facet,
		partitionIdentifier,
		index,
		shard,
	}: PartitionQueryOptions<T, PK, SK, GSIPK, GSISK>) {
		this.#facet = facet;
		this.#index = index;

		if (this.#index) {
			const IndexKeys = IndexKeyNameMap[this.#index.indexName];
			this.#PK = IndexKeys.PK;
			this.#SK = IndexKeys.SK;
			this.#partition = this.#index.pk(partitionIdentifier, shard);
		} else {
			this.#PK = PK;
			this.#SK = SK;
			this.#partition = this.#facet.pk(partitionIdentifier, shard);
		}
	}

	/**
	 * Get records from a partition by a comparison with the sort key.
	 */
	private async compare(
		comparison: Comparison,
		sort: Partial<T> | string,
		{ cursor, limit, scanForward = true, shard, filter }: QueryOptions<T>,
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

		const queryInput: QueryInput = {
			TableName: tableName,
			IndexName: this.#index?.indexName,
			KeyConditionExpression: `#PK = :partition AND #SK ${comparison} :sort`,
			ExpressionAttributeNames: {
				'#PK': this.#PK,
				'#SK': this.#SK,
			},
			ExpressionAttributeValues: {
				':partition': {
					S: this.#partition,
				},
				':sort': {
					S: sortKey,
				},
			},
			Limit: limit,
			ScanIndexForward: scanForward,
		};

		if (cursor) {
			queryInput.ExclusiveStartKey = decodeCursor(cursor);
		}

		if (filter) {
			const filterExpression = expressionBuilder.filter(filter);
			queryInput.FilterExpression = filterExpression.expression;
			Object.assign(
				queryInput.ExpressionAttributeNames,
				filterExpression.names,
			);
			Object.assign(
				queryInput.ExpressionAttributeValues,
				filterExpression.values,
			);
		}

		const results = await dynamoDb.query(queryInput);

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

	/**
	 * Query for records where the sort key equals
	 * the given sort key values
	 * @param sort
	 * @param options
	 */
	equals(
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK> = {},
	) {
		return this.compare(Comparison.Equals, sort as Partial<T>, options);
	}

	/**
	 * Query for records where the sort key is greater than
	 * the given sort key values
	 * @param sort
	 * @param options
	 */
	greaterThan(
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK> = {},
	) {
		return this.compare(Comparison.Greater, sort as Partial<T>, options);
	}

	/**
	 * Query for records where the sort key is greater than or equal to
	 * the given sort key values
	 * @param sort
	 * @param options
	 */
	greaterThanOrEqual(
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T> = {},
	) {
		return this.compare(Comparison.GreaterOrEqual, sort as Partial<T>, options);
	}

	/**
	 * Query for records where the sort key is less than
	 * the given sort key values
	 * @param sort
	 * @param options
	 */
	lessThan(
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK> = {},
	) {
		return this.compare(Comparison.Less, sort as Partial<T>, options);
	}

	/**
	 * Query for records where the sort key is less than or equal to
	 * the given sort key values
	 * @param sort
	 * @param options
	 */
	lessThanOrEqual(
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK> = {},
	) {
		return this.compare(Comparison.LessOrEqual, sort as Partial<T>, options);
	}

	/**
	 * Query for all records where the sort key
	 * starts with the facet prefix
	 *
	 * @param options
	 */
	list(options: QueryOptions<T, PK, SK> = {}) {
		return this.beginsWith({}, options);
	}

	/**
	 * This is equivalent to running `list()` and picking
	 * the first result.
	 *
	 * If no results are found this will return `null`
	 * @param options
	 */
	async first({
		filter,
		scanForward,
		shard,
	}: Omit<
		QueryOptions<T, PK, SK>,
		'cursor' | 'limit'
	> = {}): Promise<T | null> {
		const listResults = await this.list({
			filter,
			limit: 1,
			scanForward,
			shard,
		});

		const [firstRecord] = listResults.records;
		if (firstRecord) {
			return firstRecord;
		}
		return null;
	}

	/**
	 * Query for records where the sort key begins with the given sort key values
	 *
	 * @param sort
	 * @param options
	 */
	async beginsWith(
		sort: Partial<Pick<T, GSISK>> | string,
		{
			cursor,
			limit,
			scanForward = true,
			shard,
			filter,
		}: QueryOptions<T, PK, SK> = {},
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
				? this.#index.sk(sort as Partial<T>)
				: this.#facet.sk(sort as Partial<T>, shard);
		}

		const queryInput: QueryInput = {
			TableName: tableName,
			IndexName: this.#index?.indexName,
			KeyConditionExpression: '#PK = :partition AND begins_with(#SK, :sort)',
			ExpressionAttributeNames: {
				'#PK': this.#PK,
				'#SK': this.#SK,
			},
			ExpressionAttributeValues: {
				':partition': {
					S: this.#partition,
				},
				':sort': {
					S: sortKey,
				},
			},
			Limit: limit,
			ScanIndexForward: scanForward,
		};

		if (cursor) {
			queryInput.ExclusiveStartKey = decodeCursor(cursor);
		}

		if (filter) {
			const filterExpression = expressionBuilder.filter(filter);
			queryInput.FilterExpression = filterExpression.expression;
			Object.assign(
				queryInput.ExpressionAttributeNames,
				filterExpression.names,
			);
			Object.assign(
				queryInput.ExpressionAttributeValues,
				filterExpression.values,
			);
		}

		const results = await dynamoDb.query(queryInput);

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

	/**
	 * Query for records that are greater than or equal to the starting
	 * sort key, and less than or equal to the ending sort key
	 *
	 * @param start
	 * @param end
	 * @param options
	 */
	async between(
		start: Partial<Pick<T, GSISK>> | string,
		end: Partial<Pick<T, GSISK>> | string,
		{ cursor, limit, scanForward = true, shard, filter }: QueryOptions<T> = {},
	) {
		const { dynamoDb, tableName } = this.#facet.connection;

		const queryResult: QueryResult<T> = {
			records: [],
		};

		let startKey: string;
		let endKey: string;

		/**
		 * If we were given a string we'll use it, otherwise we'll
		 * create the sort key using the getKey function of the facet
		 */
		if (typeof start === 'string') {
			startKey = start;
		} else {
			startKey = this.#index
				? this.#index.sk(start as Partial<T>)
				: this.#facet.sk(start as Partial<T>, shard);
		}

		/**
		 * If we were given a string we'll use it, otherwise we'll
		 * create the sort key using the getKey function of the facet
		 */
		if (typeof end === 'string') {
			endKey = end;
		} else {
			endKey = this.#index
				? this.#index.sk(end as Partial<T>)
				: this.#facet.sk(end as Partial<T>, shard);
		}

		const queryInput: QueryInput = {
			TableName: tableName,
			IndexName: this.#index?.indexName,
			KeyConditionExpression:
				'#PK = :partition AND #SK BETWEEN :start AND :end',
			ExpressionAttributeNames: {
				'#PK': this.#PK,
				'#SK': this.#SK,
			},
			ExpressionAttributeValues: {
				':partition': {
					S: this.#partition,
				},
				':start': {
					S: startKey,
				},
				':end': {
					S: endKey,
				},
			},
			Limit: limit,
			ScanIndexForward: scanForward,
		};

		if (cursor) {
			queryInput.ExclusiveStartKey = decodeCursor(cursor);
		}

		if (filter) {
			const filterExpression = expressionBuilder.filter(filter);
			queryInput.FilterExpression = filterExpression.expression;
			Object.assign(
				queryInput.ExpressionAttributeNames,
				filterExpression.names,
			);
			Object.assign(
				queryInput.ExpressionAttributeValues,
				filterExpression.values,
			);
		}

		const results = await dynamoDb.query(queryInput);

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
}
