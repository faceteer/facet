import { decodeCursor, encodeCursor } from './cursor';
import {
	Facet,
	FacetIndex,
	PickValidator,
	WithoutReservedAttributes,
} from './facet';
import * as expressionBuilder from '@faceteer/expression-builder';
import { IndexKeyNameMap, PK, SK, Keys } from './keys';
import { buildProjectionExpression } from './projection';
import type { QueryInput } from '@aws-sdk/client-dynamodb';

/**
 * The T-level field names that a projected query auto-includes in its
 * response. For base-table queries this is the facet's `PK | SK`. For
 * index queries, under the library's documented assumption that GSIs
 * are created with `ProjectionType: ALL`, the base-table key fields
 * are still present in the index and are included alongside the
 * index's own `GSIPK | GSISK`.
 */
type AutoKeys<
	PK extends PropertyKey,
	SK extends PropertyKey,
	GSIPK extends PropertyKey,
	GSISK extends PropertyKey,
> = [GSIPK] extends [never] ? PK | SK : PK | SK | GSIPK | GSISK;

export interface PartitionQueryOptions<
	T extends WithoutReservedAttributes<T>,
	PK extends Keys<T>,
	SK extends Keys<T>,
	GSIPK extends Keys<T>,
	GSISK extends Keys<T>,
	PV extends PickValidator<T> | undefined = PickValidator<T> | undefined,
> {
	facet: Facet<T, PK, SK, PV>;
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

export interface QueryOptions<
	T,
	PK extends keyof T,
	SK extends keyof T,
	K extends keyof T = keyof T,
> {
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

	/**
	 * Restrict the read to a subset of attributes via a Dynamo DB
	 * `ProjectionExpression`. The facet's PK/SK fields (and the index's
	 * GSIPK/GSISK on index queries) are always included in the result,
	 * even if omitted from `select`.
	 *
	 * Requires `pickValidator` on the facet; the method overloads that
	 * accept `select` are gated off at the type level on facets without
	 * one.
	 */
	select?: readonly [K, ...K[]];
}

export class PartitionQuery<
	T extends WithoutReservedAttributes<T>,
	PK extends Keys<T>,
	SK extends Keys<T>,
	GSIPK extends Keys<T> = never,
	GSISK extends Keys<T> = never,
	PV extends PickValidator<T> | undefined = PickValidator<T> | undefined,
> {
	#facet: Facet<T, PK, SK, PV>;
	#index?: FacetIndex<T, PK, SK, GSIPK, GSISK>;
	#PK: string;
	#SK: string;
	#partition: string;
	#autoKeyFields: readonly (PK | SK | GSIPK | GSISK)[];

	constructor({
		facet,
		partitionIdentifier,
		index,
		shard,
	}: PartitionQueryOptions<T, PK, SK, GSIPK, GSISK, PV>) {
		this.#facet = facet;
		this.#index = index;

		if (this.#index) {
			const IndexKeys = IndexKeyNameMap[this.#index.indexName];
			this.#PK = IndexKeys.PK;
			this.#SK = IndexKeys.SK;
			this.#partition = this.#index.pk(partitionIdentifier, shard);
			this.#autoKeyFields = [
				...this.#facet.keyFields,
				...this.#index.keyFields,
			];
		} else {
			this.#PK = PK;
			this.#SK = SK;
			this.#partition = this.#facet.pk(partitionIdentifier, shard);
			this.#autoKeyFields = this.#facet.keyFields;
		}
	}

	/**
	 * Execute a prepared `QueryInput`, merging in any filter / projection /
	 * cursor from `options`, then mapping each returned item through the
	 * facet's projection-aware or full validator as appropriate.
	 */
	async #execute<K extends keyof T>(
		queryInput: QueryInput,
		options: QueryOptions<T, PK, SK, K>,
	): Promise<QueryResult<T> | QueryResult<Pick<T, K | AutoKeys<PK, SK, GSIPK, GSISK>>>> {
		const { dynamoDb } = this.#facet.connection;
		const { cursor, filter, select } = options;

		if (cursor) {
			queryInput.ExclusiveStartKey = decodeCursor(cursor);
		}

		if (filter) {
			const filterExpression = expressionBuilder.filter(filter);
			queryInput.FilterExpression = filterExpression.expression;
			Object.assign(queryInput.ExpressionAttributeNames!, filterExpression.names);
			Object.assign(
				queryInput.ExpressionAttributeValues!,
				filterExpression.values,
			);
		}

		const projectedKeys = select
			? ([...select, ...this.#autoKeyFields] as unknown as readonly (
					| K
					| AutoKeys<PK, SK, GSIPK, GSISK>
				)[])
			: undefined;

		if (projectedKeys) {
			const projection = buildProjectionExpression(projectedKeys);
			queryInput.ProjectionExpression = projection.expression;
			Object.assign(queryInput.ExpressionAttributeNames!, projection.names);
		}

		const results = await dynamoDb.query(queryInput);

		const projectedResult: QueryResult<
			Pick<T, K | AutoKeys<PK, SK, GSIPK, GSISK>>
		> = { records: [] };
		const fullResult: QueryResult<T> = { records: [] };

		if (results.Items) {
			if (projectedKeys) {
				for (const item of results.Items) {
					projectedResult.records.push(this.#facet.pick(item, projectedKeys));
				}
			} else {
				for (const item of results.Items) {
					fullResult.records.push(this.#facet.out(item));
				}
			}
		}

		if (results.LastEvaluatedKey) {
			const encoded = encodeCursor(results.LastEvaluatedKey);
			projectedResult.cursor = encoded;
			fullResult.cursor = encoded;
		}

		return projectedKeys ? projectedResult : fullResult;
	}

	/**
	 * Build the base `QueryInput` skeleton (table, index, key condition,
	 * placeholder values, limit, direction). Filter / projection / cursor
	 * are merged by {@link PartitionQuery.#execute}.
	 */
	#baseQueryInput(
		keyConditionExpression: string,
		sortValues: Record<string, { S: string }>,
		options: { limit?: number; scanForward?: boolean },
	): QueryInput {
		return {
			TableName: this.#facet.connection.tableName,
			IndexName: this.#index?.indexName,
			KeyConditionExpression: keyConditionExpression,
			ExpressionAttributeNames: {
				'#PK': this.#PK,
				'#SK': this.#SK,
			},
			ExpressionAttributeValues: {
				':partition': { S: this.#partition },
				...sortValues,
			},
			Limit: options.limit,
			ScanIndexForward: options.scanForward ?? true,
		};
	}

	/**
	 * Build the sort-key string from an object/string argument.
	 */
	#resolveSortKey(sort: Partial<T> | string, shard?: number): string {
		if (typeof sort === 'string') return sort;
		return this.#index
			? this.#index.sk(sort)
			: this.#facet.sk(sort, shard);
	}

	/**
	 * Get records from a partition by a comparison with the sort key.
	 * Private single-signature method; public overloads route through it.
	 */
	async #compareExec<K extends keyof T>(
		comparison: Comparison,
		sort: Partial<T> | string,
		options: QueryOptions<T, PK, SK, K>,
	) {
		const queryInput = this.#baseQueryInput(
			`#PK = :partition AND #SK ${comparison} :sort`,
			{ ':sort': { S: this.#resolveSortKey(sort, options.shard) } },
			options,
		);
		return this.#execute(queryInput, options);
	}

	/**
	 * begins_with execution. Private single-signature method; public
	 * `beginsWith`, `list`, and `first` route through it.
	 */
	async #beginsWithExec<K extends keyof T>(
		sort: Partial<T> | string,
		options: QueryOptions<T, PK, SK, K>,
	) {
		const queryInput = this.#baseQueryInput(
			'#PK = :partition AND begins_with(#SK, :sort)',
			{ ':sort': { S: this.#resolveSortKey(sort, options.shard) } },
			options,
		);
		return this.#execute(queryInput, options);
	}

	/**
	 * BETWEEN execution. Private single-signature method; public `between`
	 * routes through it.
	 */
	async #betweenExec<K extends keyof T>(
		start: Partial<T> | string,
		end: Partial<T> | string,
		options: QueryOptions<T, PK, SK, K>,
	) {
		const queryInput = this.#baseQueryInput(
			'#PK = :partition AND #SK BETWEEN :start AND :end',
			{
				':start': { S: this.#resolveSortKey(start, options.shard) },
				':end': { S: this.#resolveSortKey(end, options.shard) },
			},
			options,
		);
		return this.#execute(queryInput, options);
	}

	/**
	 * Fetch records whose sort key equals the given value.
	 *
	 * @param sort - Object of sort-key field values used to build the
	 * composite key, or a raw string if you need to bypass key construction.
	 * @param options - Optional {@link QueryOptions} — filter, limit,
	 * cursor, scanForward, shard.
	 * @returns {@link QueryResult} with `records` and an optional `cursor`
	 * if more pages are available.
	 *
	 * @example
	 * ```ts
	 * const { records } = await PostFacet.GSIStatusSendAt
	 *   .query({ postStatus: 'queued' })
	 *   .equals({ sendAt: new Date('2024-01-01') });
	 * ```
	 *
	 * @example
	 * Projected, requires `pickValidator` on the facet:
	 * ```ts
	 * const { records } = await PostFacet.GSIStatusSendAt
	 *   .query({ postStatus: 'queued' })
	 *   .equals({ sendAt: new Date('2024-01-01') }, { select: ['postTitle'] });
	 * ```
	 */
	equals(
		sort: Partial<Pick<T, GSISK>> | string,
		options?: QueryOptions<T, PK, SK> & { select?: never },
	): Promise<QueryResult<T>>;
	equals<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> & { select: readonly [K, ...K[]] },
	): Promise<QueryResult<Pick<T, K | AutoKeys<PK, SK, GSIPK, GSISK>>>>;
	equals<K extends keyof T>(
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> = {},
	) {
		return this.#compareExec(Comparison.Equals, sort as Partial<T>, options);
	}

	/**
	 * Fetch records whose sort key is strictly greater than the given value.
	 *
	 * @remarks
	 * Accepts `select` on facets configured with a `pickValidator`; see
	 * {@link PartitionQuery.equals} for a projected example.
	 *
	 * @param sort - Object of sort-key field values, or a raw string.
	 * @param options - Optional {@link QueryOptions}.
	 * @returns {@link QueryResult}.
	 */
	greaterThan(
		sort: Partial<Pick<T, GSISK>> | string,
		options?: QueryOptions<T, PK, SK> & { select?: never },
	): Promise<QueryResult<T>>;
	greaterThan<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> & { select: readonly [K, ...K[]] },
	): Promise<QueryResult<Pick<T, K | AutoKeys<PK, SK, GSIPK, GSISK>>>>;
	greaterThan<K extends keyof T>(
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> = {},
	) {
		return this.#compareExec(Comparison.Greater, sort as Partial<T>, options);
	}

	/**
	 * Fetch records whose sort key is greater than or equal to the given value.
	 *
	 * @remarks
	 * Accepts `select` on facets configured with a `pickValidator`; see
	 * {@link PartitionQuery.equals} for a projected example.
	 *
	 * @param sort - Object of sort-key field values, or a raw string.
	 * @param options - Optional {@link QueryOptions}.
	 * @returns {@link QueryResult}.
	 */
	greaterThanOrEqual(
		sort: Partial<Pick<T, GSISK>> | string,
		options?: QueryOptions<T, PK, SK> & { select?: never },
	): Promise<QueryResult<T>>;
	greaterThanOrEqual<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> & { select: readonly [K, ...K[]] },
	): Promise<QueryResult<Pick<T, K | AutoKeys<PK, SK, GSIPK, GSISK>>>>;
	greaterThanOrEqual<K extends keyof T>(
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> = {},
	) {
		return this.#compareExec(
			Comparison.GreaterOrEqual,
			sort as Partial<T>,
			options,
		);
	}

	/**
	 * Fetch records whose sort key is strictly less than the given value.
	 *
	 * @remarks
	 * Accepts `select` on facets configured with a `pickValidator`; see
	 * {@link PartitionQuery.equals} for a projected example.
	 *
	 * @param sort - Object of sort-key field values, or a raw string.
	 * @param options - Optional {@link QueryOptions}.
	 * @returns {@link QueryResult}.
	 */
	lessThan(
		sort: Partial<Pick<T, GSISK>> | string,
		options?: QueryOptions<T, PK, SK> & { select?: never },
	): Promise<QueryResult<T>>;
	lessThan<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> & { select: readonly [K, ...K[]] },
	): Promise<QueryResult<Pick<T, K | AutoKeys<PK, SK, GSIPK, GSISK>>>>;
	lessThan<K extends keyof T>(
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> = {},
	) {
		return this.#compareExec(Comparison.Less, sort as Partial<T>, options);
	}

	/**
	 * Fetch records whose sort key is less than or equal to the given value.
	 *
	 * @remarks
	 * Accepts `select` on facets configured with a `pickValidator`; see
	 * {@link PartitionQuery.equals} for a projected example.
	 *
	 * @param sort - Object of sort-key field values, or a raw string.
	 * @param options - Optional {@link QueryOptions}.
	 * @returns {@link QueryResult}.
	 */
	lessThanOrEqual(
		sort: Partial<Pick<T, GSISK>> | string,
		options?: QueryOptions<T, PK, SK> & { select?: never },
	): Promise<QueryResult<T>>;
	lessThanOrEqual<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> & { select: readonly [K, ...K[]] },
	): Promise<QueryResult<Pick<T, K | AutoKeys<PK, SK, GSIPK, GSISK>>>>;
	lessThanOrEqual<K extends keyof T>(
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> = {},
	) {
		return this.#compareExec(
			Comparison.LessOrEqual,
			sort as Partial<T>,
			options,
		);
	}

	/**
	 * List every record in the partition (sort key starts with the facet's
	 * sort-key prefix).
	 *
	 * Equivalent to `beginsWith({})` — it uses the prefix alone, so every
	 * record this facet writes into the partition matches.
	 *
	 * @param options - Optional {@link QueryOptions} — filter, limit, cursor.
	 * @returns {@link QueryResult}.
	 *
	 * @example
	 * ```ts
	 * const { records, cursor } = await PostFacet
	 *   .query({ pageId: 'p1' })
	 *   .list({ limit: 50 });
	 * ```
	 *
	 * @example
	 * Projected, requires `pickValidator` on the facet:
	 * ```ts
	 * const { records } = await PostFacet
	 *   .query({ pageId: 'p1' })
	 *   .list({ select: ['postTitle'] });
	 * ```
	 */
	list(
		options?: QueryOptions<T, PK, SK> & { select?: never },
	): Promise<QueryResult<T>>;
	list<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		options: QueryOptions<T, PK, SK, K> & { select: readonly [K, ...K[]] },
	): Promise<QueryResult<Pick<T, K | AutoKeys<PK, SK, GSIPK, GSISK>>>>;
	list<K extends keyof T>(options: QueryOptions<T, PK, SK, K> = {}) {
		return this.#beginsWithExec({}, options);
	}

	/**
	 * Fetch the first record in the partition (or `null` if empty).
	 *
	 * Internally runs `list({ limit: 1 })` and unwraps the first row.
	 * Useful for "does any record exist for this partition?" checks and
	 * for cheap "earliest/latest" lookups when combined with `scanForward`.
	 *
	 * @param options - Subset of {@link QueryOptions}; `cursor` and
	 * `limit` are not meaningful here and are omitted.
	 * @returns The first record in the partition or `null`.
	 *
	 * @example
	 * ```ts
	 * // Most recent post (sort descending, take first)
	 * const latest = await PostFacet.GSIStatusSendAt
	 *   .query({ postStatus: 'sent' })
	 *   .first({ scanForward: false });
	 * ```
	 *
	 * @example
	 * Projected, requires `pickValidator` on the facet:
	 * ```ts
	 * const head = await PostFacet
	 *   .query({ pageId: 'p1' })
	 *   .first({ select: ['postTitle'] });
	 * ```
	 */
	first(
		options?: Omit<QueryOptions<T, PK, SK>, 'cursor' | 'limit'> & {
			select?: never;
		},
	): Promise<T | null>;
	first<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		options: Omit<QueryOptions<T, PK, SK, K>, 'cursor' | 'limit'> & {
			select: readonly [K, ...K[]];
		},
	): Promise<Pick<T, K | AutoKeys<PK, SK, GSIPK, GSISK>> | null>;
	async first<K extends keyof T>({
		filter,
		scanForward,
		shard,
		select,
	}: Omit<QueryOptions<T, PK, SK, K>, 'cursor' | 'limit'> = {}) {
		const listResults = await this.#beginsWithExec(
			{},
			{ filter, limit: 1, scanForward, shard, select },
		);
		const [firstRecord] = listResults.records;
		return firstRecord ?? null;
	}

	/**
	 * Fetch records whose sort key begins with the given value.
	 *
	 * Passing a partial object like `{ status: 'draft' }` only populates
	 * the sort-key fields you provide — the remaining fields are omitted
	 * from the composite key, so the generated prefix matches every
	 * record that shares the leading portion.
	 *
	 * @param sort - Object of sort-key field values, or a raw string
	 * prefix if you need to bypass key construction.
	 * @param options - Optional {@link QueryOptions}.
	 * @returns {@link QueryResult}.
	 *
	 * @example
	 * ```ts
	 * // Every post whose postTitle starts with "aa"
	 * const aa = await PostFacet.GSIPostByTitle
	 *   .query({ pageId: 'p1' })
	 *   .beginsWith({ postTitle: 'aa' });
	 * ```
	 *
	 * @example
	 * Projected, requires `pickValidator` on the facet:
	 * ```ts
	 * const aa = await PostFacet.GSIPostByTitle
	 *   .query({ pageId: 'p1' })
	 *   .beginsWith({ postTitle: 'aa' }, { select: ['postStatus'] });
	 * ```
	 */
	beginsWith(
		sort: Partial<Pick<T, GSISK>> | string,
		options?: QueryOptions<T, PK, SK> & { select?: never },
	): Promise<QueryResult<T>>;
	beginsWith<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> & { select: readonly [K, ...K[]] },
	): Promise<QueryResult<Pick<T, K | AutoKeys<PK, SK, GSIPK, GSISK>>>>;
	beginsWith<K extends keyof T>(
		sort: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> = {},
	) {
		return this.#beginsWithExec(sort as Partial<T>, options);
	}

	/**
	 * Fetch records whose sort key is between `start` and `end`, inclusive
	 * on both ends.
	 *
	 * Composite sort keys are compared lexicographically in their
	 * string-joined form. For date-valued sort keys that ISO-encode, a
	 * calendar range "between Jan 1 and Feb 28" works as expected.
	 *
	 * @param start - Object of sort-key field values for the lower bound,
	 * or a raw string.
	 * @param end - Object of sort-key field values for the upper bound,
	 * or a raw string.
	 * @param options - Optional {@link QueryOptions}.
	 * @returns {@link QueryResult}.
	 *
	 * @example
	 * ```ts
	 * const range = await PostFacet.GSIPostByTitle
	 *   .query({ pageId: 'p1' })
	 *   .between({ postTitle: 'ab' }, { postTitle: 'ae' });
	 * ```
	 *
	 * @example
	 * Projected, requires `pickValidator` on the facet:
	 * ```ts
	 * const range = await PostFacet.GSIPostByTitle
	 *   .query({ pageId: 'p1' })
	 *   .between(
	 *     { postTitle: 'ab' },
	 *     { postTitle: 'ae' },
	 *     { select: ['postStatus'] },
	 *   );
	 * ```
	 */
	between(
		start: Partial<Pick<T, GSISK>> | string,
		end: Partial<Pick<T, GSISK>> | string,
		options?: QueryOptions<T, PK, SK> & { select?: never },
	): Promise<QueryResult<T>>;
	between<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		start: Partial<Pick<T, GSISK>> | string,
		end: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> & { select: readonly [K, ...K[]] },
	): Promise<QueryResult<Pick<T, K | AutoKeys<PK, SK, GSIPK, GSISK>>>>;
	between<K extends keyof T>(
		start: Partial<Pick<T, GSISK>> | string,
		end: Partial<Pick<T, GSISK>> | string,
		options: QueryOptions<T, PK, SK, K> = {},
	) {
		return this.#betweenExec(
			start as Partial<T>,
			end as Partial<T>,
			options,
		);
	}
}
