import { Converter } from '@faceteer/converter';
import type { ConverterOptions } from '@faceteer/converter/converter-options';
import type { DynamoDB, AttributeValue } from '@aws-sdk/client-dynamodb';
import {
	deleteItems,
	DeleteOptions,
	DeleteResponse,
	deleteSingleItem,
} from './delete';
import { GetOptions, getBatchItems, getSingleItem } from './get';
import {
	buildKey,
	Index,
	IndexKeyNameMap,
	KeyConfiguration,
	PK,
	SK,
	Keys,
} from './keys';
import {
	putItems,
	PutOptions,
	PutResponse,
	putSingleItem,
	PutSingleItemResponse,
} from './put';
import { PartitionQuery } from './query';

export interface AttributeMap {
	[key: string]: AttributeValue;
}

/**
 * A `Validator` is a function that is used by Faceteer whenever
 * it reads records from Dynamo DB.
 *
 * This function should return a valid object of type T if the
 * input is valid, or it should throw if the input is invalid
 *
 * ## Example using AJV
 * ```ts
 * import AJV, { JSONSchemaType } from "ajv";
 *
 * export interface Team {
 *   teamId: string;
 *   teamName: string;
 *   dateCreated: Date;
 *   dateDeleted?: Date;
 * }
 *
 * const schema: JSONSchemaType<Team> = {
 *   type: "object",
 *   additionalProperties: false,
 *   properties: {
 *     teamId: { type: "string" },
 *     teamName: { type: "string" },
 *     dateCreated: { type: "object", format: "date-time" },
 *     dateDeleted: { type: "object", format: "date-time", nullable: true },
 *   },
 *   required: ["teamId", "teamName", "dateCreated"],
 * };
 * const validateTeam = ajv.compile(schema);
 *
 * export function teamValidator(input: unknown): Team {
 *   if (validateTeam(input)) {
 *     return input;
 *   }
 *   throw validateTeam.errors[0];
 * }
 * ```
 */
export type Validator<T> = (input: unknown) => T;

/**
 * A `PickValidator` is a factory that, given a subset of keys, returns a
 * {@link Validator} for `Pick<T, K>`. It is used on projected reads
 * (`select`) where only a subset of the record's attributes is returned and
 * the full-record `Validator` would reject the shape.
 *
 * The factory shape mirrors how real validator libraries split work: the
 * outer function is for deriving a sub-schema (expensive, run once per
 * key-set); the inner function is the per-record parse (cheap, run for
 * every row).
 *
 * ## Example using Zod
 *
 * Zod exposes `.pick()` directly, so derivation is a one-liner:
 *
 * ```ts
 * import { z } from 'zod';
 *
 * const teamSchema = z.object({
 *   teamId: z.string(),
 *   teamName: z.string(),
 *   dateCreated: z.date(),
 *   dateDeleted: z.date().optional(),
 * });
 *
 * export const teamPickValidator: PickValidator<Team> = (keys) => {
 *   const mask: { [K in keyof Team]?: true } = {};
 *   for (const k of keys) mask[k as keyof Team] = true;
 *   const picked = teamSchema.pick(mask);
 *   return (input) => picked.parse(input) as Pick<Team, (typeof keys)[number]>;
 * };
 * ```
 *
 * ## Example using AJV
 *
 * AJV's `compile()` is expensive, so derive a sub-schema and compile it
 * once per key-set. The cache lives in module scope, keyed by a canonical
 * signature of the key tuple, so the same projection only compiles once
 * across the whole process:
 *
 * ```ts
 * import Ajv, { ValidateFunction } from 'ajv';
 *
 * // Pre-built dictionary of per-field JSON schemas and the full required list.
 * const teamFieldSchemas = {
 *   teamId: { type: 'string' },
 *   teamName: { type: 'string' },
 *   dateCreated: { type: 'object', format: 'date-time' },
 *   dateDeleted: { type: 'object', format: 'date-time', nullable: true },
 * } as const;
 * const teamRequired: Array<keyof Team> = ['teamId', 'teamName', 'dateCreated'];
 *
 * const ajv = new Ajv();
 * const cache = new Map<string, ValidateFunction>();
 *
 * export const teamPickValidator: PickValidator<Team> = (keys) => {
 *   const signature = [...keys].sort().join(',');
 *   let validate = cache.get(signature);
 *   if (!validate) {
 *     validate = ajv.compile({
 *       type: 'object',
 *       additionalProperties: false,
 *       properties: Object.fromEntries(
 *         keys.map((k) => [k as string, teamFieldSchemas[k]]),
 *       ),
 *       required: (keys as readonly (keyof Team)[]).filter((k) =>
 *         teamRequired.includes(k),
 *       ) as string[],
 *     });
 *     cache.set(signature, validate);
 *   }
 *   return (input) => {
 *     if (validate!(input)) return input as never;
 *     throw new Error(ajv.errorsText(validate!.errors));
 *   };
 * };
 * ```
 */
export type PickValidator<T> = <K extends keyof T>(
	keys: readonly K[],
) => Validator<Pick<T, K>>;

/**
 * Attribute names that Facet writes synthetically on every record.
 * A model field with any of these names would silently collide with
 * the synthetic value — see {@link Facet.in}.
 */
export type ReservedAttributeName =
	| 'PK'
	| 'SK'
	| 'facet'
	| 'ttl'
	| `GSI${number}PK`
	| `GSI${number}SK`;

/**
 * Constraint used on `T` to forbid reserved attribute names at the type
 * level. Mapping over `keyof T` (rather than over the reserved-name union
 * directly) is important: a mapped type over a template-literal union
 * produces an index signature that no concrete type structurally matches,
 * which would reject every `T`. Mapping over `keyof T` instead marks any
 * colliding field as `never`, so a `T` that declares a reserved name has
 * at least one impossible field and fails the bound.
 *
 * Use as `T extends WithoutReservedAttributes<T>`.
 */
export type WithoutReservedAttributes<T> = {
	[K in keyof T]: K extends ReservedAttributeName ? never : T[K];
};

export type FacetIndexKeys<
	T extends WithoutReservedAttributes<T>,
	PK extends Keys<T>,
	SK extends Keys<T>,
	GSIPK extends Keys<T>,
	GSISK extends Keys<T>,
	I extends Index,
	A extends string = never,
	PV extends PickValidator<T> | undefined = PickValidator<T> | undefined,
> = Record<I, FacetIndex<T, PK, SK, GSIPK, GSISK, PV>> &
	Record<A, FacetIndex<T, PK, SK, GSIPK, GSISK, PV>>;

export type FacetWithIndex<F, K> = F & K;

const RESERVED_ATTRIBUTE_SET: ReadonlySet<string> = new Set([
	'PK',
	'SK',
	'facet',
	'ttl',
]);
const RESERVED_GSI_PATTERN = /^GSI\d+(?:PK|SK)$/;

function assertNoReservedAttributes(model: object): void {
	for (const key of Object.keys(model)) {
		if (RESERVED_ATTRIBUTE_SET.has(key) || RESERVED_GSI_PATTERN.test(key)) {
			throw new Error(
				`Model contains reserved attribute "${key}"; Facet uses this name for a synthetic key.`,
			);
		}
	}
}

/**
 * # Facet
 * A Facet is a utility that wraps Dynamo DB commands
 * along with calculating properties that are used for
 * indexing records in a Dynamo DB single-table design.
 *
 * To construct a facet you must specify how a partition key (`PK`)
 * and a sort key (`SK`) should be constructed for the facet.
 *
 * The `PK` and `SK` are then generated using a specified prefix for the
 * key along with any values of the properties specified.
 *
 * A {@link Validator} that can validate results from the database is also required
 * to make sure that invalid records don't make their way into the application.
 *
 * ## Example
 *
 * ```ts
 *const PostFacet = new Facet({
 *  name: Name.Post,
 *  validator: postValidator,
 *  PK: {
 *    keys: ['pageId'],
 *    prefix: Prefix.Page,
 *  },
 *  SK: {
 *    keys: ['postId'],
 *    prefix: Prefix.Post,
 *  },
 *  connection: {
 *    dynamoDb: ddb,
 *    tableName: tableName,
 *  },
 *  ttl: 'deleteAt',
 *})
 *  .addIndex({
 *    index: Index.GSI1,
 *    PK: {
 *      keys: ['postStatus'],
 *      shard: { count: 4, keys: ['postId'] },
 *      prefix: Prefix.Status,
 *    },
 *    SK: {
 *      keys: ['sendAt'],
 *      prefix: Prefix.Status,
 *    },
 *    alias: 'byStatusSendAt',
 *  })
 *  .addIndex({
 *    index: Index.GSI2,
 *    PK: {
 *      keys: ['pageId', 'postStatus'],
 *      prefix: Prefix.Page,
 *    },
 *    SK: {
 *      keys: ['postId'],
 *      prefix: Prefix.Post,
 *    },
 *    alias: 'GSIPagePostStatus',
 *  });
 * ```
 */
class FacetImpl<
	T extends WithoutReservedAttributes<T>,
	PK extends Keys<T> = never,
	SK extends Keys<T> = never,
	PV extends PickValidator<T> | undefined = PickValidator<T> | undefined,
> {
	#PK: KeyConfiguration<T, PK>;
	#SK: KeyConfiguration<T, SK>;
	#validator: Validator<T>;
	#pickValidator?: PickValidator<T>;
	#dateFormat?: ConverterOptions['dateFormat'];
	#convertEmptyValues?: ConverterOptions['convertEmptyValues'];
	#validateInput: boolean;
	/**
	 * Indexes that have been configured for the facet
	 */
	#indexes: Map<Index, FacetIndex<T, PK, SK, Keys<T>, Keys<T>, PV>> = new Map();

	/**
	 * The delimiter used when construction partition and sort keys
	 */
	readonly delimiter: string;
	/**
	 * The property thats used for the TTL column in Dynamo DB
	 */
	readonly ttl?: Keys<T>;
	/**
	 * The configured connection to Dynamo DB
	 */
	readonly connection: {
		dynamoDb: DynamoDB;
		tableName: string;
	};
	/**
	 * The name of the facet that is stored under `facet` for every record.
	 */
	readonly name: string;

	constructor({
		name,
		PK,
		SK,
		connection,
		validator,
		pickValidator,
		convertEmptyValues,
		dateFormat,
		delimiter = '_',
		ttl,
		validateInput = false,
	}: FacetOptions<T, PK, SK, PV>) {
		this.name = name;
		this.#PK = PK;
		this.#SK = SK;
		this.#validator = validator;
		this.#pickValidator = pickValidator;
		this.#convertEmptyValues = convertEmptyValues;
		this.#dateFormat = dateFormat;
		this.delimiter = delimiter;
		this.ttl = ttl;
		this.#validateInput = validateInput;
		this.connection = connection;
	}

	/**
	 * Construct the partition key
	 */
	pk(model: Partial<T>, shard?: number) {
		return buildKey(this.#PK, model, this.delimiter, shard);
	}

	/**
	 * Construct the sort key
	 */
	sk(model: Partial<T>, shard?: number) {
		return buildKey(this.#SK, model, this.delimiter, shard);
	}

	/**
	 * The T-level field names that compose this facet's partition and sort
	 * keys. Projected reads auto-include these so callers can always round
	 * a result back into a `get`/`delete`/`put`.
	 */
	get keyFields(): readonly (PK | SK)[] {
		return [...this.#PK.keys, ...this.#SK.keys];
	}

	/**
	 * Convert a model to a record that can be
	 * stored directly in DynamoDB
	 */
	in(model: T): AttributeMap {
		if (this.#validateInput) {
			model = this.#validator(model);
		}
		assertNoReservedAttributes(model as object);
		const attributes: Partial<T> = { ...model };

		/**
		 * Create the partition keys and the sort keys
		 */
		const facetKeys: Record<string, string> = {};
		facetKeys[PK] = this.pk(model);
		facetKeys[SK] = this.sk(model);

		/**
		 * Create any Global Secondary Index partition and
		 * sort keys
		 */
		this.#indexes.forEach((facetIndex, indexName) => {
			const indexKeyNames = IndexKeyNameMap[indexName];
			facetKeys[indexKeyNames.PK] = facetIndex.pk(model);
			facetKeys[indexKeyNames.SK] = facetIndex.sk(model);
		});

		/**
		 * DynamoDB's TTL reaper only recognises epoch-seconds `N`
		 * attributes, so normalise Date and numeric-string inputs
		 * before marshalling.
		 */
		let ttlAttribute: number | undefined;
		if (this.ttl) {
			const raw: unknown = model[this.ttl];
			if (raw instanceof Date) {
				ttlAttribute = Math.floor(raw.getTime() / 1000);
			} else if (typeof raw === 'number') {
				ttlAttribute = raw;
			} else if (typeof raw === 'string') {
				ttlAttribute = parseInt(raw, 10);
			}
			if (ttlAttribute !== undefined && Number.isNaN(ttlAttribute)) {
				ttlAttribute = undefined;
			}
		}

		const dynamoDbRecord = {
			...attributes,
			...facetKeys,
			facet: this.name,
			ttl: ttlAttribute,
		};

		return Converter.marshall(dynamoDbRecord, {
			wrapNumbers: true,
			dateFormat: this.#dateFormat,
			convertEmptyValues: this.#convertEmptyValues,
		}) as AttributeMap;
	}

	/**
	 * Convert and validate a projected Dynamo DB record. Intended as a
	 * library-internal helper called by the read path when `select` is set.
	 *
	 * The user-facing compile-time gate for "is projection available on this
	 * facet?" lives on `Facet.get`, not here — `pick` is reached internally
	 * with the class default `PV = PickValidator<T> | undefined`, so it
	 * cannot carry the same `this:` constraint. The defensive throw below
	 * is the safety net for paths that bypass the `get` gate (cross-boundary
	 * upcasts, direct `.pick()` calls), so misuse surfaces as a descriptive
	 * error instead of an opaque `TypeError`.
	 */
	pick<K extends keyof T>(
		record: AttributeMap,
		keys: readonly K[],
	): Pick<T, K> {
		if (!this.#pickValidator) {
			throw new Error(
				`Facet "${this.name}" has no pickValidator; projected reads require one. This call bypassed the compile-time gate on Facet.get — configure a pickValidator in the facet options.`,
			);
		}
		const unmarshalled: unknown = Converter.unmarshall(record);
		return this.#pickValidator(keys)(unmarshalled);
	}

	/**
	 * Convert and validate a dynamo DB record
	 */
	out(record: AttributeMap): T {
		const recordToValidate: Record<string, unknown> =
			Converter.unmarshall(record);

		/**
		 * Delete any constructed keys from the model before
		 * validating and returning
		 */
		delete recordToValidate['facet'];
		delete recordToValidate['PK'];
		delete recordToValidate['SK'];
		for (const index of this.#indexes.keys()) {
			const indexKeyNames = IndexKeyNameMap[index];
			delete recordToValidate[indexKeyNames.PK];
			delete recordToValidate[indexKeyNames.SK];
		}
		if (this.ttl) {
			delete recordToValidate['ttl'];
		}

		return this.#validator(recordToValidate);
	}

	/**
	 * Fetch a single record by its exact partition and sort key.
	 *
	 * The `query` object must contain the model fields that compose this
	 * facet's `PK` and `SK`. Other fields may be supplied but are ignored —
	 * only the key fields are used to build the Dynamo DB `GetItem` request.
	 * The result is run through the configured `validator` before being
	 * returned; if the record is missing the promise resolves to `null`.
	 *
	 * @param query - Object providing the PK and SK field values.
	 * @returns The record if found, or `null`.
	 *
	 * @example
	 * ```ts
	 * const post = await PostFacet.get({ pageId: 'p1', postId: 'abc' });
	 * if (post) console.log(post.postTitle);
	 * ```
	 */
	async get(query: Pick<T, PK | SK> & Partial<T>): Promise<T | null>;
	/**
	 * Batch-fetch records by their exact partition and sort keys.
	 *
	 * Splits the input into `BatchGetItem` batches of 100 (Dynamo DB's
	 * hard limit), issues them in parallel, and retries
	 * `UnprocessedKeys` with exponential backoff up to 10 attempts.
	 * Records that never come back after retries are silently dropped,
	 * so the returned array may be shorter than `queries`.
	 *
	 * @param queries - Array of objects each providing the PK and SK field values.
	 * @returns The records that were found, in unspecified order.
	 *
	 * @example
	 * ```ts
	 * const posts = await PostFacet.get([
	 *   { pageId: 'p1', postId: 'a' },
	 *   { pageId: 'p1', postId: 'b' },
	 * ]);
	 * ```
	 */
	async get(queries: (Pick<T, PK | SK> & Partial<T>)[]): Promise<T[]>;
	/**
	 * Fetch a single record and project only the requested attributes.
	 *
	 * Issues a Dynamo DB `GetItem` with a `ProjectionExpression` built from
	 * `options.select` plus the facet's PK and SK field names (always
	 * auto-included). The result is validated by the facet's
	 * `pickValidator` — the full `validator` is skipped because the record
	 * is intentionally partial.
	 *
	 * @remarks
	 * This overload is only callable on facets constructed with a
	 * `pickValidator`. Calling it on a facet without one is a type error
	 * (the `this:` constraint on the signature resolves to `never`).
	 *
	 * @param query - Object providing the PK and SK field values.
	 * @param options - Must include `select`; other {@link GetOptions} fields are forwarded.
	 * @returns A `Pick<T, K | PK | SK>` if found, or `null`. The PK/SK
	 * fields are always present in the result even if omitted from `select`.
	 *
	 * @example
	 * ```ts
	 * const slim = await PostFacet.get(
	 *   { pageId: 'p1', postId: 'abc' },
	 *   { select: ['postTitle', 'postStatus'] },
	 * );
	 * // slim has type: { postTitle; postStatus; pageId; postId } | null
	 * ```
	 */
	async get<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		query: Pick<T, PK | SK> & Partial<T>,
		options: GetOptions<T, K> & { select: readonly [K, ...K[]] },
	): Promise<Pick<T, K | PK | SK> | null>;
	/**
	 * Batch-fetch records and project only the requested attributes.
	 *
	 * Same batching, retry, and drop semantics as the non-projected array
	 * overload; the projection is applied per-batch via
	 * `BatchGetItem.RequestItems[table].ProjectionExpression`. Each record
	 * is run through the facet's `pickValidator`.
	 *
	 * @remarks
	 * Only callable on facets constructed with a `pickValidator`.
	 *
	 * @param queries - Array of objects each providing the PK and SK field values.
	 * @param options - Must include `select`; other {@link GetOptions} fields are forwarded.
	 * @returns The projected records that were found. PK/SK fields are
	 * always present even if omitted from `select`.
	 *
	 * @example
	 * ```ts
	 * const slim = await PostFacet.get(
	 *   [{ pageId: 'p1', postId: 'a' }, { pageId: 'p1', postId: 'b' }],
	 *   { select: ['postTitle'] },
	 * );
	 * ```
	 */
	async get<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		queries: (Pick<T, PK | SK> & Partial<T>)[],
		options: GetOptions<T, K> & { select: readonly [K, ...K[]] },
	): Promise<Pick<T, K | PK | SK>[]>;
	async get<K extends keyof T>(
		query: (Pick<T, PK | SK> & Partial<T>)[] | (Pick<T, PK | SK> & Partial<T>),
		options: GetOptions<T, K> = {},
	): Promise<
		T[] | T | null | Pick<T, K | PK | SK>[] | Pick<T, K | PK | SK> | null
	> {
		if (!Array.isArray(query)) {
			return getSingleItem(this, query, options);
		}
		if (query.length === 0) {
			return [];
		}

		return getBatchItems(this, query, options);
	}

	/**
	 * Delete a single record by its exact partition and sort key.
	 *
	 * The `record` must contain the model fields that compose this facet's
	 * `PK` and `SK`; other fields are ignored. An optional
	 * {@link DeleteOptions.condition} expression is compiled into a
	 * Dynamo DB `ConditionExpression` — if the stored record doesn't
	 * satisfy it, the delete is reported as a failure rather than
	 * throwing.
	 *
	 * @param record - Object providing the PK and SK field values.
	 * @param options - Optional {@link DeleteOptions}, e.g. a `condition`.
	 * @returns A {@link DeleteResponse} with `deleted` / `failed` arrays
	 * and a `hasFailures` flag. Individual failures do not reject the
	 * returned promise.
	 *
	 * @example
	 * ```ts
	 * const result = await PostFacet.delete(
	 *   { pageId: 'p1', postId: 'abc' },
	 *   { condition: ['postStatus', '=', 'draft'] },
	 * );
	 * if (result.hasFailures) console.error(result.failed);
	 * ```
	 */
	async delete(
		record: Pick<T, PK | SK> & Partial<T>,
		options?: DeleteOptions<Pick<T, PK | SK> & Partial<T>>,
	): Promise<DeleteResponse<Pick<T, PK | SK> & Partial<T>>>;
	/**
	 * Batch-delete records by their exact partition and sort keys.
	 *
	 * Splits the input into `BatchWriteItem` batches of 25 (Dynamo DB's
	 * hard limit), issues them in parallel, and retries `UnprocessedItems`
	 * up to 5 times with exponential backoff. Any records that still fail
	 * after retries land in the response's `failed` array — they do not
	 * reject the promise.
	 *
	 * @remarks
	 * Conditional deletes are not supported in the batch form; use the
	 * single-record overload if you need a {@link DeleteOptions.condition}.
	 *
	 * @param records - Array of objects each providing the PK and SK field values.
	 * @returns A {@link DeleteResponse} aggregating successes and failures
	 * across all batches.
	 *
	 * @example
	 * ```ts
	 * const result = await PostFacet.delete([
	 *   { pageId: 'p1', postId: 'a' },
	 *   { pageId: 'p1', postId: 'b' },
	 * ]);
	 * ```
	 */
	async delete(
		records: (Pick<T, PK | SK> & Partial<T>)[],
	): Promise<DeleteResponse<Pick<T, PK | SK> & Partial<T>>>;
	async delete(
		records:
			| (Pick<T, PK | SK> & Partial<T>)[]
			| (Pick<T, PK | SK> & Partial<T>),
		options?: DeleteOptions<Pick<T, PK | SK> & Partial<T>>,
	): Promise<DeleteResponse<Pick<T, PK | SK> & Partial<T>>> {
		if (Array.isArray(records)) {
			return deleteItems(this, records);
		}

		return deleteSingleItem(this, records, options);
	}

	/**
	 * Write a single record to the Dynamo DB table.
	 *
	 * The record is marshalled through {@link Facet.in} — synthetic `PK`,
	 * `SK`, `GSI*PK`/`GSI*SK`, `facet`, and `ttl` attributes are stamped
	 * on before the `PutItem` request. An optional
	 * {@link PutOptions.condition} expression is compiled into a
	 * `ConditionExpression`; a failed condition resolves as
	 * `wasSuccessful: false` rather than throwing.
	 *
	 * @remarks
	 * Input validation is off by default (see `validateInput` on
	 * {@link FacetOptions}); enable it to run the facet's `validator`
	 * against the record before marshalling.
	 *
	 * @param record - The full model to write.
	 * @param options - Optional {@link PutOptions}, e.g. a `condition`.
	 * @returns {@link PutSingleItemResponse} carrying the written record and
	 * a `wasSuccessful` flag.
	 *
	 * @example
	 * ```ts
	 * const result = await PostFacet.put(post, {
	 *   condition: ['postId', 'not_exists'],
	 * });
	 * if (!result.wasSuccessful) console.error(result.error);
	 * ```
	 */
	async put(
		record: T,
		options?: PutOptions<T>,
	): Promise<PutSingleItemResponse<T>>;
	/**
	 * Batch-write records to the Dynamo DB table.
	 *
	 * Splits the input into `BatchWriteItem` batches of 25 (Dynamo DB's
	 * hard limit), deduplicates records that share the same PK+SK within
	 * a batch (last-write-wins), and issues batches in parallel.
	 * `UnprocessedItems` are retried up to 5 times with exponential
	 * backoff; anything that still fails lands in the response's
	 * `failed` array rather than rejecting the promise.
	 *
	 * @remarks
	 * Conditional writes are not supported in the batch form; use the
	 * single-record overload if you need a {@link PutOptions.condition}.
	 *
	 * @param records - Array of full models to write.
	 * @returns A {@link PutResponse} aggregating successes and failures.
	 *
	 * @example
	 * ```ts
	 * const result = await PostFacet.put([post1, post2, post3]);
	 * console.log(`wrote ${result.put.length}, failed ${result.failed.length}`);
	 * ```
	 */
	async put(records: T[]): Promise<PutResponse<T>>;
	async put(
		records: T[] | T,
		options?: PutOptions<T>,
	): Promise<PutResponse<T> | PutSingleItemResponse<T>> {
		if (Array.isArray(records)) {
			return putItems(this, records);
		}

		return putSingleItem(this, records, options);
	}

	/**
	 * Begin a query over a single partition on the base table.
	 *
	 * Returns a {@link PartitionQuery} builder that exposes the sort-key
	 * operators — `equals`, `greaterThan`, `greaterThanOrEqual`, `lessThan`,
	 * `lessThanOrEqual`, `beginsWith`, `between`, `list`, and `first`. Each
	 * accepts a filter expression, pagination cursor, and limit via
	 * {@link QueryOptions}.
	 *
	 * @param partition - Object providing the PK field values. Extra fields
	 * are ignored; only the facet's PK fields are read.
	 * @param shard - Optional shard id when the partition key is configured
	 * with a {@link ShardConfiguration}. If omitted on a sharded facet, you
	 * must iterate every shard to list the full partition.
	 * @returns A {@link PartitionQuery} builder.
	 *
	 * @example
	 * ```ts
	 * // All posts in page 'p1'
	 * const all = await PostFacet.query({ pageId: 'p1' }).list();
	 *
	 * // Posts in page 'p1' whose postId starts with 'draft-'
	 * const drafts = await PostFacet.query({ pageId: 'p1' })
	 *   .beginsWith({ postId: 'draft-' });
	 *
	 * // With a filter, limit, and cursor
	 * const page = await PostFacet.query({ pageId: 'p1' }).list({
	 *   filter: ['postStatus', '<>', 'deleted'],
	 *   limit: 20,
	 *   cursor: previousPage.cursor,
	 * });
	 * ```
	 */
	query(
		partition: Pick<T, PK> & Partial<T>,
		shard?: number,
	): PartitionQuery<T, PK, SK, never, never, PV> {
		return new PartitionQuery({
			facet: this,
			partitionIdentifier: partition,
			shard: shard,
		});
	}

	/**
	 * Register a Global Secondary Index on this facet and thread it into
	 * the facet's type.
	 *
	 * Mutates `this`: after the call, the GSI is reachable both by its
	 * enum name (e.g. `facet.GSI1`) and, if provided, by its `alias`
	 * (e.g. `facet.byStatus`). The return type is the facet narrowed with
	 * those index accessors, so the chained `.addIndex(...)` pattern
	 * keeps the full type visible at the call site.
	 *
	 * @remarks
	 * The same `index` slot cannot be registered twice — attempting to
	 * reuse it throws. Aliases must not collide with existing properties
	 * on the facet.
	 *
	 * Every registered index's `GSIxPK`/`GSIxSK` attributes are written on
	 * every subsequent `put`, and stripped from every read, so the table
	 * must declare them as `AttributeDefinitions`. This library assumes
	 * the GSIs are created with `ProjectionType: ALL`.
	 *
	 * @param options - {@link AddIndexOptions} — the GSI slot, its PK/SK
	 * {@link KeyConfiguration}s, and an optional alias.
	 * @returns The facet, with the index's accessors merged into its type.
	 *
	 * @example
	 * ```ts
	 * const PostFacet = new Facet({ ...baseOptions })
	 *   .addIndex({
	 *     index: Index.GSI1,
	 *     alias: 'byStatus',
	 *     PK: { keys: ['userId', 'status'], prefix: '#STATUS' },
	 *     SK: { keys: ['timestamp'], prefix: '#TS' },
	 *   });
	 *
	 * // Both work and return a PartitionQuery over the index:
	 * await PostFacet.GSI1.query({ userId: 'u1', status: 'queued' }).list();
	 * await PostFacet.byStatus.query({ userId: 'u1', status: 'queued' }).list();
	 * ```
	 */
	addIndex<
		I extends Index,
		GSIPK extends Keys<T>,
		GSISK extends Keys<T>,
		A extends string,
	>({
		PK,
		SK,
		index,
		alias,
	}: AddIndexOptions<T, I, GSIPK, GSISK, A>): FacetWithIndex<
		this,
		FacetIndexKeys<T, PK, SK, GSIPK, GSISK, I, A, PV>
	> {
		if (this.#indexes.has(index)) {
			throw new Error(
				`Index ${index} is already registered on this Facet. Each GSI slot can only be used once.`,
			);
		}
		if (alias && Object.prototype.hasOwnProperty.call(this, alias)) {
			throw new Error(
				`The index alias ${alias} already exists on this Facet. Pick another index to use for this alias.`,
			);
		}

		const facetIndex = new FacetIndex(index, this, PK, SK);
		this.#indexes.set(index, facetIndex);

		Object.assign(this, {
			[index]: facetIndex,
		});

		if (alias) {
			Object.assign(this, {
				[alias]: facetIndex,
			});
		}

		/**
		 * We mutate this class as a function
		 */
		return this as unknown as FacetWithIndex<
			this,
			FacetIndexKeys<T, PK, SK, GSIPK, GSISK, I, A, PV>
		>;
	}
}

/**
 * `Facet<T, PK, SK, PV>` is the type of a Facet instance. `PV` is the
 * phantom type that tracks whether a `pickValidator` was configured on
 * the facet; projected read overloads (`get(q, { select })`) are only
 * callable when `PV extends PickValidator<T>`.
 */
export type Facet<
	T extends WithoutReservedAttributes<T>,
	PK extends Keys<T> = never,
	SK extends Keys<T> = never,
	PV extends PickValidator<T> | undefined = PickValidator<T> | undefined,
> = FacetImpl<T, PK, SK, PV>;

/**
 * The constructor-type shape for `Facet`. Overloaded so that passing a
 * `pickValidator` narrows the instance type to `Facet<T, PK, SK, PickValidator<T>>`
 * (unlocking projected reads), while omitting it yields
 * `Facet<T, PK, SK, undefined>` (projection methods are compile-time gated
 * off). Writing `new Facet({ ... })` picks the correct overload based on
 * whether the options object contains a `pickValidator`.
 */
export interface FacetConstructor {
	new <
		T extends WithoutReservedAttributes<T>,
		PK extends Keys<T> = never,
		SK extends Keys<T> = never,
	>(
		opts: FacetOptions<T, PK, SK, PickValidator<T>> & {
			pickValidator: PickValidator<T>;
		},
	): FacetImpl<T, PK, SK, PickValidator<T>>;
	new <
		T extends WithoutReservedAttributes<T>,
		PK extends Keys<T> = never,
		SK extends Keys<T> = never,
	>(
		opts: FacetOptions<T, PK, SK, undefined>,
	): FacetImpl<T, PK, SK, undefined>;
}

// `Facet` is deliberately both a type alias (above) and a value (the typed
// constructor); TS merges them across the type and value namespaces.
// eslint-disable-next-line no-redeclare
export const Facet: FacetConstructor = FacetImpl as unknown as FacetConstructor;

export interface AddIndexOptions<
	T extends WithoutReservedAttributes<T>,
	I extends Index,
	GSIPK extends Keys<T>,
	GSISK extends Keys<T>,
	A extends string,
> {
	index: I;
	PK: KeyConfiguration<T, GSIPK>;
	SK: KeyConfiguration<T, GSISK>;
	alias?: A;
}

export class FacetIndex<
	T extends WithoutReservedAttributes<T>,
	PK extends Keys<T> = Keys<T>,
	SK extends Keys<T> = Keys<T>,
	GSIPK extends Keys<T> = Keys<T>,
	GSISK extends Keys<T> = Keys<T>,
	PV extends PickValidator<T> | undefined = PickValidator<T> | undefined,
> {
	#facet: Facet<T, PK, SK, PV>;
	#PK: KeyConfiguration<T, GSIPK>;
	#SK: KeyConfiguration<T, GSISK>;

	readonly indexName: Index;

	/**
	 * @internal Not intended for direct use. A `FacetIndex` is wired to
	 * its parent `Facet` via `Facet.addIndex(...)`, which registers the
	 * index in the facet's internal map so `in()`/`out()` stamp and strip
	 * the synthetic `GSInPK`/`GSInSK` attributes. A manually constructed
	 * instance is inert.
	 */
	constructor(
		indexName: Index,
		facet: Facet<T, PK, SK, PV>,
		gsipk: KeyConfiguration<T, GSIPK>,
		gsisk: KeyConfiguration<T, GSISK>,
	) {
		this.indexName = indexName;
		this.#facet = facet;
		this.#PK = gsipk;
		this.#SK = gsisk;
	}

	/**
	 * Construct the partition key
	 */
	pk(model: Partial<T>, shard?: number) {
		return buildKey(this.#PK, model, this.#facet.delimiter, shard);
	}

	/**
	 * Construct the sort key
	 */
	sk(model: Partial<T>, shard?: number) {
		return buildKey(this.#SK, model, this.#facet.delimiter, shard);
	}

	/**
	 * The T-level field names that compose this index's partition and sort
	 * keys. Projected reads on the index auto-include these so callers can
	 * round results back into a `get`/`delete`/`put`.
	 */
	get keyFields(): readonly (GSIPK | GSISK)[] {
		return [...this.#PK.keys, ...this.#SK.keys];
	}

	/**
	 * Query a partition within the index
	 */
	query(
		partition: Pick<T, GSIPK> & Partial<T>,
		shard?: number,
	): PartitionQuery<T, PK, SK, GSIPK, GSISK, PV> {
		return new PartitionQuery({
			facet: this.#facet,
			partitionIdentifier: partition,
			index: this,
			shard: shard,
		});
	}
}

/**
 * Options for configuring a Faceteer Facet
 */
export interface FacetOptions<
	T extends WithoutReservedAttributes<T>,
	PK extends Keys<T>,
	SK extends Keys<T>,
	PV extends PickValidator<T> | undefined = PickValidator<T> | undefined,
> {
	/**
	 * The name of the facet that is stored under `facet` for every record.
	 */
	name: string;

	/**
	 * How to build the partition key
	 * for this facet in the table
	 *
	 * ## Example
	 * ```
	 * {
	 *   keys: ['pageId'],
	 *   prefix: Prefix.Page,
	 * }
	 * ```
	 */
	PK: KeyConfiguration<T, PK>;
	/**
	 * How to build the sort key
	 * for this object in the table
	 */
	SK: KeyConfiguration<T, SK>;
	/**
	 * A {@link Validator} for records in this {@link Facet}
	 */
	validator: Validator<T>;

	/**
	 * An optional {@link PickValidator} used on projected reads (`select`).
	 *
	 * If omitted, the `select` option on read methods is not available at
	 * compile time — the facet type gates projection methods behind the
	 * presence of this property. The gate is enforced purely via the `PV`
	 * generic; there is no runtime check.
	 */
	pickValidator?: PV;

	/**
	 * The delimiter used when constructing composite keys
	 * for Dynamo DB records
	 *
	 * By default this is `_`
	 */
	delimiter?: string;

	/**
	 * An optional key that should be used as the TTL key
	 * in Dynamo DB
	 */
	ttl?: Keys<T>;

	/**
	 * Tells converter to use `iso` or `unix` format
	 */
	dateFormat?: ConverterOptions['dateFormat'];

	/**
	 * Prevents DynamoDB from converting empty values such
	 * as "" to `null` at a cost to storage optimization.
	 */
	convertEmptyValues?: ConverterOptions['convertEmptyValues'];

	/**
	 * Validates types before putting them into the database.
	 *
	 * **WARNING:** This can weaken performance so use sparingly.
	 */
	validateInput?: boolean;

	/**
	 * Connection information for Dynamo DB.
	 */
	connection: {
		/**
		 * A configured `DynamoDB` client from `@aws-sdk/client-dynamodb`.
		 * The caller owns this instance — its region, credentials,
		 * endpoint, and middleware are whatever the caller configured.
		 */
		dynamoDb: DynamoDB;
		/**
		 * The Dynamo DB table to write to
		 */
		tableName: string;
	};
}
