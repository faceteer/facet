import { Converter } from '@faceteer/converter';
import type { ConverterOptions } from '@faceteer/converter/converter-options';
import { DynamoDB, AttributeValue } from '@aws-sdk/client-dynamodb';
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
> = Record<I, FacetIndex<T, PK, SK, GSIPK, GSISK>> &
	Record<A, FacetIndex<T, PK, SK, GSIPK, GSISK>>;

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
	#indexes: Map<Index, FacetIndex<T, PK, SK>> = new Map();

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
		let { dynamoDb, tableName } = connection;
		if (!dynamoDb) {
			dynamoDb = new DynamoDB({});
		}
		this.connection = {
			dynamoDb,
			tableName,
		};
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
	 * Get records from the table by their exact partition
	 * key and sort key
	 * @param query
	 */
	async get<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		query: (Pick<T, PK | SK> & Partial<T>)[],
		options: GetOptions<T, K> & { select: readonly [K, ...K[]] },
	): Promise<Pick<T, K | PK | SK>[]>;
	async get<K extends keyof T>(
		this: [PV] extends [PickValidator<T>] ? this : never,
		query: Pick<T, PK | SK> & Partial<T>,
		options: GetOptions<T, K> & { select: readonly [K, ...K[]] },
	): Promise<Pick<T, K | PK | SK> | null>;
	async get(query: (Pick<T, PK | SK> & Partial<T>)[]): Promise<T[]>;
	async get(query: Pick<T, PK | SK> & Partial<T>): Promise<T | null>;
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
	 * Put a record into the Dynamo DB table
	 * @param records
	 */
	async delete(
		record: Pick<T, PK | SK> & Partial<T>,
		options?: DeleteOptions<Pick<T, PK | SK> & Partial<T>>,
	): Promise<DeleteResponse<Pick<T, PK | SK> & Partial<T>>>;
	/**
	 * Put multiple records into the Dynamo DB table
	 * @param records
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
	 * Put a record into the Dynamo DB table
	 * @param records
	 */
	async put(
		record: T,
		options?: PutOptions<T>,
	): Promise<PutSingleItemResponse<T>>;
	/**
	 * Put multiple records into the Dynamo DB table
	 * @param records
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
	 * Query a partition on the Facet
	 */
	query(
		partition: Pick<T, PK> & Partial<T>,
		shard?: number,
	): PartitionQuery<T, PK, SK> {
		return new PartitionQuery({
			facet: this,
			partitionIdentifier: partition,
			shard: shard,
		});
	}

	/**
	 * Register a GSI for a Facet
	 * @param index The name of the actual GSI configured in Dynamo DB
	 * @param partitionKey The partition key for the index
	 * @param sortKey The sort key for the index
	 * @param alias An optional alias to call the index by
	 * @returns
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
		FacetIndexKeys<T, PK, SK, GSIPK, GSISK, I, A>
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
			FacetIndexKeys<T, PK, SK, GSIPK, GSISK, I, A>
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
> {
	#facet: Facet<T, PK, SK>;
	#PK: KeyConfiguration<T, GSIPK>;
	#SK: KeyConfiguration<T, GSISK>;

	readonly indexName: Index;

	constructor(
		indexName: Index,
		facet: Facet<T, PK, SK>,
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
	 * Query a partition within the index
	 */
	query(
		partition: Pick<T, GSIPK> & Partial<T>,
		shard?: number,
	): PartitionQuery<T, PK, SK, GSIPK, GSISK> {
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
		 * A configured connection to Dynamo DB from
		 * the aws-sdk. If this is not set Faceteer will
		 * attempt to make it's own connection to Dynamo DB
		 */
		dynamoDb?: DynamoDB;
		/**
		 * The Dynamo DB table to write to
		 */
		tableName: string;
	};
}
