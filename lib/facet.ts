import { Converter } from '@faceteer/converter';
import type { ConverterOptions } from '@faceteer/converter/converter-options';
import { DynamoDB } from 'aws-sdk';
import {
	deleteItems,
	DeleteOptions,
	DeleteResponse,
	deleteSingleItem,
} from './delete';
import { getBatchItems, getSingleItem } from './get';
import {
	buildKey,
	Index,
	IndexKeyNameMap,
	KeyConfiguration,
	PK,
	SK,
} from './keys';
import {
	putItems,
	PutOptions,
	PutResponse,
	putSingleItem,
	PutSingleItemResponse,
} from './put';
import { PartitionQuery } from './query';

export type Validator<T> = (input: unknown) => T;

export type FacetWithIndex<
	T,
	PK extends keyof T,
	SK extends keyof T,
	GSIPK extends keyof T,
	GSISK extends keyof T,
	I extends Index,
	A extends string = never,
> = Record<I, FacetIndex<T, PK, SK, GSIPK, GSISK>> &
	Record<A, FacetIndex<T, PK, SK, GSIPK, GSISK>>;

export class Facet<
	T,
	PK extends keyof T = keyof T,
	SK extends keyof T = keyof T,
> {
	#PK: KeyConfiguration<T, PK>;
	#SK: KeyConfiguration<T, SK>;
	#validator: Validator<T>;
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
	readonly ttl?: keyof T;
	/**
	 * The configured connection to Dynamo DB
	 */
	readonly connection: { dynamoDb: DynamoDB; tableName: string };

	constructor({
		PK,
		SK,
		connection,
		validator,
		convertEmptyValues,
		dateFormat,
		delimiter = '_',
		ttl,
		validateInput = false,
	}: FacetOptions<T, PK, SK>) {
		this.#PK = PK;
		this.#SK = SK;
		this.#validator = validator;
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
		return buildKey(this.#SK, model, this.delimiter, shard ?? null);
	}

	/**
	 * Convert a model to a record that can be
	 * stored directly in DynamoDB
	 */
	in(model: T): DynamoDB.AttributeMap {
		if (this.#validateInput) {
			model = this.#validator(model);
		}
		const attributes: Partial<T> = {};

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
		 * Attempt to convert the TTL attribute to a unix timestamp
		 */
		let ttlAttribute: unknown = this.ttl ? model[this.ttl] : undefined;
		if (ttlAttribute instanceof Date) {
			ttlAttribute = Math.floor(ttlAttribute.getTime() / 1000);
		} else if (typeof ttlAttribute === 'string') {
			ttlAttribute = parseInt(ttlAttribute);
		}

		if (Number.isNaN(ttlAttribute)) {
			ttlAttribute = undefined;
		}

		const dynamoDbRecord = {
			...attributes,
			...facetKeys,
			ttl: this.ttl ? model[this.ttl] : undefined,
		};

		return Converter.marshall(dynamoDbRecord, {
			wrapNumbers: true,
			dateFormat: this.#dateFormat,
			convertEmptyValues: this.#convertEmptyValues,
		});
	}

	/**
	 * Convert and validate a dynamo DB record
	 */
	out(record: DynamoDB.AttributeMap): T {
		const parsedRecord = Converter.unmarshall(record);

		const recordToValidate: any = parsedRecord;

		/**
		 * Delete any constructed keys from the model before
		 * validating and returning
		 */
		delete recordToValidate['PK'];
		delete recordToValidate['SK'];
		for (const index of this.#indexes.keys()) {
			const indexKeyNames = IndexKeyNameMap[index];
			delete recordToValidate[indexKeyNames.PK];
			delete recordToValidate[indexKeyNames.SK];
			if (this.ttl) {
				delete recordToValidate['ttl'];
			}
		}

		return this.#validator(recordToValidate);
	}

	/**
	 * Get records from the table by their exact partition
	 * key and sort key
	 * @param query
	 */
	async get(query: (Pick<T, PK | SK> & Partial<T>)[]): Promise<T[]>;
	async get(query: Pick<T, PK | SK> & Partial<T>): Promise<T | null>;
	async get(
		query: (Pick<T, PK | SK> & Partial<T>)[] | (Pick<T, PK | SK> & Partial<T>),
	): Promise<T[] | T | null> {
		if (!Array.isArray(query)) {
			return getSingleItem(this, query);
		}
		if (query.length === 0) {
			return [];
		}

		return getBatchItems(this, query);
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
		GSIPK extends keyof T,
		GSISK extends keyof T,
		A extends string,
	>({
		PK,
		SK,
		index,
		alias,
	}: AddIndexOptions<T, I, GSIPK, GSISK, A>): this &
		FacetWithIndex<T, PK, SK, GSIPK, GSISK, I, A> {
		const facetIndex = new FacetIndex(index, this, PK, SK);

		Object.assign(this, {
			[index]: facetIndex,
		});

		if (alias) {
			/**
			 * Make sure that the alias is not an existing method or property on a Facet
			 */
			if (Object.prototype.hasOwnProperty.call(this, alias)) {
				throw new Error(
					`The index alias ${alias} already exists on this Facet. Pick another index to use for this alias.`,
				);
			}

			Object.assign(this, {
				[alias]: facetIndex,
			});
		}

		/**
		 * We mutate this class as a function
		 */
		return this as unknown as this &
			FacetWithIndex<T, PK, SK, GSIPK, GSISK, I, A>;
	}
}

export interface AddIndexOptions<
	T,
	I extends Index,
	GSIPK extends keyof T,
	GSISK extends keyof T,
	A extends string,
> {
	index: I;
	PK: KeyConfiguration<T, GSIPK>;
	SK: KeyConfiguration<T, GSISK>;
	alias?: A;
}

export class FacetIndex<
	T,
	PK extends keyof T = keyof T,
	SK extends keyof T = keyof T,
	GSIPK extends keyof T = keyof T,
	GSISK extends keyof T = keyof T,
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
		return buildKey(this.#SK, model, this.#facet.delimiter, shard ?? null);
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
export interface FacetOptions<T, PK extends keyof T, SK extends keyof T> {
	/**
	 * How to build the partition key
	 * for this facet in the table
	 */
	PK: KeyConfiguration<T, PK>;
	/**
	 * How to build the sort key
	 * for this object in the table
	 */
	SK: KeyConfiguration<T, SK>;
	/**
	 * A function that can take in any input and either
	 * return a valid object of type `T` or throw if the
	 * input is invalid
	 */
	validator: Validator<T>;

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
	ttl?: keyof T;

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
	 * WARNING: This can weaken performance so use sparingly.
	 */
	validateInput?: boolean;

	/**
	 * Connection information for Dynamo DB.
	 *
	 * Required to use any of the data methods
	 */
	connection: {
		/**
		 * A configured connection to Dynamo DB from
		 * the aws-sdk
		 */
		dynamoDb?: DynamoDB;
		/**
		 * The Dynamo DB table to write to
		 */
		tableName: string;
	};
}
