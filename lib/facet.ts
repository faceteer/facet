import type { DynamoDB } from 'aws-sdk';
import { Converter } from '@faceteer/converter';
import {
	buildKey,
	Index,
	IndexKeyConfiguration,
	IndexKeyNameMap,
	IndexKeyOptions,
	IndexPrivatePropertyMap,
	isIndex,
	KeyConfiguration,
	PK,
	SK,
} from './keys';
import msgpack from '@msgpack/msgpack';
import cbor from 'cbor';
export type Validator<T> = (input: unknown) => T;
import zlib from 'zlib';
import { getBatchItems, getSingleItem } from './get';
import { PartitionQuery } from './query';
import { putItems, PutOptions, PutResponse, putSingleItem } from './put';
import { ConverterOptions } from '@faceteer/converter/converter-options';

export type RawFormat = 'json' | 'msgpack' | 'cbor';

export interface FacetOptions<
	T,
	PK extends keyof T,
	SK extends keyof T,
	GSI1PK extends keyof T = any,
	GSI1SK extends keyof T = any,
	GSI2PK extends keyof T = any,
	GSI2SK extends keyof T = any,
	GSI3PK extends keyof T = any,
	GSI3SK extends keyof T = any,
	GSI4PK extends keyof T = any,
	GSI4SK extends keyof T = any,
	GSI5PK extends keyof T = any,
	GSI5SK extends keyof T = any,
	GSI6PK extends keyof T = any,
	GSI6SK extends keyof T = any,
	GSI7PK extends keyof T = any,
	GSI7SK extends keyof T = any,
	GSI8PK extends keyof T = any,
	GSI8SK extends keyof T = any,
	GSI9PK extends keyof T = any,
	GSI9SK extends keyof T = any,
	GSI10PK extends keyof T = any,
	GSI10SK extends keyof T = any,
	GSI11PK extends keyof T = any,
	GSI11SK extends keyof T = any,
	GSI12PK extends keyof T = any,
	GSI12SK extends keyof T = any,
	GSI13PK extends keyof T = any,
	GSI13SK extends keyof T = any,
	GSI14PK extends keyof T = any,
	GSI14SK extends keyof T = any,
	GSI15PK extends keyof T = any,
	GSI15SK extends keyof T = any,
	GSI16PK extends keyof T = any,
	GSI16SK extends keyof T = any,
	GSI17PK extends keyof T = any,
	GSI17SK extends keyof T = any,
	GSI18PK extends keyof T = any,
	GSI18SK extends keyof T = any,
	GSI19PK extends keyof T = any,
	GSI19SK extends keyof T = any,
	GSI20PK extends keyof T = any,
	GSI20SK extends keyof T = any,
> {
	/**
	 * How to build the partition key
	 * for this object in the table
	 */
	PK: KeyConfiguration<T, PK>;

	/**
	 * How to build the sort key
	 * for this object in the table
	 */
	SK: KeyConfiguration<T, SK>;

	/**
	 * Optional configuration for how to build
	 * the partition and sort keys for Global
	 * Secondary Indexes.
	 */
	indexes?: IndexKeyOptions<
		T,
		GSI1PK,
		GSI1SK,
		GSI2PK,
		GSI2SK,
		GSI3PK,
		GSI3SK,
		GSI4PK,
		GSI4SK,
		GSI5PK,
		GSI5SK,
		GSI6PK,
		GSI6SK,
		GSI7PK,
		GSI7SK,
		GSI8PK,
		GSI8SK,
		GSI9PK,
		GSI9SK,
		GSI10PK,
		GSI10SK,
		GSI11PK,
		GSI11SK,
		GSI12PK,
		GSI12SK,
		GSI13PK,
		GSI13SK,
		GSI14PK,
		GSI14SK,
		GSI15PK,
		GSI15SK,
		GSI16PK,
		GSI16SK,
		GSI17PK,
		GSI17SK,
		GSI18PK,
		GSI18SK,
		GSI19PK,
		GSI19SK,
		GSI20PK,
		GSI20SK
	>;
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
	 * Store the model under a single key
	 * instead of individual attributes in a
	 * Dynamo DB object
	 *
	 * - `json`: JSON serialize
	 * - `msgpack`: Convert to MessagePack
	 * - `cbor`: Convert to CBOR
	 */
	raw?: RawFormat;
	/**
	 * Enable compression.
	 *
	 * Only used if `raw` is set to `
	 *
	 * Default `false`.
	 */
	compress?: boolean;

	dateFormat?: ConverterOptions['dateFormat'];
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
		dynamoDb: DynamoDB;
		/**
		 * The Dynamo DB table to write to
		 */
		tableName: string;
	};
}

export class Facet<
	T,
	PK extends keyof T,
	SK extends keyof T,
	GSI1PK extends keyof T = any,
	GSI1SK extends keyof T = any,
	GSI2PK extends keyof T = any,
	GSI2SK extends keyof T = any,
	GSI3PK extends keyof T = any,
	GSI3SK extends keyof T = any,
	GSI4PK extends keyof T = any,
	GSI4SK extends keyof T = any,
	GSI5PK extends keyof T = any,
	GSI5SK extends keyof T = any,
	GSI6PK extends keyof T = any,
	GSI6SK extends keyof T = any,
	GSI7PK extends keyof T = any,
	GSI7SK extends keyof T = any,
	GSI8PK extends keyof T = any,
	GSI8SK extends keyof T = any,
	GSI9PK extends keyof T = any,
	GSI9SK extends keyof T = any,
	GSI10PK extends keyof T = any,
	GSI10SK extends keyof T = any,
	GSI11PK extends keyof T = any,
	GSI11SK extends keyof T = any,
	GSI12PK extends keyof T = any,
	GSI12SK extends keyof T = any,
	GSI13PK extends keyof T = any,
	GSI13SK extends keyof T = any,
	GSI14PK extends keyof T = any,
	GSI14SK extends keyof T = any,
	GSI15PK extends keyof T = any,
	GSI15SK extends keyof T = any,
	GSI16PK extends keyof T = any,
	GSI16SK extends keyof T = any,
	GSI17PK extends keyof T = any,
	GSI17SK extends keyof T = any,
	GSI18PK extends keyof T = any,
	GSI18SK extends keyof T = any,
	GSI19PK extends keyof T = any,
	GSI19SK extends keyof T = any,
	GSI20PK extends keyof T = any,
	GSI20SK extends keyof T = any,
> {
	#PK: KeyConfiguration<T, PK>;
	#SK: KeyConfiguration<T, SK>;
	#indexes: Index[] = [];
	#validator: Validator<T>;
	#raw?: RawFormat;
	#compress: boolean;
	#ttl?: keyof T;
	#dateFormat?: ConverterOptions['dateFormat'];
	readonly connection: { dynamoDb: DynamoDB; tableName: string };

	readonly delimiter: string;

	constructor({
		PK,
		SK,
		indexes = {},
		validator,
		delimiter = '_',
		raw,
		compress,
		ttl,
		connection,
		dateFormat,
	}: FacetOptions<
		T,
		PK,
		SK,
		GSI1PK,
		GSI1SK,
		GSI2PK,
		GSI2SK,
		GSI3PK,
		GSI3SK,
		GSI4PK,
		GSI4SK,
		GSI5PK,
		GSI5SK,
		GSI6PK,
		GSI6SK,
		GSI7PK,
		GSI7SK,
		GSI8PK,
		GSI8SK,
		GSI9PK,
		GSI9SK,
		GSI10PK,
		GSI10SK,
		GSI11PK,
		GSI11SK,
		GSI12PK,
		GSI12SK,
		GSI13PK,
		GSI13SK,
		GSI14PK,
		GSI14SK,
		GSI15PK,
		GSI15SK,
		GSI16PK,
		GSI16SK,
		GSI17PK,
		GSI17SK,
		GSI18PK,
		GSI18SK,
		GSI19PK,
		GSI19SK,
		GSI20PK,
		GSI20SK
	>) {
		this.#PK = PK;
		this.#SK = SK;
		this.#validator = validator;
		this.delimiter = delimiter;
		this.#raw = raw;
		this.#compress = !!compress;
		this.#ttl = ttl;
		this.#dateFormat = dateFormat;
		this.connection = connection;
		/**
		 * Create the index properties for this model for
		 * any indexes that were configured
		 */
		for (const [index, indexKeyConfig] of Object.entries(indexes)) {
			if (isIndex(index) && indexKeyConfig) {
				this.#indexes.push(index);
				const privateProperty = IndexPrivatePropertyMap[index];

				this[privateProperty] = new FacetIndex(
					index,
					this as any,
					indexKeyConfig,
				) as any;
			}
		}
		this.#indexes = Object.keys(indexes) as Index[];
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
	 * Convert a model to a record that can be
	 * stored directly in DynamoDB
	 */
	in(model: T): DynamoDB.AttributeMap {
		let attributes: Partial<T> & {
			_raw?: Uint8Array;
			_json?: string;
		} = {};

		/** If this is  */
		if (this.#raw) {
			switch (this.#raw) {
				case 'json':
					attributes._json = JSON.stringify(model);
					break;
				case 'cbor':
					attributes._raw = cbor.encode(model);
					if (this.#compress) {
						attributes._raw = zlib.brotliCompressSync(attributes._raw);
					}
					break;
				case 'msgpack':
					attributes._raw = msgpack.encode(model);
					if (this.#compress) {
						attributes._raw = zlib.brotliCompressSync(attributes._raw);
					}
					break;
				default:
					break;
			}
		} else {
			attributes = { ...model };
		}

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
		for (const index of this.#indexes) {
			const indexKeyNames = IndexKeyNameMap[index];
			facetKeys[indexKeyNames.PK] = this[index].pk(model);
			facetKeys[indexKeyNames.SK] = this[index].sk(model);
		}

		/**
		 * Attempt to convert the TTL attribute to a unix timestamp
		 */
		let ttlAttribute: T[keyof T] | undefined | number = this.#ttl
			? model[this.#ttl]
			: undefined;
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
			ttl: this.#ttl ? model[this.#ttl] : undefined,
		};

		return Converter.marshall(dynamoDbRecord, {
			convertEmptyValues: true,
			wrapNumbers: true,
			dateFormat: this.#dateFormat,
		});
	}

	/**
	 * Convert and validate a dynamo DB record
	 */
	out(record: DynamoDB.AttributeMap): T {
		const parsedRecord = Converter.unmarshall(record);

		let recordToValidate: any = parsedRecord;

		/**
		 * If the record is in a raw format we'll extract it
		 */
		if (this.#raw) {
			switch (this.#raw) {
				case 'msgpack':
					if (this.#compress) {
						recordToValidate = msgpack.decode(
							zlib.brotliDecompressSync(parsedRecord._raw),
						);
					}
					recordToValidate = msgpack.decode(parsedRecord._raw);
					break;
				case 'cbor':
					if (this.#compress) {
						recordToValidate = cbor.decode(
							zlib.brotliDecompressSync(parsedRecord._raw),
						);
					}
					recordToValidate = cbor.decode(parsedRecord._raw);
					break;
				case 'json':
					recordToValidate = JSON.parse(parsedRecord._json);
					break;
				default:
					break;
			}
		}

		/**
		 * Delete any constructed keys from the model before
		 * validating and returning
		 */
		delete recordToValidate['PK'];
		delete recordToValidate['SK'];
		for (const index of this.#indexes) {
			const indexKeyNames = IndexKeyNameMap[index];
			delete recordToValidate[indexKeyNames.PK];
			delete recordToValidate[indexKeyNames.SK];
			if (this.#ttl) {
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
	async put(record: T, options?: PutOptions<T>): Promise<PutResponse<T>>;
	/**
	 * Put multiple records into the Dynamo DB table
	 * @param records
	 */
	async put(records: T[]): Promise<PutResponse<T>>;
	async put(
		records: T[] | T,
		options?: PutOptions<T>,
	): Promise<PutResponse<T>> {
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

	// Global Secondary Indexes
	private readonly _GSI1?: FacetIndex<T, PK, SK, GSI1PK, GSI1SK>;
	private readonly _GSI2?: FacetIndex<T, PK, SK, GSI2PK, GSI2SK>;
	private readonly _GSI3?: FacetIndex<T, PK, SK, GSI3PK, GSI3SK>;
	private readonly _GSI4?: FacetIndex<T, PK, SK, GSI4PK, GSI4SK>;
	private readonly _GSI5?: FacetIndex<T, PK, SK, GSI5PK, GSI5SK>;
	private readonly _GSI6?: FacetIndex<T, PK, SK, GSI6PK, GSI6SK>;
	private readonly _GSI7?: FacetIndex<T, PK, SK, GSI7PK, GSI7SK>;
	private readonly _GSI8?: FacetIndex<T, PK, SK, GSI8PK, GSI8SK>;
	private readonly _GSI9?: FacetIndex<T, PK, SK, GSI9PK, GSI9SK>;
	private readonly _GSI10?: FacetIndex<T, PK, SK, GSI10PK, GSI10SK>;
	private readonly _GSI11?: FacetIndex<T, PK, SK, GSI11PK, GSI11SK>;
	private readonly _GSI12?: FacetIndex<T, PK, SK, GSI12PK, GSI12SK>;
	private readonly _GSI13?: FacetIndex<T, PK, SK, GSI13PK, GSI13SK>;
	private readonly _GSI14?: FacetIndex<T, PK, SK, GSI14PK, GSI14SK>;
	private readonly _GSI15?: FacetIndex<T, PK, SK, GSI15PK, GSI15SK>;
	private readonly _GSI16?: FacetIndex<T, PK, SK, GSI16PK, GSI16SK>;
	private readonly _GSI17?: FacetIndex<T, PK, SK, GSI17PK, GSI17SK>;
	private readonly _GSI18?: FacetIndex<T, PK, SK, GSI18PK, GSI18SK>;
	private readonly _GSI19?: FacetIndex<T, PK, SK, GSI19PK, GSI19SK>;
	private readonly _GSI20?: FacetIndex<T, PK, SK, GSI20PK, GSI20SK>;

	get GSI1(): FacetIndex<T, PK, SK, GSI1PK, GSI1SK> {
		const facetIndex = this._GSI1;
		if (!facetIndex) {
			throw new Error(`There is no configuration defined for GSI1`);
		}
		return facetIndex;
	}
	get GSI2(): FacetIndex<T, PK, SK, GSI2PK, GSI2SK> {
		const facetIndex = this._GSI2;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI2');
		}
		return facetIndex;
	}
	get GSI3(): FacetIndex<T, PK, SK, GSI3PK, GSI3SK> {
		const facetIndex = this._GSI3;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI3');
		}
		return facetIndex;
	}
	get GSI4(): FacetIndex<T, PK, SK, GSI4PK, GSI4SK> {
		const facetIndex = this._GSI4;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI4');
		}
		return facetIndex;
	}
	get GSI5(): FacetIndex<T, PK, SK, GSI5PK, GSI5SK> {
		const facetIndex = this._GSI5;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI5');
		}
		return facetIndex;
	}
	get GSI6(): FacetIndex<T, PK, SK, GSI6PK, GSI6SK> {
		const facetIndex = this._GSI6;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI6');
		}
		return facetIndex;
	}
	get GSI7(): FacetIndex<T, PK, SK, GSI7PK, GSI7SK> {
		const facetIndex = this._GSI7;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI7');
		}
		return facetIndex;
	}
	get GSI8(): FacetIndex<T, PK, SK, GSI8PK, GSI8SK> {
		const facetIndex = this._GSI8;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI8');
		}
		return facetIndex;
	}
	get GSI9(): FacetIndex<T, PK, SK, GSI9PK, GSI9SK> {
		const facetIndex = this._GSI9;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI9');
		}
		return facetIndex;
	}
	get GSI10(): FacetIndex<T, PK, SK, GSI10PK, GSI10SK> {
		const facetIndex = this._GSI10;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI10');
		}
		return facetIndex;
	}
	get GSI11(): FacetIndex<T, PK, SK, GSI11PK, GSI11SK> {
		const facetIndex = this._GSI11;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI11');
		}
		return facetIndex;
	}
	get GSI12(): FacetIndex<T, PK, SK, GSI12PK, GSI12SK> {
		const facetIndex = this._GSI12;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI12');
		}
		return facetIndex;
	}
	get GSI13(): FacetIndex<T, PK, SK, GSI13PK, GSI13SK> {
		const facetIndex = this._GSI13;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI13');
		}
		return facetIndex;
	}
	get GSI14(): FacetIndex<T, PK, SK, GSI14PK, GSI14SK> {
		const facetIndex = this._GSI14;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI14');
		}
		return facetIndex;
	}
	get GSI15(): FacetIndex<T, PK, SK, GSI15PK, GSI15SK> {
		const facetIndex = this._GSI15;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI15');
		}
		return facetIndex;
	}
	get GSI16(): FacetIndex<T, PK, SK, GSI16PK, GSI16SK> {
		const facetIndex = this._GSI16;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI16');
		}
		return facetIndex;
	}
	get GSI17(): FacetIndex<T, PK, SK, GSI17PK, GSI17SK> {
		const facetIndex = this._GSI17;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI17');
		}
		return facetIndex;
	}
	get GSI18(): FacetIndex<T, PK, SK, GSI18PK, GSI18SK> {
		const facetIndex = this._GSI18;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI18');
		}
		return facetIndex;
	}
	get GSI19(): FacetIndex<T, PK, SK, GSI19PK, GSI19SK> {
		const facetIndex = this._GSI19;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI19');
		}
		return facetIndex;
	}
	get GSI20(): FacetIndex<T, PK, SK, GSI20PK, GSI20SK> {
		const facetIndex = this._GSI20;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI20');
		}
		return facetIndex;
	}
}

export class FacetIndex<
	T,
	PK extends keyof T,
	SK extends keyof T,
	GSIPK extends keyof T,
	GSISK extends keyof T,
> {
	readonly name: Index;
	#facet: Facet<T, PK, SK>;
	#PK: KeyConfiguration<T, GSIPK>;
	#SK: KeyConfiguration<T, GSISK>;

	constructor(
		index: Index,
		facet: Facet<T, PK, SK>,
		keyConfig: IndexKeyConfiguration<T, GSIPK, GSISK>,
	) {
		this.name = index;
		this.#facet = facet;
		this.#PK = keyConfig.PK;
		this.#SK = keyConfig.SK;
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
