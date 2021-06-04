import type { DynamoDB } from 'aws-sdk';
import { Converter } from 'aws-sdk/clients/dynamodb';
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

export type RawFormat = 'json' | 'msgpack' | 'cbor';

export interface FacetOptions<T> {
	/**
	 * How to build the partition key
	 * for this object in the table
	 */
	PK: KeyConfiguration<T>;

	/**
	 * How to build the sort key
	 * for this object in the table
	 */
	SK: KeyConfiguration<T>;

	/**
	 * Optional configuration for how to build
	 * the partition and sort keys for Global
	 * Secondary Indexes.
	 */
	indexes?: IndexKeyOptions<T>;
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

export class Facet<T> {
	#PK: KeyConfiguration<T>;
	#SK: KeyConfiguration<T>;
	#indexes: Index[] = [];
	#validator: Validator<T>;
	#raw?: RawFormat;
	#compress: boolean;
	#ttl?: keyof T;
	#connection: { dynamoDb: DynamoDB; tableName: string };

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
	}: FacetOptions<T>) {
		this.#PK = PK;
		this.#SK = SK;
		this.#validator = validator;
		this.delimiter = delimiter;
		this.#raw = raw;
		this.#compress = !!compress;
		this.#ttl = ttl;
		this.#connection = connection;
		/**
		 * Create the index properties for this model for
		 * any indexes that were configured
		 */
		for (const [index, indexKeyConfig] of Object.entries(indexes)) {
			if (isIndex(index) && indexKeyConfig) {
				this.#indexes.push(index);
				const privateProperty = IndexPrivatePropertyMap[index];
				this[privateProperty] = new FacetIndex(index, this, indexKeyConfig);
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

		const dynamoDbRecord = {
			...attributes,
			...facetKeys,
			ttl: this.#ttl ? model[this.#ttl] : undefined,
		};

		return Converter.marshall(dynamoDbRecord, {
			convertEmptyValues: true,
			wrapNumbers: true,
		});
	}

	/**
	 * Convert and validate a dynamo DB record
	 */
	out(record: DynamoDB.AttributeMap): T {
		const parsedRecord = Converter.unmarshall(record);

		let recordToValidate: unknown = parsedRecord;

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

		return this.#validator(recordToValidate);
	}

	/**
	 * Get records from the table by their exact partition
	 * key and sort key
	 * @param query
	 */
	async get(query: Partial<T>[]): Promise<T[]>;
	async get(query: Partial<T>): Promise<T | null>;
	async get(query: Partial<T>[] | Partial<T>): Promise<T[] | T | null> {
		if (!Array.isArray(query)) {
			return getSingleItem(
				this,
				query,
				this.#connection.dynamoDb,
				this.#connection.tableName,
			);
		}
		if (query.length === 0) {
			return [];
		}

		return getBatchItems(
			this,
			query,
			this.#connection.dynamoDb,
			this.#connection.tableName,
		);
	}

	// Global Secondary Indexes
	private readonly _GSI1?: FacetIndex<T>;
	private readonly _GSI2?: FacetIndex<T>;
	private readonly _GSI3?: FacetIndex<T>;
	private readonly _GSI4?: FacetIndex<T>;
	private readonly _GSI5?: FacetIndex<T>;
	private readonly _GSI6?: FacetIndex<T>;
	private readonly _GSI7?: FacetIndex<T>;
	private readonly _GSI8?: FacetIndex<T>;
	private readonly _GSI9?: FacetIndex<T>;
	private readonly _GSI10?: FacetIndex<T>;
	private readonly _GSI11?: FacetIndex<T>;
	private readonly _GSI12?: FacetIndex<T>;
	private readonly _GSI13?: FacetIndex<T>;
	private readonly _GSI14?: FacetIndex<T>;
	private readonly _GSI15?: FacetIndex<T>;
	private readonly _GSI16?: FacetIndex<T>;
	private readonly _GSI17?: FacetIndex<T>;
	private readonly _GSI18?: FacetIndex<T>;
	private readonly _GSI19?: FacetIndex<T>;
	private readonly _GSI20?: FacetIndex<T>;

	get GSI1(): FacetIndex<T> {
		const facetIndex = this._GSI1;
		if (!facetIndex) {
			throw new Error(`There is no configuration defined for GSI1`);
		}
		return facetIndex;
	}
	get GSI2(): FacetIndex<T> {
		const facetIndex = this._GSI2;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI2');
		}
		return facetIndex;
	}
	get GSI3(): FacetIndex<T> {
		const facetIndex = this._GSI3;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI3');
		}
		return facetIndex;
	}
	get GSI4(): FacetIndex<T> {
		const facetIndex = this._GSI4;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI4');
		}
		return facetIndex;
	}
	get GSI5(): FacetIndex<T> {
		const facetIndex = this._GSI5;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI5');
		}
		return facetIndex;
	}
	get GSI6(): FacetIndex<T> {
		const facetIndex = this._GSI6;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI6');
		}
		return facetIndex;
	}
	get GSI7(): FacetIndex<T> {
		const facetIndex = this._GSI7;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI7');
		}
		return facetIndex;
	}
	get GSI8(): FacetIndex<T> {
		const facetIndex = this._GSI8;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI8');
		}
		return facetIndex;
	}
	get GSI9(): FacetIndex<T> {
		const facetIndex = this._GSI9;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI9');
		}
		return facetIndex;
	}
	get GSI10(): FacetIndex<T> {
		const facetIndex = this._GSI10;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI10');
		}
		return facetIndex;
	}
	get GSI11(): FacetIndex<T> {
		const facetIndex = this._GSI11;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI11');
		}
		return facetIndex;
	}
	get GSI12(): FacetIndex<T> {
		const facetIndex = this._GSI12;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI12');
		}
		return facetIndex;
	}
	get GSI13(): FacetIndex<T> {
		const facetIndex = this._GSI13;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI13');
		}
		return facetIndex;
	}
	get GSI14(): FacetIndex<T> {
		const facetIndex = this._GSI14;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI14');
		}
		return facetIndex;
	}
	get GSI15(): FacetIndex<T> {
		const facetIndex = this._GSI15;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI15');
		}
		return facetIndex;
	}
	get GSI16(): FacetIndex<T> {
		const facetIndex = this._GSI16;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI16');
		}
		return facetIndex;
	}
	get GSI17(): FacetIndex<T> {
		const facetIndex = this._GSI17;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI17');
		}
		return facetIndex;
	}
	get GSI18(): FacetIndex<T> {
		const facetIndex = this._GSI18;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI18');
		}
		return facetIndex;
	}
	get GSI19(): FacetIndex<T> {
		const facetIndex = this._GSI19;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI19');
		}
		return facetIndex;
	}
	get GSI20(): FacetIndex<T> {
		const facetIndex = this._GSI20;
		if (!facetIndex) {
			throw new Error('There is no configuration defined for GSI20');
		}
		return facetIndex;
	}
}

class FacetIndex<T> {
	#index: Index;
	#facet: Facet<T>;
	#PK: KeyConfiguration<T>;
	#SK: KeyConfiguration<T>;

	constructor(
		index: Index,
		facet: Facet<T>,
		keyConfig: IndexKeyConfiguration<T>,
	) {
		this.#index = index;
		this.#facet = facet;
		this.#PK = keyConfig.PK;
		this.#SK = keyConfig.SK;
	}

	/**
	 * Construct the partition key
	 */
	pk(model: T, shard?: number) {
		return buildKey(this.#PK, model, this.#facet.delimiter, shard);
	}

	/**
	 * Construct the sort key
	 */
	sk(model: T, shard?: number) {
		return buildKey(this.#SK, model, this.#facet.delimiter, shard);
	}
}
