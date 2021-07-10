import { crcShard } from './hash/crc-shard';

/**
 * How to shard a key into multiple groups
 */
export interface ShardConfiguration<T> {
	keys: (keyof T)[];
	count: number;
}

export enum Index {
	GSI1 = 'GSI1',
	GSI2 = 'GSI2',
	GSI3 = 'GSI3',
	GSI4 = 'GSI4',
	GSI5 = 'GSI5',
	GSI6 = 'GSI6',
	GSI7 = 'GSI7',
	GSI8 = 'GSI8',
	GSI9 = 'GSI9',
	GSI10 = 'GSI10',
	GSI11 = 'GSI11',
	GSI12 = 'GSI12',
	GSI13 = 'GSI13',
	GSI14 = 'GSI14',
	GSI15 = 'GSI15',
	GSI16 = 'GSI16',
	GSI17 = 'GSI17',
	GSI18 = 'GSI18',
	GSI19 = 'GSI19',
	GSI20 = 'GSI20',
}

export const PK = 'PK';
export const SK = 'SK';

export const IndexSet: Set<Index> = new Set([
	Index.GSI1,
	Index.GSI2,
	Index.GSI3,
	Index.GSI4,
	Index.GSI5,
	Index.GSI6,
	Index.GSI7,
	Index.GSI8,
	Index.GSI9,
	Index.GSI10,
	Index.GSI11,
	Index.GSI12,
	Index.GSI13,
	Index.GSI14,
	Index.GSI15,
	Index.GSI16,
	Index.GSI17,
	Index.GSI18,
	Index.GSI19,
	Index.GSI20,
]);

export const IndexPrivatePropertyMap = {
	GSI1: '_GSI1',
	GSI2: '_GSI2',
	GSI3: '_GSI3',
	GSI4: '_GSI4',
	GSI5: '_GSI5',
	GSI6: '_GSI6',
	GSI7: '_GSI7',
	GSI8: '_GSI8',
	GSI9: '_GSI9',
	GSI10: '_GSI10',
	GSI11: '_GSI11',
	GSI12: '_GSI12',
	GSI13: '_GSI13',
	GSI14: '_GSI14',
	GSI15: '_GSI15',
	GSI16: '_GSI16',
	GSI17: '_GSI17',
	GSI18: '_GSI18',
	GSI19: '_GSI19',
	GSI20: '_GSI20',
} as const;

/**
 * Type guard to make sure a string is actually an Index
 */
export function isIndex(indexName: string): indexName is Index {
	/**
	 * Kind of dumb. Since `IndexSet` has it's typing as `Set<Index>` you
	 * can only pass in an `Index` to `IndexSet.has()`.
	 *
	 * But obviously I want to check and see if any arbitrary string is
	 * in the set so I'm forcing the type here
	 */
	return IndexSet.has(indexName as Index);
}

/**
 * How to build a composite key from an object
 */
export interface KeyConfiguration<T, U extends keyof T> {
	/**
	 * An array of object keys that will be used
	 * to create the composite key.
	 *
	 * If a key is specified that is optional, that
	 * value will be excluded from the composite key
	 */
	keys: U[];
	/**
	 * What to prefix the composite key with.
	 */
	prefix: string;
	/**
	 * Optional parameter describing how to
	 * shard a key into multiple groups. The group
	 * id will be prefixed at the beginning of the
	 */
	shard?: ShardConfiguration<T>;
}

export const IndexKeyNameMap = {
	GSI1: { PK: 'GSI1PK', SK: 'GSI1SK' },
	GSI2: { PK: 'GSI2PK', SK: 'GSI2SK' },
	GSI3: { PK: 'GSI3PK', SK: 'GSI3SK' },
	GSI4: { PK: 'GSI4PK', SK: 'GSI4SK' },
	GSI5: { PK: 'GSI5PK', SK: 'GSI5SK' },
	GSI6: { PK: 'GSI6PK', SK: 'GSI6SK' },
	GSI7: { PK: 'GSI7PK', SK: 'GSI7SK' },
	GSI8: { PK: 'GSI8PK', SK: 'GSI8SK' },
	GSI9: { PK: 'GSI9PK', SK: 'GSI9SK' },
	GSI10: { PK: 'GSI10PK', SK: 'GSI10SK' },
	GSI11: { PK: 'GSI11PK', SK: 'GSI11SK' },
	GSI12: { PK: 'GSI12PK', SK: 'GSI12SK' },
	GSI13: { PK: 'GSI13PK', SK: 'GSI13SK' },
	GSI14: { PK: 'GSI14PK', SK: 'GSI14SK' },
	GSI15: { PK: 'GSI15PK', SK: 'GSI15SK' },
	GSI16: { PK: 'GSI16PK', SK: 'GSI16SK' },
	GSI17: { PK: 'GSI17PK', SK: 'GSI17SK' },
	GSI18: { PK: 'GSI18PK', SK: 'GSI18SK' },
	GSI19: { PK: 'GSI19PK', SK: 'GSI19SK' },
	GSI20: { PK: 'GSI20PK', SK: 'GSI20SK' },
} as const;

/**
 * Partition and sort key definitions for a
 * Global Secondary Index
 */
export interface IndexKeyConfiguration<
	T,
	P extends keyof T,
	K extends keyof T,
> {
	PK: KeyConfiguration<T, P>;
	SK: KeyConfiguration<T, K>;
}

/**
 * Optional configuration for how to build
 * the partition and sort keys for Global
 * Secondary Indexes.
 */
export interface IndexKeyOptions<
	T,
	GSI1PK extends keyof T = never,
	GSI1SK extends keyof T = never,
	GSI2PK extends keyof T = never,
	GSI2SK extends keyof T = never,
	GSI3PK extends keyof T = never,
	GSI3SK extends keyof T = never,
	GSI4PK extends keyof T = never,
	GSI4SK extends keyof T = never,
	GSI5PK extends keyof T = never,
	GSI5SK extends keyof T = never,
	GSI6PK extends keyof T = never,
	GSI6SK extends keyof T = never,
	GSI7PK extends keyof T = never,
	GSI7SK extends keyof T = never,
	GSI8PK extends keyof T = never,
	GSI8SK extends keyof T = never,
	GSI9PK extends keyof T = never,
	GSI9SK extends keyof T = never,
	GSI10PK extends keyof T = never,
	GSI10SK extends keyof T = never,
	GSI11PK extends keyof T = never,
	GSI11SK extends keyof T = never,
	GSI12PK extends keyof T = never,
	GSI12SK extends keyof T = never,
	GSI13PK extends keyof T = never,
	GSI13SK extends keyof T = never,
	GSI14PK extends keyof T = never,
	GSI14SK extends keyof T = never,
	GSI15PK extends keyof T = never,
	GSI15SK extends keyof T = never,
	GSI16PK extends keyof T = never,
	GSI16SK extends keyof T = never,
	GSI17PK extends keyof T = never,
	GSI17SK extends keyof T = never,
	GSI18PK extends keyof T = never,
	GSI18SK extends keyof T = never,
	GSI19PK extends keyof T = never,
	GSI19SK extends keyof T = never,
	GSI20PK extends keyof T = never,
	GSI20SK extends keyof T = never,
> {
	GSI1?: IndexKeyConfiguration<T, GSI1PK, GSI1SK>;
	GSI2?: IndexKeyConfiguration<T, GSI2PK, GSI2SK>;
	GSI3?: IndexKeyConfiguration<T, GSI3PK, GSI3SK>;
	GSI4?: IndexKeyConfiguration<T, GSI4PK, GSI4SK>;
	GSI5?: IndexKeyConfiguration<T, GSI5PK, GSI5SK>;
	GSI6?: IndexKeyConfiguration<T, GSI6PK, GSI6SK>;
	GSI7?: IndexKeyConfiguration<T, GSI7PK, GSI7SK>;
	GSI8?: IndexKeyConfiguration<T, GSI8PK, GSI8SK>;
	GSI9?: IndexKeyConfiguration<T, GSI9PK, GSI9SK>;
	GSI10?: IndexKeyConfiguration<T, GSI10PK, GSI10SK>;
	GSI11?: IndexKeyConfiguration<T, GSI11PK, GSI11SK>;
	GSI12?: IndexKeyConfiguration<T, GSI12PK, GSI12SK>;
	GSI13?: IndexKeyConfiguration<T, GSI13PK, GSI13SK>;
	GSI14?: IndexKeyConfiguration<T, GSI14PK, GSI14SK>;
	GSI15?: IndexKeyConfiguration<T, GSI15PK, GSI15SK>;
	GSI16?: IndexKeyConfiguration<T, GSI16PK, GSI16SK>;
	GSI17?: IndexKeyConfiguration<T, GSI17PK, GSI17SK>;
	GSI18?: IndexKeyConfiguration<T, GSI18PK, GSI18SK>;
	GSI19?: IndexKeyConfiguration<T, GSI19PK, GSI19SK>;
	GSI20?: IndexKeyConfiguration<T, GSI20PK, GSI20SK>;
}
/**
 * Build a composite primary or sort key based on the key
 * configuration and a model
 */
export function buildKey<T, U extends keyof T>(
	keyConfig: KeyConfiguration<T, U>,
	model: Partial<T>,
	delimiter: string,
	shard?: number,
) {
	const compositeKey: string[] = [keyConfig.prefix];

	/**
	 * Calculate the shard ID for a composite key
	 */
	if (keyConfig.shard) {
		/**
		 * If the shard was passed in to the `buildKey` function
		 * we're mostly likely building a key to query with.
		 */
		if (shard) {
			const padLength = (keyConfig.shard.count - 1).toString(16).length;
			compositeKey.push(shard.toString(16).padStart(padLength, '0'));
		} else {
			const valuesToHash = [];
			for (const key of keyConfig.shard.keys) {
				const value = model[key];
				if (value != undefined) {
					valuesToHash.push(value);
				}
			}

			const keyToHash = valuesToHash.join('');
			const shardId = crcShard(keyToHash, keyConfig.shard.count);
			compositeKey.push(shardId);
		}
	}

	for (const key of keyConfig.keys) {
		const value = model[key];
		/**
		 * We only want to use a value in a composite
		 * key if the value is a string, number, or boolean
		 * literal. We don't want [object Object] to show
		 * up in our keys
		 */
		switch (typeof value) {
			case 'bigint':
			case 'boolean':
			case 'number':
			case 'string':
				compositeKey.push(`${value}`);
				break;
			default:
				/**
				 * Store any dates as ISO strings
				 */
				if (value instanceof Date) {
					compositeKey.push(value.toISOString());
				}
				break;
		}
	}

	return compositeKey.join(delimiter);
}
