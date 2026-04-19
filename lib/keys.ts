import { crcShard } from './hash/crc-shard';

export type Keys<T> = T extends T ? keyof T : never;

/**
 * The `Keys<T>` whose values are primitives that serialise cleanly into
 * a hash input — `string | number | bigint | boolean`, plus `undefined`
 * so optional fields qualify. Used to constrain
 * {@link ShardConfiguration.keys}: a field whose value is an object,
 * array, or `Date` would otherwise silently contribute `[object Object]`
 * or a timezone-dependent string to the hash input, producing a
 * deterministic but opaque shard id.
 */
export type PrimitiveShardKey<T> = {
	[K in Keys<T>]: [T[K]] extends [
		string | number | bigint | boolean | undefined,
	]
		? K
		: never;
}[Keys<T>];

/**
 * How to shard a key into multiple groups
 */
export interface ShardConfiguration<T> {
	keys: PrimitiveShardKey<T>[];
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

/**
 * How to build a composite key from an object
 */
export interface KeyConfiguration<T, U extends Keys<T>> {
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
 * Build a composite primary or sort key based on the key
 * configuration and a model
 */
export function buildKey<T, U extends Keys<T>>(
	keyConfig: KeyConfiguration<T, U>,
	model: Partial<T>,
	delimiter: string,
	shard?: number | null,
) {
	const compositeKey: string[] = [keyConfig.prefix];

	/**
	 * Calculate the shard ID for a composite key
	 */
	if (keyConfig.shard && shard !== null) {
		/**
		 * If the shard was passed in to the `buildKey` function
		 * we're mostly likely building a key to query with. A
		 * truthy check would silently treat `shard === 0` as
		 * "unspecified" and fall through to the hash path.
		 */
		if (shard !== undefined) {
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
