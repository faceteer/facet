import type { AttributeValue } from '@aws-sdk/client-dynamodb';
import type { ConverterOptions } from './converter-options.js';
import { typeOf } from './types.js';

export type AttributeMap = Record<string, AttributeValue>;

/**
 * Convert a JavaScript value to its equivalent DynamoDB AttributeValue type.
 *
 * Returns `undefined` for values DynamoDB has no representation for
 * (`undefined` and functions), which callers omit from maps.
 *
 * See {@link marshall} to convert entire records rather than individual
 * attributes.
 */
export function toAttributeValue(
	data: unknown,
	options: ConverterOptions = {},
): AttributeValue | undefined {
	if (data instanceof Set) {
		return formatSet(data, options);
	}
	const type = typeOf(data);
	if (type === 'Date') {
		return formatDate(data as Date, options);
	}
	if (type === 'Object') {
		return formatMap(data as Record<string, unknown>, options);
	} else if (type === 'Array') {
		return formatList(data as unknown[], options);
	} else if (type === 'String') {
		if ((data as string).length === 0 && options.convertEmptyValues) {
			return { NULL: true };
		}
		return { S: data as string };
	} else if (type === 'Number' || type === 'NumberValue') {
		return { N: String(data) };
	} else if (type === 'Binary') {
		if ((data as Uint8Array).length === 0 && options.convertEmptyValues) {
			return { NULL: true };
		}
		return { B: data as Uint8Array };
	} else if (type === 'Boolean') {
		return { BOOL: data as boolean };
	} else if (type === 'null') {
		return { NULL: true };
	} else if (type !== 'undefined' && type !== 'Function') {
		// This value has a custom constructor
		return formatMap(data as Record<string, unknown>, options);
	}
	return undefined;
}

/**
 * Convert a JavaScript object into a DynamoDB record.
 *
 * @example
 * ```ts
 * const marshalled = marshall({
 * 	string: 'foo',
 * 	list: ['fizz', 'buzz', 'pop'],
 * 	map: { nestedMap: { key: 'value' } },
 * 	number: 123,
 * 	nullValue: null,
 * 	boolValue: true,
 * 	stringSet: new Set(['foo', 'bar', 'baz']),
 * });
 * ```
 */
export function marshall(
	data: Record<string, unknown>,
	options?: ConverterOptions,
): AttributeMap {
	return toAttributeValue(data, options)?.M ?? {};
}

/**
 * Convert a DynamoDB AttributeValue to its equivalent JavaScript type.
 *
 * Throws on an AttributeValue whose key is not one of the ten DynamoDB
 * data types, rather than silently returning `undefined`.
 *
 * See {@link unmarshall} to convert entire records rather than individual
 * attributes.
 */
export function fromAttributeValue(
	data: AttributeValue,
	options: ConverterOptions = {},
): unknown {
	if (data.M !== undefined) {
		const map: Record<string, unknown> = {};
		for (const [key, value] of Object.entries(data.M)) {
			map[key] = fromAttributeValue(value, options);
		}
		return map;
	}
	if (data.L !== undefined) {
		return data.L.map((item) => fromAttributeValue(item, options));
	}
	if (data.SS !== undefined) {
		return new Set(data.SS);
	}
	if (data.NS !== undefined) {
		return new Set(
			data.NS.map((value) => convertNumber(value, options.wrapNumbers)),
		);
	}
	if (data.BS !== undefined) {
		return new Set(data.BS);
	}
	if (data.S !== undefined) {
		return data.S;
	}
	if (data.N !== undefined) {
		if (data.N.length === 0) {
			return null;
		}
		return convertNumber(data.N, options.wrapNumbers);
	}
	if (data.B !== undefined) {
		return data.B;
	}
	if (data.BOOL !== undefined) {
		return data.BOOL;
	}
	if (data.NULL !== undefined) {
		return null;
	}
	throw new Error(
		`Unrecognized DynamoDB attribute value with key(s): ${Object.keys(
			data,
		).join(', ')}`,
	);
}

/**
 * Convert a DynamoDB record into a JavaScript object.
 */
export function unmarshall(
	data: AttributeMap,
	options?: ConverterOptions,
): unknown {
	return fromAttributeValue({ M: data }, options);
}

function formatList(
	data: unknown[],
	options?: ConverterOptions,
): AttributeValue {
	const list: AttributeValue[] = [];
	for (const item of data) {
		const converted = toAttributeValue(item, options);
		if (converted === undefined) {
			throw new TypeError(
				`List members of type ${typeOf(item)} cannot be marshalled`,
			);
		}
		list.push(converted);
	}
	return { L: list };
}

function formatDate(data: Date, options: ConverterOptions): AttributeValue {
	if (options.dateFormat === 'unix') {
		return {
			S: `${Math.floor(data.getTime() / 1000)}`,
		};
	}
	return {
		S: data.toISOString(),
	};
}

function convertNumber(value: string, wrapNumbers?: boolean): unknown {
	return wrapNumbers ? new NumberValue(value) : Number(value);
}

function formatMap(
	data: Record<string, unknown>,
	options?: ConverterOptions,
): AttributeValue {
	const map: Record<string, AttributeValue> = {};
	for (const key of Object.keys(data)) {
		const formatted = toAttributeValue(data[key], options);
		if (formatted !== undefined) {
			map[key] = formatted;
		}
	}
	return { M: map };
}

/**
 * Marshall a native JavaScript Set into a DynamoDB set by element type:
 * all-string values become `SS`, numbers become `NS`, and binary values
 * become `BS`. Mixed-type and empty sets have no DynamoDB representation
 * and throw, except that an empty set becomes `NULL` when
 * `convertEmptyValues` is enabled.
 */
function formatSet(
	data: Set<unknown>,
	options: ConverterOptions,
): AttributeValue {
	let values = [...data];
	if (options.convertEmptyValues) {
		values = values.filter(
			(value) => typeOf(value) !== 'Binary' || (value as Uint8Array).length > 0,
		);
		values = values.filter(
			(value) => typeof value !== 'string' || value.length > 0,
		);
		if (values.length === 0) {
			return { NULL: true };
		}
	}
	if (values.length === 0) {
		throw new TypeError(
			'Cannot marshall an empty Set: DynamoDB sets must have at least one member. Enable convertEmptyValues to store empty sets as NULL.',
		);
	}

	const memberTypes = new Set(values.map((value) => typeOf(value)));
	if (memberTypes.size === 1 && memberTypes.has('String')) {
		return { SS: values as string[] };
	}
	if (
		[...memberTypes].every(
			(type) => type === 'Number' || type === 'NumberValue',
		)
	) {
		return { NS: values.map((value) => String(value)) };
	}
	if (memberTypes.size === 1 && memberTypes.has('Binary')) {
		return { BS: values as Uint8Array[] };
	}
	throw new TypeError(
		`Set members must be all strings, all numbers, or all binary values; got: ${[
			...memberTypes,
		].join(', ')}`,
	);
}

/**
 * Wraps a number so it round-trips through marshalling without losing
 * precision, for numbers outside JavaScript's safe integer range.
 */
class NumberValue {
	readonly #value: number | string;

	constructor(value: number | string) {
		this.#value = value;
	}

	/**
	 * Render the underlying value as a number when converting to JSON.
	 */
	toJSON(): number {
		return this.toNumber();
	}

	/**
	 * Convert the underlying value to a JavaScript number.
	 */
	toNumber(): number {
		return Number(this.#value);
	}

	/**
	 * Return a string representing the unaltered value provided to the
	 * constructor.
	 */
	toString(): string {
		return String(this.#value);
	}
}
