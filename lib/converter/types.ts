/**
 * Determine which marshalling branch a JavaScript value belongs to.
 *
 * Returns the constructor name for object values (`'Object'`, `'Array'`,
 * `'Set'`, ...), `'Binary'` for buffer-like values, and the primitive
 * wrapper names (`'String'`, `'Number'`, `'Boolean'`) for primitives.
 */
export function typeOf(data: unknown): string {
	if (data instanceof Date) {
		return 'Date';
	}
	if (data === null) {
		return 'null';
	}
	if (data === undefined) {
		return 'undefined';
	}
	if (isBinary(data)) {
		return 'Binary';
	}
	const constructor = (data as { constructor?: unknown }).constructor;
	if (constructor) {
		return typeName(constructor);
	}
	if (typeof data === 'object') {
		// The result of Object.create(null), hence the absence of a
		// defined constructor
		return 'Object';
	}
	return 'undefined';
}

const binaryTypes = [
	'Buffer',
	'File',
	'Blob',
	'ArrayBuffer',
	'DataView',
	'Int8Array',
	'Uint8Array',
	'Uint8ClampedArray',
	'Int16Array',
	'Uint16Array',
	'Int32Array',
	'Uint32Array',
	'Float32Array',
	'Float64Array',
];

function isBinary(data: unknown): boolean {
	if (Buffer.isBuffer(data)) {
		return true;
	}
	if (data === undefined || !(data as { constructor?: unknown }).constructor) {
		return false;
	}
	for (const type of binaryTypes) {
		if (isType(data, type)) {
			return true;
		}
		if (typeName((data as { constructor: unknown }).constructor) === type) {
			return true;
		}
	}

	return false;
}

function typeName(type: unknown): string {
	if (Object.prototype.hasOwnProperty.call(type, 'name')) {
		return (type as { name: string }).name;
	}
	const str = String(type);
	const match = /^\s*function (.+)\(/.exec(str);
	return match ? match[1] : str;
}

function isType(obj: unknown, type: string): boolean {
	return Object.prototype.toString.call(obj) === '[object ' + type + ']';
}
