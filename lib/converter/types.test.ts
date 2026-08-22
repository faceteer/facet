import { describe, expect, test } from 'vitest';
import { typeOf } from './types.js';

describe('typeOf', () => {
	test('classifies primitives and built-ins', () => {
		expect(typeOf('text')).toBe('String');
		expect(typeOf(42)).toBe('Number');
		expect(typeOf(true)).toBe('Boolean');
		expect(typeOf(null)).toBe('null');
		expect(typeOf(undefined)).toBe('undefined');
		expect(typeOf(new Date())).toBe('Date');
		expect(typeOf({})).toBe('Object');
		expect(typeOf([])).toBe('Array');
		expect(typeOf(new Set())).toBe('Set');
	});

	test('classifies buffer-like values as Binary', () => {
		expect(typeOf(Buffer.from('abc'))).toBe('Binary');
		expect(typeOf(new Uint8Array([1]))).toBe('Binary');
		expect(typeOf(new Float64Array([1.5]))).toBe('Binary');
		expect(typeOf(new DataView(new ArrayBuffer(1)))).toBe('Binary');
		expect(typeOf(new ArrayBuffer(1))).toBe('Binary');
	});

	test('classifies constructor-less objects as Object', () => {
		expect(typeOf(Object.create(null))).toBe('Object');
	});

	test('classifies values with no constructor and no object type as undefined', () => {
		const bareFunction = () => undefined;
		Object.setPrototypeOf(bareFunction, null);
		expect(typeOf(bareFunction)).toBe('undefined');
	});

	test('classifies classes named after binary types by constructor name', () => {
		class File {
			name = 'not-a-real-file';
		}
		expect(typeOf(new File())).toBe('Binary');
	});

	test('falls back to the constructor string when name is not an own property', () => {
		// A constructor with no own `name` property exercises the
		// function-source fallback in typeName.
		const namelessFunction = function named() {
			return undefined;
		};
		Reflect.deleteProperty(namelessFunction, 'name');
		expect(typeOf({ constructor: namelessFunction })).toBe('named');

		// A non-function constructor has no function source to match, so
		// the raw string is returned.
		expect(typeOf({ constructor: {} })).toBe('[object Object]');
	});
});
