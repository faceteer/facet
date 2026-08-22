import { describe, expect, test } from 'vitest';
import { marshall, unmarshall } from './converter.js';

describe('converter.ts', () => {
	test('round-trips a record through marshall and unmarshall', () => {
		const pictureBuffer = Buffer.from(
			'iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg==',
			'base64',
		);

		const user = {
			id: 575,
			name: 'Danny',
			favorites: ['apples', 'pears'],
			lastPayment: null,
			createdAt: new Date('2021-07-09T21:44:07.015Z'),
			address: {
				streetNumber: 112,
				streetName: 'Drive',
			},
			profilePicture: pictureBuffer,
		};

		const userObject = marshall(user);

		expect(userObject).toEqual({
			id: { N: '575' },
			name: { S: 'Danny' },
			favorites: { L: [{ S: 'apples' }, { S: 'pears' }] },
			lastPayment: { NULL: true },
			createdAt: { S: '2021-07-09T21:44:07.015Z' },
			address: {
				M: { streetNumber: { N: '112' }, streetName: { S: 'Drive' } },
			},
			profilePicture: {
				B: pictureBuffer,
			},
		});

		const unmarshalled = unmarshall(userObject);

		const stringDateUser = {
			...user,
			createdAt: '2021-07-09T21:44:07.015Z',
		};

		expect(unmarshalled).toEqual(stringDateUser);
	});

	test('marshalls dates as epoch seconds with dateFormat unix', () => {
		const marshalled = marshall(
			{ createdAt: new Date('2021-07-09T21:44:07.015Z') },
			{ dateFormat: 'unix' },
		);

		expect(marshalled).toEqual({
			createdAt: { S: '1625867047' },
		});
	});

	test('converts empty strings, buffers, and sets to NULL with convertEmptyValues', () => {
		const marshalled = marshall(
			{
				emptyString: '',
				emptyBuffer: Buffer.from(''),
				emptySet: new Set<string>(),
				setOfEmptyStrings: new Set(['']),
			},
			{ convertEmptyValues: true },
		);

		expect(marshalled).toEqual({
			emptyString: { NULL: true },
			emptyBuffer: { NULL: true },
			emptySet: { NULL: true },
			setOfEmptyStrings: { NULL: true },
		});
	});

	test('round-trips numbers through NumberValue with wrapNumbers', () => {
		const bigNumber = '9007199254740993';
		const unmarshalled = unmarshall(
			{ big: { N: bigNumber } },
			{ wrapNumbers: true },
		) as Record<string, { toString(): string; toNumber(): number }>;

		expect(unmarshalled.big.toString()).toBe(bigNumber);

		const remarshalled = marshall({ big: unmarshalled.big });
		expect(remarshalled).toEqual({ big: { N: bigNumber } });
	});

	test('round-trips a string set as SS', () => {
		const marshalled = marshall({ tags: new Set(['alpha', 'beta']) });

		expect(marshalled).toEqual({ tags: { SS: ['alpha', 'beta'] } });
		expect(unmarshall(marshalled)).toEqual({
			tags: new Set(['alpha', 'beta']),
		});
	});

	test('round-trips a number set as NS', () => {
		const marshalled = marshall({ scores: new Set([1, 2.5, 3]) });

		expect(marshalled).toEqual({ scores: { NS: ['1', '2.5', '3'] } });
		expect(unmarshall(marshalled)).toEqual({
			scores: new Set([1, 2.5, 3]),
		});
	});

	test('round-trips a binary set as BS', () => {
		const members = [Buffer.from('one'), Buffer.from('two')];
		const marshalled = marshall({ blobs: new Set(members) });

		expect(marshalled).toEqual({ blobs: { BS: members } });
		expect(unmarshall(marshalled)).toEqual({ blobs: new Set(members) });
	});

	test('throws on a mixed-type set', () => {
		expect(() => marshall({ mixed: new Set(['a', 1]) })).toThrow(
			/all strings, all numbers, or all binary/,
		);
		expect(() => marshall({ mixed: new Set(['a', Buffer.from('b')]) })).toThrow(
			/all strings, all numbers, or all binary/,
		);
		expect(() => marshall({ bools: new Set([true, false]) })).toThrow(
			/all strings, all numbers, or all binary/,
		);
	});

	test('throws on a mixed-type set even when convertEmptyValues filters a member', () => {
		// The homogeneity check runs before empty-member filtering, so an
		// empty member of one type mixed with members of another type is
		// an error, not a silently truncated set.
		expect(() =>
			marshall(
				{ mixed: new Set([Buffer.from(''), 'hello']) },
				{ convertEmptyValues: true },
			),
		).toThrow(/all strings, all numbers, or all binary/);
	});

	test('marshalls empty strings and buffers literally without convertEmptyValues', () => {
		const emptyBuffer = Buffer.from('');
		expect(marshall({ s: '', b: emptyBuffer })).toEqual({
			s: { S: '' },
			b: { B: emptyBuffer },
		});
	});

	test('unix dates truncate fractional seconds toward zero', () => {
		expect(
			marshall(
				{ at: new Date('2021-07-09T21:44:07.999Z') },
				{ dateFormat: 'unix' },
			),
		).toEqual({ at: { S: '1625867047' } });
	});

	test('throws on non-finite numbers', () => {
		expect(() => marshall({ bad: NaN })).toThrow(/must be finite/);
		expect(() => marshall({ bad: Infinity })).toThrow(/must be finite/);
		expect(() => marshall({ bad: new Set([-Infinity, 5]) })).toThrow(
			/must be finite/,
		);
	});

	test('marshalls a set of wrapped numbers as NS', () => {
		const unmarshalled = unmarshall(
			{ scores: { NS: ['9007199254740993'] } },
			{ wrapNumbers: true },
		) as { scores: Set<unknown> };

		expect(marshall({ scores: unmarshalled.scores })).toEqual({
			scores: { NS: ['9007199254740993'] },
		});
	});

	test('throws on an empty set without convertEmptyValues', () => {
		expect(() => marshall({ empty: new Set() })).toThrow(/empty Set/);
	});

	test('throws on an unrecognized attribute value', () => {
		expect(() =>
			unmarshall({ bad: { $unknown: ['whatever', 1] } as never }),
		).toThrow(/Unrecognized DynamoDB attribute value/);
	});

	test('marshalls unrepresentable input to an empty map', () => {
		expect(marshall(undefined as never)).toEqual({});
	});

	test('round-trips booleans', () => {
		const marshalled = marshall({ isActive: true, isDeleted: false });

		expect(marshalled).toEqual({
			isActive: { BOOL: true },
			isDeleted: { BOOL: false },
		});
		expect(unmarshall(marshalled)).toEqual({
			isActive: true,
			isDeleted: false,
		});
	});

	test('unmarshalls an empty N value to null', () => {
		expect(unmarshall({ count: { N: '' } })).toEqual({ count: null });
	});

	test('omits undefined and function values from maps', () => {
		const marshalled = marshall({
			kept: 'value',
			missing: undefined,
			callback: () => 'nope',
		});

		expect(marshalled).toEqual({ kept: { S: 'value' } });
	});

	test('throws on a list member with no DynamoDB representation', () => {
		expect(() => marshall({ items: ['fine', undefined] })).toThrow(
			/List members of type undefined cannot be marshalled/,
		);
	});

	test('NumberValue converts back to a number via toNumber and toJSON', () => {
		const unmarshalled = unmarshall(
			{ total: { N: '42' } },
			{ wrapNumbers: true },
		) as Record<string, { toNumber(): number }>;

		expect(unmarshalled.total.toNumber()).toBe(42);
		expect(JSON.stringify(unmarshalled)).toBe('{"total":42}');
	});

	test('marshalls class instances as maps', () => {
		class Address {
			constructor(
				public streetNumber: number,
				public streetName: string,
			) {}
		}

		const marshalled = marshall({ address: new Address(112, 'Drive') });

		expect(marshalled).toEqual({
			address: {
				M: { streetNumber: { N: '112' }, streetName: { S: 'Drive' } },
			},
		});
	});

	test('marshalls constructor-less objects as maps', () => {
		const bare: Record<string, unknown> = Object.create(null) as Record<
			string,
			unknown
		>;
		bare.key = 'value';

		expect(marshall({ bare })).toEqual({
			bare: { M: { key: { S: 'value' } } },
		});
	});

	test('marshalls typed arrays and DataViews as binary', () => {
		const typedArray = new Uint8Array([1, 2, 3]);
		const view = new DataView(new ArrayBuffer(2));

		expect(marshall({ typedArray, view })).toEqual({
			typedArray: { B: typedArray },
			view: { B: view },
		});
	});

	test('filters empty binary members from sets with convertEmptyValues', () => {
		const kept = Buffer.from('kept');
		const marshalled = marshall(
			{ blobs: new Set([kept, Buffer.from('')]) },
			{ convertEmptyValues: true },
		);

		expect(marshalled).toEqual({ blobs: { BS: [kept] } });
	});
});
