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
	});

	test('throws on an empty set without convertEmptyValues', () => {
		expect(() => marshall({ empty: new Set() })).toThrow(/empty Set/);
	});

	test('throws on an unrecognized attribute value', () => {
		expect(() =>
			unmarshall({ bad: { $unknown: ['whatever', 1] } as never }),
		).toThrow(/Unrecognized DynamoDB attribute value/);
	});
});
