import { normalizeTtl } from './ttl.js';

describe('normalizeTtl', () => {
	test('converts a Date to epoch seconds, flooring sub-second precision', () => {
		expect(normalizeTtl(new Date('2030-01-01T00:00:00.000Z'))).toBe(1893456000);
		expect(normalizeTtl(new Date(1893456000_500))).toBe(1893456000);
	});

	test('passes numbers through unchanged', () => {
		expect(normalizeTtl(1893456000)).toBe(1893456000);
		expect(normalizeTtl(0)).toBe(0);
	});

	test('parses numeric strings', () => {
		expect(normalizeTtl('1893456000')).toBe(1893456000);
	});

	test('yields undefined for values the TTL reaper cannot use', () => {
		expect(normalizeTtl('soon')).toBeUndefined();
		expect(normalizeTtl(NaN)).toBeUndefined();
		expect(normalizeTtl(new Date('not a date'))).toBeUndefined();
		expect(normalizeTtl(undefined)).toBeUndefined();
		expect(normalizeTtl(null)).toBeUndefined();
		expect(normalizeTtl({ at: 5 })).toBeUndefined();
	});
});
