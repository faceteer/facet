import { describe, expect, test } from 'vitest';
import { mapWithConcurrency } from './concurrency.js';
import { wait } from './wait.js';

describe('mapWithConcurrency', () => {
	test('never runs more than `limit` workers at once', async () => {
		let inFlight = 0;
		let peak = 0;
		const items = Array.from({ length: 20 }, (_, i) => i);

		await mapWithConcurrency(items, 3, async () => {
			inFlight += 1;
			peak = Math.max(peak, inFlight);
			await wait(5);
			inFlight -= 1;
		});

		expect(peak).toBe(3);
	});

	test('preserves input order in the output, even with jittered worker timings', async () => {
		const items = Array.from({ length: 10 }, (_, i) => i);
		const results = await mapWithConcurrency(items, 4, async (item) => {
			await wait(Math.random() * 10);
			return item * 2;
		});

		expect(
			results.map((r) => (r.status === 'fulfilled' ? r.value : null)),
		).toEqual([0, 2, 4, 6, 8, 10, 12, 14, 16, 18]);
	});

	test('rejections are reported per-slot and do not halt peers', async () => {
		const items = [1, 2, 3, 4, 5];
		const results = await mapWithConcurrency(items, 2, async (item) => {
			await wait(1);
			if (item === 3) {
				throw new Error(`boom ${item}`);
			}
			return item;
		});

		expect(results).toHaveLength(5);
		expect(results[0]).toEqual({ status: 'fulfilled', value: 1 });
		expect(results[2].status).toBe('rejected');
		expect(results[4]).toEqual({ status: 'fulfilled', value: 5 });
	});

	test('limit > items.length does not spawn idle runners', async () => {
		let started = 0;
		const results = await mapWithConcurrency([1, 2], 100, async (item) => {
			started += 1;
			return item;
		});

		expect(started).toBe(2);
		expect(results).toEqual([
			{ status: 'fulfilled', value: 1 },
			{ status: 'fulfilled', value: 2 },
		]);
	});

	test('empty input resolves immediately to []', async () => {
		const results = await mapWithConcurrency([], 8, async () => {
			throw new Error('should not run');
		});
		expect(results).toEqual([]);
	});
});
