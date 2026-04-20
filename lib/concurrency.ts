/**
 * Default ceiling on the number of DynamoDB batch requests in flight
 * at once for `putItems`, `deleteItems`, and `getBatchItems`.
 *
 * Chosen to sit above a new on-demand table's starting capacity
 * (~4k WCU / 12k RCU) without overwhelming it: 8 × `BatchWriteItem`
 * (25 items, ~30 ms p50) ≈ 6.7k WCU/s sustained. Warm tables scale
 * into it via on-demand's adaptive-capacity doubling. Well under the
 * AWS SDK v3 default `maxSockets = 50`, so the connection pool is
 * not the bottleneck.
 */
export const DEFAULT_BATCH_CONCURRENCY = 8;

/**
 * Run `worker` against every item with at most `limit` invocations
 * in flight at once. Returns a stable-order `PromiseSettledResult`
 * array so callers can pick either `allSettled` or `all` semantics
 * depending on how they want to surface batch-level failures.
 */
export async function mapWithConcurrency<T, R>(
	items: readonly T[],
	limit: number,
	worker: (item: T, index: number) => Promise<R>,
): Promise<PromiseSettledResult<R>[]> {
	const results = new Array<PromiseSettledResult<R>>(items.length);
	const workerCount = Math.min(Math.max(limit, 1), items.length);
	let cursor = 0;

	const runners = Array.from({ length: workerCount }, async () => {
		while (cursor < items.length) {
			const index = cursor++;
			try {
				results[index] = {
					status: 'fulfilled',
					value: await worker(items[index], index),
				};
			} catch (reason: unknown) {
				results[index] = { status: 'rejected', reason };
			}
		}
	});

	await Promise.all(runners);
	return results;
}
