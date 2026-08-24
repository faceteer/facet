/**
 * Normalize a model's TTL field value into the epoch-seconds number
 * that DynamoDB's TTL reaper recognises.
 *
 * DynamoDB only expires items whose TTL attribute is a number of epoch
 * seconds, so `Date` values and numeric strings are converted before
 * marshalling. Values that can't be normalized (non-numeric strings,
 * `NaN`, objects, `undefined`) yield `undefined`, and callers omit or
 * remove the attribute.
 *
 * Shared by `Facet.in` and `Facet.patch` so both write paths apply the
 * same rule.
 */
export function normalizeTtl(raw: unknown): number | undefined {
	let value: number | undefined;
	if (raw instanceof Date) {
		value = Math.floor(raw.getTime() / 1000);
	} else if (typeof raw === 'number') {
		value = raw;
	} else if (typeof raw === 'string') {
		value = parseInt(raw, 10);
	}
	if (value !== undefined && Number.isNaN(value)) {
		value = undefined;
	}
	return value;
}
