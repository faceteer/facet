/**
 * Build a Dynamo DB `ProjectionExpression` from a set of attribute names.
 *
 * Placeholder names use the `#p0, #p1, ...` prefix, which is reserved for
 * projections. Key conditions use `#PK`/`#SK`, and
 * `@faceteer/expression-builder` uses `#C_<hex>` for conditions and
 * `#F_<hex>` for filters, so the namespaces do not collide when merged
 * into the same `ExpressionAttributeNames` map.
 */
export function buildProjectionExpression(keys: readonly PropertyKey[]): {
	expression: string;
	names: Record<string, string>;
} {
	const unique = Array.from(new Set(keys.map((key) => String(key))));
	const names: Record<string, string> = {};
	const parts: string[] = [];
	unique.forEach((key, index) => {
		const placeholder = `#p${index}`;
		names[placeholder] = key;
		parts.push(placeholder);
	});
	return { expression: parts.join(', '), names };
}
