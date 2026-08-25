# Changelog

All notable changes to `@faceteer/facet` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

npm dist-tags track the most recent publish in each channel: `latest` points at the newest stable release, `alpha` / `beta` / `rc` at the newest prerelease of that kind.

## [Unreleased]

### Added

- Cross-entity queries over item collections through the new `collection()` function ([#23](https://github.com/faceteer/facet/issues/23)). A collection groups facets that share a partition behind one query surface, so a single DynamoDB `Query` returns every member's records, each dispatched to its own validator and typed by the object key it was registered under (`item.type` narrows `item.record`, and `grouped.<key>` collects per member). Members wrapped in `single()` read back as `Model | undefined` instead of `Model[]`. Default collections accept any sort-key layouts and offer `list`, `listAll`, and `first`; declaring `orderBy` builds an ordered collection whose members are verified at construction to lead their sort keys with that field on a shared prefix and delimiter, which unlocks `between`, `equals`, the four comparisons, `latest(n)`, `earliest(n)`, and a `direction` option, all taking plain values of the axis field's type. The axis can also live on a GSI (`{ orderBy, index }`), which is the retrofit path for existing tables. Rows whose `facet` attribute matches no member are collected in `unmatched` (or rejected with `onUnknown: 'throw'`), and validator failures either throw (the default) or collect into `failed` with `onInvalid: 'collect'`. Construction verifies that members share one client, table, and unique facet names; each query verifies that every member's partition-key fields are supplied and that every member builds the same partition-key string.
- `Facet.patch` for partial updates without rewriting the full record ([#58](https://github.com/faceteer/facet/issues/58)). A patch issues a single DynamoDB `UpdateItem` that writes only the supplied fields and recomputes every GSI key and TTL attribute whose inputs it touches, so indexes never silently drift from the record. Key-input completeness is enforced at compile time by default: a patch that touches a key input without supplying the key's other inputs in `query` or `patch` does not compile, and the error lists one required property per missing field (`Missing key input: authorId`) on the patch argument. Shard key names are captured in `ShardConfiguration` and `KeyConfiguration` type parameters so the check covers them too. The check reads the argument types inferred at the call site, so arguments typed wider than their contents (or a facet widened to a type without its index accessors) fall back to a runtime `PatchMissingKeyInputsError` instead of a compile error. Opting into `missingKeyInputs: 'read'` trades the compile-time check for a fallback read that resolves missing inputs from the stored record, surfaced as `usedFallbackRead: true` on the response. Key inputs sourced from outside the patch are asserted in the write's condition, so a concurrent change fails the patch instead of writing a stale key. Patches never throw and never upsert. Results report through a `wasSuccessful` discriminated union: the success branch carries the validated post-patch record, and a condition failure carries the conflicting record when DynamoDB returns one. Setting a field to `undefined` removes the attribute. `Facet.patchInputs` reports which extra fields a patch of given fields needs, so requirements are discoverable in code and tooling.

## [6.1.0] - 2026-08-22

### Added

- Opt-in strongly consistent reads. Single gets, batch gets, and base-table queries accept `consistentRead: true`, which sets DynamoDB's `ConsistentRead` flag on the request (including batch-get retry requests). Index queries reject the option at compile time and with a runtime error, because DynamoDB does not support consistent reads on global secondary indexes.
- DynamoDB transaction support. Each facet exposes `transaction.put`, `transaction.delete`, `transaction.check`, and `transaction.get` builders, and the new `transactWrite` and `transactGet` functions execute the built operations atomically across facets and tables. Failures resolve with `wasSuccessful: false` instead of throwing; a canceled transaction also carries a `failures` array with one entry per operation, typed per position for array literals, including the conflicting item parsed from DynamoDB's `ReturnValuesOnConditionCheckFailure` response when one is returned ([#25](https://github.com/faceteer/facet/issues/25)).

### Changed

- `@faceteer/expression-builder` and `@faceteer/converter` are no longer dependencies — their code now lives in this package (`lib/expression/`, `lib/converter/`), leaving `@faceteer/facet` with zero runtime dependencies. The condition and filter types (`ConditionExpression`, `FilterConditionExpression`, `Condition` and its variants, `ConverterOptions`, `AttributeMap`) are now exported from the package root; the standalone packages will be deprecated.
- DynamoDB sets marshall from and unmarshall to native JavaScript `Set` objects (`SS`/`NS`/`BS` chosen by member type). The former `DynamoDBSet` wrapper class was never constructible — a bug made its constructor always throw — so set support effectively did not exist before this change. Mixed-type and empty sets throw a `TypeError`; with `convertEmptyValues` enabled, empty sets marshall as `NULL`.

### Fixed

- Conditions and filters using the `size` operator no longer fail with a `ValidationException`. The compiled expression contained an unbalanced closing parenthesis, so every request using `size` was rejected by DynamoDB.
- A condition using `in` with an empty list throws a descriptive error at compile time instead of sending `IN ()`, which DynamoDB rejects with an opaque syntax error.
- Marshalling `NaN` or `Infinity` (as a field or a set member) throws a `TypeError` instead of producing a value DynamoDB rejects at request time.
- Unmarshalling an unrecognized `AttributeValue` shape now throws a descriptive error instead of silently producing `undefined`.
- Marshalling a list containing `undefined` or a function now throws a `TypeError` instead of producing a malformed request.

### Infrastructure

- Test coverage raised to 100% (statements, branches, functions, lines). New tests pin batch write/get failure and retry handling, `validateInput`, TTL edge cases, shard-hash placement, raw-string sort keys, and value-comparing conditions.
- Every condition operator now executes against DynamoDB Local in the integration suite (`in`, `begins_with`, `contains`, `between`, all comparators, and nested `AND`/`OR`/`NOT`), so a malformed compiled expression fails tests rather than shipping — the gap that let the `size` bug through.
- `prettier`, `typescript`, and `eslint` are pinned to exact versions. `package-lock.json` is gitignored, so caret ranges made CI resolve different versions than local machines; upgrades now go through an explicit commit.
- The repository now doubles as a Claude Code plugin marketplace. The `faceteer-facet` plugin ships a skill that teaches coding agents the library's key-construction and error-handling conventions; install it with `/plugin marketplace add faceteer/facet` followed by `/plugin install faceteer-facet@faceteer`.

## [6.0.0] - 2026-04-20

Stable promotion of `6.0.0-alpha.0`. No behavioral changes to library code; see the `6.0.0-alpha.0` entry for the full set of v6 changes.

### Infrastructure

- Regenerated typedoc output for v6 and added a `docs` npm script.
- Bumped `actions/checkout` and `actions/setup-node` from v4 to v6 in CI.

## [6.0.0-alpha.0] - 2026-04-20

First publish of the v6 line. Breaking changes from v5 touch module format, the DynamoDB client wiring, the cursor format, and the public API surface — migrating existing v5 consumers is not a drop-in upgrade.

### Changed (breaking)

- Package is now **ESM** (`"type": "module"`) with a single `exports` entry. CJS consumers on Node ≥ 22.12 can still `require()` via `require(esm)`; older Node cannot.
- Minimum Node bumped to **`>=22.12.0`** (the first LTS where `require(esm)` is unflagged).
- DynamoDB client must be passed in explicitly on `connection.dynamoDb`. The library no longer constructs a client internally; consumers own credential chains, endpoint overrides, and retry config. `@aws-sdk/client-dynamodb` is also a peer dep now, not a direct dep.
- **Cursor format replaced** (CBOR → custom binary tuples, base64url-encoded). Cursors minted by v5 will not decode in v6. Pagination state stored from v5 callers is not forward-compatible.
- Cursors now use URL-safe **base64url**, not standard base64 ([#48](https://github.com/faceteer/facet/issues/48)).
- **Public API fenced**: subpath imports are no longer resolvable. Only the root barrel (`import { Facet } from '@faceteer/facet'`) is supported ([#55](https://github.com/faceteer/facet/issues/55)).
- Reserved attribute names (`PK`, `SK`, `facet`, `ttl`, `GSI*PK`/`GSI*SK`) are now rejected at both the type level (via `WithoutReservedAttributes`) and at runtime in the constructor. v5 silently clobbered colliding model fields ([#54](https://github.com/faceteer/facet/issues/54)).
- Query sort-key arguments are now typed against the **active sort key** — base-table queries used to accept only GSI SK shapes ([#41](https://github.com/faceteer/facet/issues/41)).
- `ShardConfiguration.keys` is restricted to primitive-typed fields at the type level; non-primitive fields are no longer silently hashed as `[object Object]` ([#44](https://github.com/faceteer/facet/issues/44)).

### Added

- **`PutOptions.concurrency` / `DeleteOptions.concurrency` / `GetOptions.concurrency`** — cap outer fan-out on batch put/delete/get. Defaults to 8, tuned to sit just above new-on-demand starting capacity. Fixes unbounded fan-out that triggered throttling storms on large batches ([#47](https://github.com/faceteer/facet/issues/47)).
- **Projected reads via `select`** on `Facet.get` (single + batch) and every `PartitionQuery` operator. Returns a `Pick<T, K | PK | SK>` and validates through a new `pickValidator` factory. PK/SK fields are always re-projected, even if omitted from `select`.
- **`Facet.addIndex(..., { alias })`** — register a human-readable alias (`facet.PagePostStatus.query(...)`) alongside the raw `GSIn` accessor. Type-level collision check prevents alias/index-name overlap.
- Tests for projected reads across `PartitionQuery` methods (equals/beginsWith/between/etc.).
- Root `index.ts` barrel widened to cover the full public surface and typedoc-validated.
- Extensive hover docs on `Facet`, including guidance on composite sort-key patterns.

### Fixed

- Batch put/delete no longer silently misreport unprocessed items as successful. The final-failure loop now iterates the post-retry `UnprocessedItems`, not the pre-retry snapshot ([#31](https://github.com/faceteer/facet/issues/31), [#35](https://github.com/faceteer/facet/issues/35)).
- Delete batch retry now checks the correct `WriteRequest` shape, so failure reporting actually fires ([#34](https://github.com/faceteer/facet/issues/34)).
- `Facet.out()` strips synthetic `ttl` even when the facet has no registered indexes, and the delete-ttl step no longer runs N times per read ([#33](https://github.com/faceteer/facet/issues/33), [#49](https://github.com/faceteer/facet/issues/49)).
- TTL attributes are now written as the DynamoDB `N` type with epoch-seconds value, not the raw `S`-typed ISO string. Date-typed TTL fields were silently broken ([#32](https://github.com/faceteer/facet/issues/32)).
- `deleteSingleItem` no longer sends empty `ExpressionAttributeNames` / `ExpressionAttributeValues` maps, which DynamoDB rejects with `ValidationException`. Conditional deletes without value placeholders now succeed ([#38](https://github.com/faceteer/facet/issues/38)).
- `SK.shard` configuration is no longer silently dropped by `Facet.sk()` ([#37](https://github.com/faceteer/facet/issues/37)).
- `buildKey()` now honours an explicit `shard: 0`; v5 treated it as unspecified ([#36](https://github.com/faceteer/facet/issues/36)).
- `addIndex` rejects silent overwrite of an already-registered GSI slot or alias ([#53](https://github.com/faceteer/facet/issues/53)).
- The `WithoutReservedAttributes<T>` constraint no longer structurally rejects every concrete `T`: it maps over `keyof T` so only colliding fields become `never`.
- Test helpers now branch on SDK v3 `error.name` instead of v2 `error.code`; the reset path was never firing ([#40](https://github.com/faceteer/facet/issues/40)).
- Removed duplicate `this.#PK = PK` assignment in the `Facet` constructor ([#39](https://github.com/faceteer/facet/issues/39)).
- `tsconfig.test.json` no longer inherits the `exclude` pattern that dropped test files from the default type-check graph.

### Removed

- **`crc-32` npm dependency** — shard hashing now uses `node:zlib.crc32` (available from Node 20 onward).
- Dead-code exports from `lib/keys.ts`: `IndexPrivatePropertyMap`, `isIndex`, `IndexKeyConfiguration`, `IndexKeyOptions` ([#42](https://github.com/faceteer/facet/issues/42)).

### Infrastructure

- Migrated test runner from **Jest → Vitest**; CI runs against Node 20, 22, and 24 with DynamoDB Local as a service container.
- Upgraded to **TypeScript 6** with strict flat ESLint v10 + Prettier 3 configuration.
- Split library and test tsconfigs so published builds no longer contain test files.
- Widened `@aws-sdk/client-dynamodb` peer dep range to `^3.0.0`.
- Shared VS Code workspace setting makes IDE auto-imports insert the required `.js` extension for NodeNext module resolution.

[unreleased]: https://github.com/faceteer/facet/compare/v6.1.0...HEAD
[6.1.0]: https://github.com/faceteer/facet/compare/v6.0.0...v6.1.0
[6.0.0]: https://github.com/faceteer/facet/compare/v6.0.0-alpha.0...v6.0.0
[6.0.0-alpha.0]: https://github.com/faceteer/facet/releases/tag/v6.0.0-alpha.0
