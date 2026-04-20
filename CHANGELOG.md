# Changelog

All notable changes to `@faceteer/facet` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

npm dist-tags track the most recent publish in each channel: `latest` points at the newest stable release, `alpha` / `beta` / `rc` at the newest prerelease of that kind.

## [Unreleased]

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

[unreleased]: https://github.com/faceteer/facet/compare/v6.0.0...HEAD
[6.0.0]: https://github.com/faceteer/facet/compare/v6.0.0-alpha.0...v6.0.0
[6.0.0-alpha.0]: https://github.com/faceteer/facet/releases/tag/v6.0.0-alpha.0
