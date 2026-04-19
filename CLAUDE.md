# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`@faceteer/facet` — a TypeScript helper library for DynamoDB single-table design. The package wraps `@aws-sdk/client-dynamodb` (a peer dependency) with typed `Facet` objects that compute composite keys and GSI keys from model fields.

Node >= 20. The package publishes the compiled output next to the source (`lib/**/*.js`, `index.js`, and their `.d.ts` siblings) — there is no separate `dist/` directory.

## Commands

- `npm run build` — clean then compile (`tsc -b --clean && tsc -b`). Always run both, since `prepublishOnly` and CI run this pair.
- `npm run build:clean` — remove compiled artifacts only.
- `npm run typecheck` — `tsc --noEmit`. Uses the default `tsconfig.json`, which covers the whole tree (lib + tests + scratch) with `vitest/globals` in `types`. `tsconfig.build.json` is a separate config used only by `npm run build`; it excludes tests and emits declarations.
- `npm test` — `vitest run --coverage`. Tests are integration tests that hit DynamoDB Local on `localhost:8000`, so start it first: `docker compose up -d` (see `docker-compose.yml`). Tests create a `TEST` table and reset it if it already exists.
- Run one test file: `npx vitest run lib/facet.test.ts`. Single test case: `npx vitest run -t 'Get Pages'`.
- `npm run lint` — `eslint .` using flat config in `eslint.config.mjs`. `npm run format` writes Prettier changes; `npm run format:check` verifies without writing (used in CI).
- CI (`.github/workflows/test.yml`) runs lint, format:check, build, typecheck, and tests on Node 20/22/24 against a DynamoDB Local service container and uses dummy AWS creds (`AWS_ACCESS_KEY_ID=test`, etc.).

## Architecture

The big idea: one `Facet<T, PK, SK>` represents one logical record type in a shared DynamoDB table. It owns the rules for turning a model `T` into a DynamoDB item (with synthetic `PK`/`SK`/`GSInPK`/`GSInSK` strings) and back.

- **`lib/facet.ts`** — `Facet` class. Stateful: `addIndex()` **mutates** `this`, attaching both `GSIn` and (optionally) `alias` properties that return `FacetIndex` instances. The return type is a TypeScript cast that threads index names and aliases into the Facet's type so `PostFacet.GSIPagePostStatus.query(...)` is typed end-to-end. `in(model)` marshalls to DDB + stamps all computed keys + `facet` name + optional `ttl`. `out(record)` unmarshalls, strips all synthetic keys (including every registered index's keys) and the `facet` field, then runs the validator. `validateInput` is off by default — validation only happens on read unless opted in.

- **`lib/keys.ts`** — `buildKey()` is the single source of truth for composite keys. Format: `prefix` + (optional shard id in hex, zero-padded to fit `count-1`) + each `keys[i]` value, joined by `delimiter` (default `_`). Values are stringified if primitive; `Date` becomes ISO string; anything else is omitted. There are 20 GSI slots (`GSI1`..`GSI20`) with a fixed `GSInPK`/`GSInSK` attribute naming convention — tables must pre-declare these.

- **`lib/query.ts`** — `PartitionQuery` builds `QueryInput`. `equals/greaterThan/greaterThanOrEqual/lessThan/lessThanOrEqual` all share `compare()`; `beginsWith` / `list` (== `beginsWith({})`) / `first` / `between` each build their own expression. Filters use `@faceteer/expression-builder` and are merged into the generated `ExpressionAttributeNames`/`Values`. Sort key args can also be raw strings to bypass key construction.

- **`lib/put.ts`** — Single-item put returns `PutSingleItemResponse` (with `wasSuccessful`); array put returns `PutResponse` and batches via `batchWriteItem` in chunks of 25 (DynamoDB's hard limit). Duplicate PK+SK within a batch are deduped. `UnprocessedItems` are retried up to 5 times with exponential backoff (`wait(10 * 2 ** retries)`); whatever remains after retries ends up in `failed`.

- **`lib/delete.ts`** / **`lib/get.ts`** — analogous single vs. batch paths, same 25-item batching pattern.

- **`lib/cursor.ts`** — pagination cursors encode DynamoDB's `LastEvaluatedKey` in a domain-specific binary format: `(code:u8)(len:varint)(utf8:bytes)` tuples, base64url'd. Exploits the invariants that every key value is a string and attribute names come from the fixed 42-name set (`PK`, `SK`, `GSI1PK..GSI20SK`). Opaque to callers.

- **`lib/hash/crc-shard.ts`** — CRC-32 based shard id when a `KeyConfiguration.shard` is present. On write, shard id is computed from the model's shard keys; on query, the caller passes `shard` explicitly to target one group.

### Mental model when editing

- When adding a new query operator or option, touch both `compare()`-style methods and the ones that inline their own `QueryInput` (`beginsWith`, `between`), since they don't share a single code path.
- When touching the `in`/`out` cycle, remember that `out` must strip every synthetic key for every registered index or the validator will reject the record. Adding a new synthetic attribute means updating the strip list in `Facet.out`.
- `addIndex` relies on `Object.assign(this, ...)` for both the index name and the alias — type safety is enforced via the generic return cast, not the runtime. Don't remove the `hasOwnProperty` check that guards against alias collisions.

### Assumptions about the DynamoDB table

- GSIs on the target table use `ProjectionType: ALL`. Projected reads (`select`) on an index rely on base-table attributes being present in the index. A KEYS_ONLY or INCLUDE GSI would return incomplete data for attributes it doesn't project; the library doesn't validate the table's projection type.

## Style

- Tabs, single quotes, trailing commas, semicolons (see `.prettierrc`). Prettier is the single source of truth for formatting — ESLint's `eslint-config-prettier` disables any formatting rules that would conflict.
- ESLint config is flat (`eslint.config.mjs`) and layers `@eslint/js` recommended, `typescript-eslint` strict-type-checked + stylistic-type-checked, `eslint-plugin-import-x` (only `no-cycle` and `no-extraneous-dependencies` enabled), and `eslint-config-prettier` last. Test files (`**/*.test.ts`) override `require-await`, `no-misused-promises` off and allow `_`-prefixed unused vars.
- Strict TypeScript (`strict: true`, `noUnusedLocals`, `noUnusedParameters`).
