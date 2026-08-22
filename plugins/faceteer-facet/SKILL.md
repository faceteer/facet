---
name: faceteer-facet
description: >-
  Develop with @faceteer/facet 6.x, the TypeScript DynamoDB single-table
  library. Use this skill whenever you write, review, or debug code that
  imports @faceteer/facet — defining a Facet, adding a GSI with addIndex,
  querying with .query()/.list()/.beginsWith(), putting or deleting records,
  conditional writes, pagination cursors, TTL, sharding, or projected reads.
  Also use it when a DynamoDB query through a facet returns empty or
  wrong-order results, when put/delete results go unchecked, or when designing
  key shapes for a new access pattern. Faceteer differs from other DynamoDB
  wrappers in ways that cause silent data bugs, so consult this skill even for
  small changes.
---

# Developing with @faceteer/facet 6.x

Faceteer wraps `@aws-sdk/client-dynamodb` for single-table design. One
`Facet<T>` represents one record type in a shared table. The facet computes
synthetic composite keys (`PK`, `SK`, `GSI1PK`…`GSI20SK`) from model fields on
write, and strips and validates them on read. This skill covers the released
6.x API, current as of 6.1.0.

Rules that prevent the most damage, in order:

1. **Read the facet definition before writing any query.** The `PK`, `SK`, and
   `addIndex` blocks are the only authority on what you can query. There is no
   catalog and no runtime error for querying a shape that doesn't exist — you
   get empty results.
2. **Writes never throw. Branch on the result.** `put` and `delete` report
   failures in their return value. An unchecked write result is a silently
   dropped failure.
3. **Key fields must never be `undefined`.** Key construction silently skips
   missing fields, producing a truncated key that reads and writes the wrong
   row. Don't put optional model fields into any key without a sentinel value.
4. **Partition-key fields go in `.query()`, sort-key fields go in the terminal
   method.** Extra fields passed to `.query()` are silently ignored — an SK
   filter placed there returns the whole partition, and `records[0]` is
   whatever sorts first, which looks like success.
5. **There is no update operation.** Every `put` replaces the whole item.
   Don't look for `.update()` or write an `UpdateExpression`; read-modify-write
   is `get` → mutate the full model → `put` with a `condition` guarding
   against a concurrent change. A bulk update is N read-modify-writes through
   the array form of `put([...])` — never one SQL-shaped `UPDATE … WHERE`
   loop, because index keys are recomputed only on the put path.
6. **Never write around the facet.** The raw client sits on
   `facet.connection.dynamoDb`, but a raw write — `updateItem`, PartiQL
   `UPDATE`, a hand-built `putItem` — bypasses the key-stamping step, so the
   synthetic `PK`/`SK`/`GSInPK`/`GSInSK`/`facet`/`ttl` attributes are not
   recomputed. Change a field that composes any key and that key silently
   desyncs: `get` shows the new value (base item), while every query on the
   affected index keeps routing by the old one. Local testing passes; the
   index is wrong. The legitimate raw-client uses are cross-partition
   enumeration (a `scan` — no facet method enumerates unknown partitions) and
   atomic operations the facet lacks; see the raw-access rules in the Mental
   model section.

## Mental model

- A DynamoDB query can only do: exact `PK` match, plus an optional range or
  prefix condition on `SK`. Every access pattern must be baked into a key
  shape at write time. If a field isn't in the base keys or a GSI's keys, you
  can't query by it — you can only post-filter, which is rarely what you want
  (see the Filters section).
- Keys are strings built as `prefix + delimiter + field1 + delimiter + field2…`
  (delimiter defaults to `_`). `Date` values become ISO-8601 strings, so
  chronological order and lexicographic order agree. Numbers are NOT
  zero-padded — `t9` sorts after `t10`. Pad numeric key fields yourself.
- Several facets can share a partition (same `PK` string, different `SK`
  prefixes). That's standard single-table design, and it's why unbounded range
  queries are dangerous: DynamoDB scopes a query to the raw `PK` string, so
  anything else sharing it comes back too.
- When a raw `scan` or `query` is genuinely necessary (deleting or listing
  "every X across all orgs", a deliberate one-round-trip read of a shared
  partition), keep the facet boundary intact: discriminate mixed rows by the
  authoritative `facet` attribute (never by sniffing model fields), route
  every read item through its owning facet's `out()` so synthetic keys are
  stripped and the record is validated, and perform the writes through
  `Facet.put`/`Facet.delete`. Raw `unmarshall` is not a facet read — it leaks
  `PK`, `SK`, `GSInPK/SK`, `facet`, and `ttl` into your objects.
- **Keys are write-only. Never parse model fields back out of a key string.**
  Prefixes, the delimiter, and values all join into one string, and values can
  contain the delimiter — a regex over `#ORG_acme` captures `_acme` or worse.
  Recover fields by running the item through `out()`; it returns the real
  model fields the keys were built from.

## Table prerequisites

The library assumes the table is set up for it and never checks:

- `PK`, `SK`, and every GSI slot the facets use (`GSI1PK`/`GSI1SK`, …) are
  declared as String attributes, with each `GSIn` index created over its pair.
- GSIs use `ProjectionType: ALL`. A `KEYS_ONLY` or `INCLUDE` index returns
  partial records that fail the facet's validator.
- If any facet uses `ttl`, the table's TTL feature is enabled on the
  attribute named `ttl` — otherwise the written expiry number is inert.
- You construct and own the `DynamoDB` client (`@aws-sdk/client-dynamodb` is
  a peer dependency): region, credentials, endpoint, and retries are yours.

## Defining a facet

```ts
const TicketFacet = new Facet({
	name: 'TICKET', // stamped on every record as `facet`
	validator: ticketValidator, // runs on every READ; throws on bad records
	pickValidator: ticketPickValidator, // optional; unlocks `select`
	PK: { keys: ['orgId'], prefix: '#ORG' },
	SK: { keys: ['ticketId'], prefix: '#TICKET' },
	connection: { dynamoDb, tableName },
	ttl: 'expiresAt', // optional; see TTL section
}).addIndex({
	index: Index.GSI1,
	alias: 'byStatusCreated',
	PK: { keys: ['orgId', 'status'], prefix: '#ORGSTATUS' },
	SK: { keys: ['createdAt'], prefix: '#CREATED' },
});
```

- The validator runs on read, not write, by default. `validateInput: true`
  turns on write-side validation at a per-call cost.
- **Dates round-trip as strings.** The converter stores `Date` fields as a
  string attribute — ISO by default, epoch-seconds string with
  `dateFormat: 'unix'`. Reads hand that string to your validator, so the
  validator must coerce it back — with Zod use `z.coerce.date()`. A validator
  that requires a real `Date` instance rejects every record the facet reads
  back. Composite keys always use ISO encoding for dates, even when
  `dateFormat` is `'unix'`.
- Reserved attribute names — `PK`, `SK`, `facet`, `ttl`, `GSI<n>PK`,
  `GSI<n>SK` — can't be model fields. TypeScript rejects them, and at runtime
  the write fails (reported through the put result, like any other write
  failure — not thrown).
- **Prefix discipline:** within a shared table, no facet's prefix may be a
  leading substring of another's in the same partition. `.list()` compiles to
  `begins_with(SK, prefix)` using the bare prefix — no delimiter appended —
  so with `#EMAIL` and `#EMAIL-ARCHIVE` in one partition, archive rows match
  the email facet's scan and explode in its validator. Compare the bare
  prefixes: `#EMAIL` vs `#EMAIL-ARCHIVE` collides, and so does `#TICKET` vs
  `#TICKET_TAG`; a pair is safe only when neither prefix begins the other
  (for example `#EMAIL` vs `#ARCHIVE`).
- **Key composition is immutable once records exist.** Changing `PK.keys` or
  `SK.keys` on a live facet orphans every existing row (they keep their old
  keys). A new access pattern means a new `addIndex`, never a base-key change.

## Queries

`.query(partitionFields)` builds a lazy `PartitionQuery`; nothing runs until
you call a terminal method:

| Terminal                                  | Key condition             | Use for                                            |
| ----------------------------------------- | ------------------------- | -------------------------------------------------- |
| `.list(opts?)`                            | `begins_with(SK, prefix)` | everything in the partition                        |
| `.first(opts?)`                           | `list` with `limit: 1`    | existence checks, newest/oldest with `scanForward` |
| `.equals(sort, opts?)`                    | `SK = full composite`     | exact-row lookup by full SK                        |
| `.beginsWith(sort, opts?)`                | `begins_with`             | leading-portion match on a composite SK            |
| `.between(start, end, opts?)`             | `BETWEEN` (inclusive)     | bounded ranges, date windows                       |
| `.greaterThan` / `.lessThan` / `…OrEqual` | comparison                | rarely — see the warning                           |

Entry points: `Facet.query({...})` queries the base table with the facet's PK
fields; `Facet.<alias>.query({...})` (or `Facet.GSI1.query(...)`) queries that
index with the _index's_ PK fields. Querying the wrong entry point with the
other one's fields returns zero rows without an error.

### Query rules

- **Supply every PK field of the entry point you're using — no more, no
  less.** A missing field silently builds a partial key (zero rows). An SK
  field passed to `.query()` is silently dropped (whole partition back).
- **`.equals` needs the complete composed SK.** A partial composite matches
  nothing; for a leading subset of SK fields use `.beginsWith`.
- **Newest-first needs `scanForward: false`.** The default is ascending. This
  is the most common "right data, wrong order" bug — and combined with
  `.first()` it silently returns the oldest record instead of the newest.
- **Avoid unbounded `.greaterThan` / `.lessThan`.** With no second bound, the
  range runs to the end of the partition and returns every row that sorts
  after yours — including _other facets' rows_ sharing the partition, which
  then explode in your validator. Prefer `.beginsWith` (the prefix bounds the
  range) or `.between` (both ends bound it). The same applies to a composite
  SK whose leading field you meant to pin: `.greaterThan({ status, sentAt })`
  matches every status that sorts after `status`, not just that one.
  For an open-ended "everything from X onward" query, keep `.between` and
  synthesize the missing bound with a max sentinel:
  `.between({ ticketId: from }, { ticketId: '\uffff' })` — `'\uffff'` sorts
  after any realistic key text, so the range stays inside this facet's rows.
- **Sort-key args accept a raw string** to bypass key construction:
  `.beginsWith('#TICKET_t00')`. Useful for prefix tricks the object form
  can't express; you own the prefix and delimiter correctness, and a missing
  prefix reads the wrong rows or none.
- **A field in the GSI partition key means one query per value.** If `status`
  is in the index PK, one query returns one status — run one query per status
  and concatenate. Only a field in the SK can be range-matched or
  prefix-matched in a single query.

### Filters run after the page is cut

`filter` is a DynamoDB `FilterExpression`: the table reads up to `limit` rows
(or 1 MB), _then_ discards non-matching rows from that page. Consequences:

- A filtered page is routinely short or empty while matching rows remain.
  **Paginate on cursor presence, never on page emptiness:**

  ```ts
  let cursor: string | undefined;
  do {
  	const page = await TicketFacet.query({ orgId }).list({ filter, cursor });
  	results.push(...page.records);
  	cursor = page.cursor;
  } while (cursor);
  ```

- `.first({ filter })` examines exactly one row. It answers "does the
  first-sorting row match?", not "find the first matching row".
- Filter grammar is narrower than condition grammar: comparators (`=`, `<>`,
  `<`, `<=`, `>`, `>=`), `between`, `begins_with`, and `AND`/`OR` nesting
  only. No `exists`, `in`, `contains`, or `NOT` in filters. Multi-value match
  is an OR tree: `[['status', '=', 'open'], 'OR', ['status', '=', 'pending']]`.
- Filters can only reference fields outside the facet's base `PK`/`SK` — the
  `filter` type excludes key fields.
- A predicate you filter on frequently belongs in a key shape (new GSI), not
  in a filter — you pay read capacity for every discarded row.

### Pagination cursors

Query results return `{ records, cursor? }`. The cursor is an opaque
base64url string scoped to the exact query that produced it; pass it back as
`options.cursor`. Don't parse cursors, and don't persist them across a
library major version (v5 cursors don't decode in v6).

## Reads throw, writes report

| Operation                             | On failure                                    | On not-found                |
| ------------------------------------- | --------------------------------------------- | --------------------------- |
| `get` (single/batch), query terminals | **throws** (SDK error or validator rejection) | `null` / missing from array |
| `put`, `delete` (single/batch)        | **returns** failure in the result object      | n/a                         |

- Result shapes differ: single `put` → `{ wasSuccessful, record, error? }`.
  Batch `put` → `{ hasFailures, put, failed }`. `delete` returns
  `{ hasFailures, deleted, failed }` for both single and batch — there is no
  `wasSuccessful` on delete.
- **A failed single `put` may still have written.** The write happens first;
  the returned record is then run through the validator (which always runs on
  the read-back path, even with `validateInput` off), and a failure reports
  `wasSuccessful: false` _after_ the item persisted. Never infer "nothing was
  written" from a failed put. Batch put is the mirror image: each record is
  validated while preparing the request, so one invalid record fails its
  entire 25-item chunk _before_ any of that chunk is written.
- Batch semantics: puts/deletes go in chunks of 25, gets in chunks of 100
  (DynamoDB hard limits), several chunks in flight (`concurrency` option,
  default 8). Unprocessed items are retried with backoff, then land in
  `failed` (writes) or are silently dropped (gets) — a batch `get` result
  can't distinguish "not found" from "gave up", though a hard transport error
  still throws.
- Batch `put`/`delete` results preserve input order but can be _shorter_ than
  the input (failures move to `failed`; duplicate keys collapse). Batch `get`
  results are unordered and silently omit missing rows. Either way, **match
  results by id; never zip with the input by index.**
- Duplicate PK+SK pairs within a batch write collapse to one item
  (last-write-wins), so `put.length` can be less than the input length.
- **A "successful" delete doesn't prove the row existed.** DynamoDB's
  `DeleteItem` succeeds on a nonexistent key, so a delete built from a wrong
  key lands in `deleted` with no failure anywhere — a cleanup job can report
  N deletions while deleting nothing. When a delete must have really removed
  data, verify by reading, not by the delete result.

## Conditional writes

`put` and `delete` accept `{ condition }` on the **single-item form only**
(the batch overloads don't accept one). Conditions are tuple trees; the types
(`ConditionExpression` and friends) are exported from the package root:

```ts
['status', '=', 'open'][
	('assigneeId', 'not_exists')
] // also: 'exists'
[('subject', 'begins_with', 'urgent')][('priority', 'between', 1, 3)][
	('status', 'in', ['open', 'pending'])
] // conditions only, not filters
[(['a', '=', 1], 'AND', ['b', '=', 2])]; // also 'OR'; nest arbitrarily
{
	NOT: ['status', '=', 'closed'];
}
```

- **The `size` operator requires 6.1.0 or later.** It takes a comparator and
  a number: `['tags', 'size', '>=', 2]`. On 6.0.0 it compiled with an
  unbalanced parenthesis, so every request that included it failed with a
  `ValidationException` — check the installed version before reaching for it.
- **`in` needs a non-empty list.** From 6.1.0 an empty list throws a
  descriptive error at compile time; on 6.0.0 it sent `IN ()`, which DynamoDB
  rejects with an opaque syntax error.
- A failed condition is not an exception. It comes back as
  `wasSuccessful: false` with `error.name === 'ConditionalCheckFailedException'`.
  Handle the two failure classes differently: a conditional failure is the
  race you designed for (return "already taken", retry the CAS loop);
  anything else is an infrastructure failure (rethrow or alert).

  ```ts
  const result = await TicketFacet.put(updated, {
  	condition: ['version', '=', current.version],
  });
  if (!result.wasSuccessful) {
  	if ((result.error as Error)?.name === 'ConditionalCheckFailedException') {
  		return { conflict: true };
  	}
  	throw result.error;
  }
  ```

- Standard patterns: **create-if-not-exists** — condition
  `['someKeyField', 'not_exists']` (the condition evaluates against whatever
  item already holds that key). **CAS / optimistic locking** — carry a
  `version` field, condition on its current value, bump it in the new record.
- A conditional protocol is only as strong as its complete writer set: one
  unconditional `put` to the same keys bypasses every guard. Before trusting
  a condition, find every writer to that facet.
- **A condition only evaluates against the exact PK+SK being written** — it
  can't enforce uniqueness of a non-key attribute. Query-for-duplicates then
  put is a time-of-check/time-of-use race that two concurrent callers both
  pass. To make a non-key field unique (one email per org, say), model it
  into a key: a dedicated lock record keyed by that value, written with
  `not_exists` — the race then resolves on the conditional write.

## TTL

`ttl: 'fieldName'` maps a model field to the synthetic `ttl` attribute that
DynamoDB's reaper reads (see Table prerequisites).

- Supply a `Date` or epoch **seconds** number. A string is `parseInt`'d: an
  ISO string like `'2027-01-01T…'` becomes `2027` — an epoch in January 1970,
  so the record is immediately eligible for deletion. An unparseable string
  drops the TTL silently.
- DynamoDB deletes expired items lazily (minutes to days late), and reads do
  not filter them. If staleness matters, compare the expiry field in
  application code.

## Projected reads (`select`)

`select: ['field1', 'field2']` on `get` and every query terminal narrows the
read to those attributes plus the key fields (always auto-included so results
can round-trip into `get`/`delete`).

- Requires `pickValidator` in the facet options. Without one, `select` is a
  compile-time error.
- Shrinks payload and validation cost, not read capacity or the 1 MB page
  budget — DynamoDB applies projection after reading the row.
- Never feed a projected read into a read-modify-write: the re-`put` replaces
  the whole item, so every field the projection omitted is dropped from the
  stored record. TypeScript rejects putting a `Pick<T, …>` — silencing that
  error with `as T` or `as any` is the trap, not the fix. `select` is
  read-only; for any read-modify-write, fetch the full record (no `select`)
  before writing it back.

## GSIs and index lifecycle

- 20 slots (`Index.GSI1`…`Index.GSI20`) with fixed `GSInPK`/`GSInSK`
  attribute names. Different facets may reuse the same slot for different
  shapes as long as their prefixes on that slot are disjoint.
- `addIndex` mutates the facet and returns it re-typed with the index and
  alias accessors — always chain (`new Facet({...}).addIndex({...})`) rather
  than calling it on a variable and discarding the return, or TypeScript
  won't know the accessor exists. Registering a slot twice or an alias that
  collides with an existing property throws at startup.
- **GSI key attributes are stamped only at `put` time.** Adding an index to a
  facet with existing data makes historical rows invisible on that index until
  each is re-put. Adding an access pattern to a live facet means shipping a
  backfill (dry-run first) alongside it.
- **Tightening the schema is a breaking change for every existing row.**
  `out()` runs the _current_ validator on every read and DynamoDB has no
  DDL-time backfill, so a new required field (`z.string()` with no
  `.default()` or `.optional()`) makes `get` and every query terminal throw
  on rows written before the change. Give new fields a read-side default or
  ship a re-put backfill first — same lifecycle as adding an index. Verify by
  reading a pre-existing row, not just a freshly created one.

## Sharding

A key config may carry `shard: { count, keys }` to spread a hot partition:
the key gains a CRC-32-derived hex shard id after the prefix.

- On write the shard id is computed from the record's shard-key values.
- On query, pass the shard explicitly: `facet.byIndex.query({...}, shardId)`.
  To read the whole logical partition, loop `0…count-1` and merge.
- **Omitting the shard on a query does not fan out** — it hashes whatever
  shard-key values happen to be present in your query object (an empty set
  hashes to one fixed shard) and quietly reads only that shard.
- Shard `0` is a valid id — don't truthy-check shard values in your own code.

## Debugging: query returns empty or wrong records

Work down this list; each step is a distinct failure mode:

1. Every PK field of this entry point present in `.query()`, and only PK
   fields? (Missing → partial key, zero rows. SK field there → it was
   ignored; you're seeing the whole partition.)
2. Right entry point? `Facet.query` vs `Facet.<alias>.query` have different
   PK shapes.
3. Terminal matches the SK shape? (`equals` with a partial composite → zero
   rows; use `beginsWith`.)
4. `scanForward` direction right? (`first()` + wrong direction = wrong record,
   not zero records.)
5. Filtered page empty but `cursor` present? That's normal — keep paging.
6. Did the write land? Check the `put` result — a dropped `hasFailures` is
   invisible in logs.
7. Querying a recently added GSI? Rows written before the index existed have
   no GSI keys — the backfill may not have run.
8. Validator throwing rather than empty results? That's a read of foreign or
   malformed rows — check for unbounded range queries, prefix collisions, or
   a validator that can't coerce stored date strings.
9. Sharded key? A query without an explicit shard read one hashed bucket,
   not the whole partition.
