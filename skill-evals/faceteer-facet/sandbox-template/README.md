# Helpdesk sandbox

A small helpdesk backend built on `@faceteer/facet` 6.1 against DynamoDB Local
(already running on `localhost:8000`).

## Layout

- `src/config.ts` — DynamoDB client and table name (`TABLE_NAME` env var, default `HELPDESK`).
- `src/models.ts` — Zod schemas and validators for `Ticket` and `Watcher`.
- `src/facets.ts` — the facet definitions. Read this before writing any query.
- `setup-table.ts` — creates or resets the table with GSI1-GSI3 (`npx tsx setup-table.ts`).
- `seed.ts` — writes deterministic sample data (`npx tsx seed.ts`).

## Commands

```sh
npx tsx setup-table.ts   # create/reset the table
npx tsx seed.ts          # seed sample data
npx tsc --noEmit         # typecheck
npx tsx <file>           # run any script
```
