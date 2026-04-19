![Faceteer](https://imagedelivery.net/zms_mS5SEgxiPTJtX-M2fQ/e83e9689-bace-449d-86b4-cccb45d65700/public)

[![codecov](https://codecov.io/gh/faceteer/facet/branch/main/graph/badge.svg?token=ORKKZWL5N6)](https://codecov.io/gh/faceteer/facet)
[![Test](https://github.com/faceteer/facet/actions/workflows/test.yml/badge.svg)](https://github.com/faceteer/facet/actions/workflows/test.yml)
[![npm version](https://img.shields.io/npm/v/@faceteer/facet)](https://www.npmjs.com/package/@faceteer/facet)

Faceteer is meant to make it easier to work with a single-table DynamoDB architecture.

The structure and records that are kept in a Dynamo DB table are more so designed around how you access the data instead of the structure of the data itself.

If you're not familiar with the Dynamo DB single table concepts, this video is a good starting point.

[Amazon DynamoDB deep dive](https://www.youtube.com/watch?v=6yqfmXiZTlM)

## Getting Started

This document aims to be a gentle introduction to the framework and its features.

We will be creating a mock "tasks" application.

### Install

Install with npm. `@aws-sdk/client-dynamodb` is a peer dependency, so install both:

```
npm i @faceteer/facet @aws-sdk/client-dynamodb --save
```

Faceteer requires Node.js 20 or newer.

### Access Patterns

Knowing and identifying your access patterns for your application is critical for a single-table design. Faceteer is not well suited for ad-hoc queries.

In our example we will be creating an application that keeps track of tasks for a team of people.

Our application should support multiple teams, teams should have have multiple users, and team tasks should be either unassigned or assigned to an individual user.

The access patterns we need to support are:

- _"Get a team by its id"_
- _"Get a user by their id"_
- _"Get a user by their email address"_
- _"Get a task by its id"_
- _"List users by team ordered by the date they were created"_
- _"List tasks by team ordered by the date they were created"_
- _"List tasks by team ordered by the date they are due"_
- _"List tasks by user and by status ordered by the date they were created"_
- _"List tasks by user and by status ordered by the date they are due"_

Here are the types we will use for our application:

```ts
export interface Team {
  teamId: string;
  teamName: string;
  dateCreated: Date;
  dateDeleted?: Date;
}

export interface User {
  userId: string;
  teamId: string;
  email: string;
  password: string;
  dateCreated: Date;
  dateDeleted?: Date;
}

export interface Task {
  taskId: string;
  teamId: string;
  assignedUserId?: string;
  dateCreated: Date;
  dateDue: Date;
  status: "open" | "completed" | "deleted";
}
```

### Validation

Faceteer requires a validator function on every facet. By default it runs on **read**, so invalid records never reach your application code. The function takes an unknown input and returns an object that matches the model for the facet, or throws if the input is invalid.

If you also want validation on **write**, pass `validateInput: true` to the facet constructor. This adds a per-call cost and is off by default.

```ts
import AJV, { JSONSchemaType } from "ajv";

export interface Team {
  teamId: string;
  teamName: string;
  dateCreated: Date;
  dateDeleted?: Date;
}

// Here we use AJV to define the schema for a team
const schema: JSONSchemaType<Team> = {
  type: "object",
  additionalProperties: false,
  properties: {
    teamId: { type: "string" },
    teamName: { type: "string" },
    dateCreated: { type: "object", format: "date-time" },
    dateDeleted: { type: "object", format: "date-time", nullable: true },
  },
  required: ["teamId", "teamName", "dateCreated"],
};
const validateTeam = ajv.compile(schema);

// We'll pass this function to our Facet for validating records from the DB
export function teamValidator(input: unknown): Team {
  if (validateTeam(input)) {
    return input;
  }
  throw validateTeam.errors[0];
}
...
```

### Creating a Facet

Let's create our facets! Since all of our application logic is isolated by team, we'll use the team id as the partition for all of our models.

Every facet needs a unique `name`. It's written to each record under the `facet` attribute, which lets you distinguish item types when several facets share a single table.

```ts
import { Facet } from "@faceteer/facet";
import { teamValidator, userValidator, taskValidator } from "./models";
import { DynamoDB } from "@aws-sdk/client-dynamodb";

const dynamoDbClient = new DynamoDB({});
const dynamoDbTableName = "ExampleTableName";

// Facet containing our teams
const TeamFacet = new Facet({
  name: "TEAM",
  PK: {
    keys: ["teamId"],
    prefix: "#TEAM",
  },
  SK: {
    keys: ["teamId"],
    prefix: "#TEAM",
  },
  connection: {
    dynamoDb: dynamoDbClient,
    tableName: dynamoDbTableName,
  },
  validator: teamValidator,
});

// Facet containing our users
const UserFacet = new Facet({
  name: "USER",
  PK: {
    keys: ["teamId"],
    prefix: "#TEAM",
  },
  SK: {
    keys: ["userId"],
    prefix: "#USER",
  },
  connection: {
    dynamoDb: dynamoDbClient,
    tableName: dynamoDbTableName,
  },
  validator: userValidator,
});

// Facet containing our tasks
const TaskFacet = new Facet({
  name: "TASK",
  PK: {
    keys: ["teamId"],
    prefix: "#TEAM",
  },
  SK: {
    keys: ["taskId"],
    prefix: "#TASK",
  },
  connection: {
    dynamoDb: dynamoDbClient,
    tableName: dynamoDbTableName,
  },
  validator: taskValidator,
});
```

### Putting Records

Now we can insert a team into our table.

```ts
import { nanoid } from "nanoid";
import { TeamFacet } from "./facets";
import type { Team } from "./models";

const team: Team = {
  teamId: nanoid(),
  teamName: "Penguin Popsicles",
  dateCreated: new Date(),
};

const putResult = await TeamFacet.put(team);

if (putResult.wasSuccessful) {
  // `record` will contain the record that was
  // put into the database
  return putResult.record;
} else {
  // If there is an issue putting the record into
  // the database the error will be attached to `error`
  throw putResult.error;
}
```

We can also put several records at once. Faceteer chunks array puts into DynamoDB's max batch size of 25 items and retries any `UnprocessedItems` up to 5 times with exponential backoff. Records that still fail after retries are reported back in `failed`.

Let's make a function to create multiple users.

```ts
import { nanoid } from "nanoid";
import { UserFacet } from "./facets";
import type { User } from "./models";

export async function createNewUsers(usersToCreate: Omit<User, "userId">[]) {
  // Generate new unique IDs for users
  const users: User[] = usersToCreate.map((userToCreate) => ({
    userId: nanoid(),
    ...userToCreate,
  }));

  // Save the new users to Dynamo DB
  const putResult = await UserFacet.put(users);

  // Check to see if any of the put requests failed
  if (putResult.hasFailures) {
    for (const putFailure of putResult.failed) {
      handleFailure(putFailure);
    }
  }

  // Return the users that were successfully created
  return putResult.put;
}
```

#### Conditional puts

Pass a `condition` with a single-item `put` to guard against writes that would clobber existing data. Conditions use the tuple syntax from [`@faceteer/expression-builder`](https://github.com/faceteer/expression-builder).

```ts
// Only create the user if one with this partition/sort key doesn't already exist
const result = await UserFacet.put(user, {
  condition: ["userId", "not_exists"],
});
```

`condition` is only applied to single-item calls. If you pass it alongside an array, it's silently ignored.

### Getting Records

Getting records from a Facet requires any properties that are used in the partition key _AND_ the sort key.

This is because Dynamo DB uniquely identifies records in a table by the combination of both keys.

```ts
import { UserFacet } from "./facets";
import type { User } from "./models";

export async function getUser(teamId: string, userId: string) {
  // Since `teamId` and `userId` are used by the UserFacet to
  // create the partition and sort keys, both must be provided
  // to get the user record
  const user = await UserFacet.get({ teamId, userId });

  // If there is no record matching the combined partition and sort
  // keys faceteer will return null
  if (!user) {
    throw new Error("User not found");
  }
  return user;
}
```

A get request will always return exactly one record, or a `null` value.

You can also pass an array of identifiers to fetch many records in one call. Faceteer chunks requests into DynamoDB's max batch-get size of 100 items and retries any `UnprocessedKeys` up to 10 times with exponential backoff. The returned array is not guaranteed to preserve input order.

```ts
const users = await UserFacet.get([
  { teamId, userId: "a" },
  { teamId, userId: "b" },
  { teamId, userId: "c" },
]);
```

### Querying

Querying for records happens in two parts.

First you must specify which partition you want to query.

```ts
import { UserFacet } from "./facets";

export async function getUsersForTeam(teamId: string) {
  const partition = UserFacet.query({ teamId });
}
```

Then you specify the query operation you want to run against that partition.

```ts
import { UserFacet } from "./facets";

export async function getUsersForTeam(teamId: string) {
  // You can store a partition in a variable to re-use it
  const teamPartition = UserFacet.query({ teamId });
  const { records, cursor } = await teamPartition.list();

  // Or you can call an operation on a partition directly
  const { records, cursor } = await UserFacet.query({ teamId }).list();

  return { users: records, cursor };
}
```

You can query in the following ways:

- `equals(key)`
  - Returns records with sort keys that exactly match the specified sort key.
- `greaterThan(key)`
  - Returns records with sort keys that are greater than the specified sort key.
- `greaterThanOrEqual(key)`
  - Returns records with sort keys that are greater than or equal to the specified sort key.
- `lessThan(key)`
  - Returns records with sort keys that are less than the specified sort key.
- `lessThanOrEqual(key)`
  - Returns records with sort keys that are less than or equal to the specified sort key.
- `beginsWith(key)`
  - Returns records with sort keys that begin with the specified sort key.
- `between(startKey, endKey)`

  - Returns records with sort keys that are greater than or equal to the start key, and are less than or equal to the end key.

- `list()`
  - Returns records where the sort key starts with the facet prefix for that sort key
- `first()`
  - Equivalent to calling `list()` and selecting the first option

The results will always be ordered by the sort key.

Every query operator accepts a shared `options` argument where you can pass a `filter`, a `limit`, a `cursor` for pagination, `scanForward: false` to reverse order, and (for sharded keys) a `shard` number. Filters use tuple syntax from [`@faceteer/expression-builder`](https://github.com/faceteer/expression-builder) and run on the server after key conditions, so they shrink the response but not the read cost.

```ts
// Everything in the partition except failed tasks
const { records, cursor } = await TaskFacet.query({ teamId }).list({
  filter: ["status", "<>", "failed"],
  limit: 50,
});
```

One of our access patterns was _"List users by team ordered by the date created"_, but our sort key is ordered by the `userId`.

One option would be to include the user's `dateCreated` as a part of the sort key for a user.

```diff
const UserFacet = new Facet({
  name: "USER",
  PK: {
    keys: ["teamId"],
    prefix: "#TEAM",
  },
  SK: {
-    keys: ["userId"],
+    keys: ["dateCreated", "userId"],
    prefix: "#USER",
  },
  connection: {
    dynamoDb: dynamoDbClient,
    tableName: dynamoDbTableName,
  },
  validator: userValidator,
});
```

We have some restrictions to consider before using this approach though.

1. Partition and sort keys are **IMMUTABLE** and cannot be changed.
2. You **MUST** provide all properties that make up a partition and sort key when getting a record by it's ID.

It is unlikely that a user's created date will change, so having it be part of the immutable partition key is fine. Having the created date as a part of the primary identifier for a user does seem odd though, and in general should be a smell that we're doing something wrong.

The second restriction ends up being problematic for us here. It would be cumbersome to have to provide the date that a user was created every time you want to get that user by their ID.

Another option is to prefix a user's ID with the date whenever you create a user. Let's re-visit our `createNewUsers()` function from earlier.

```diff
const users: User[] = usersToCreate.map((userToCreate) => ({
-  userId: nanoid(),
+  userId: userToCreate.dateCreated.getTime().toString(36) + nanoid(),
  ...userToCreate,
}));
```

Now the `userId` will start with a base 36 representation of when the user was created, ordering our sort key!

While this is a helpful technique, in many cases we won't be able to modify the sort key for the table to be ordered exactly how we need it.

To enable other access patterns we often have to use indexes...

### Indexes

Dynamo DB uses Global Secondary Indexes (GSIs) to allow for repartitioning and resorting of data in a table. This is what allows us to add more access patterns for our facets.

Let's take a look at the four access patterns we need for our tasks:

- [ ] _"List tasks by team ordered by the date they were created"_
- [ ] _"List tasks by team ordered by the date they are due"_
- [ ] _"List tasks by user and by status ordered by the date they were created"_
- [ ] _"List tasks by user and by status ordered by the date they are due"_

Here is our facet:

```ts
export const TaskFacet = new Facet({
  name: "TASK",
  PK: {
    keys: ["teamId"],
    prefix: "#TEAM",
  },
  SK: {
    keys: ["taskId"],
    prefix: "#TASK",
  },
  connection: {
    dynamoDb: dynamoDbClient,
    tableName: dynamoDbTableName,
  },
  validator: taskValidator,
});
```

Currently we can only list tasks for a team ordered by the task ids.

First we can use the trick with sequential time ids to allow have the task id sort order match the task created at sort order. This covers one of our access patterns

- [x] _"List tasks by team ordered by the date they were created"_
- [ ] _"List tasks by team ordered by the date they are due"_
- [ ] _"List tasks by user and by status ordered by the date they were created"_
- [ ] _"List tasks by user and by status ordered by the date they are due"_

To get tasks by team ordered by due date we'll have to configure a GSI using the `addIndex()` command.

```ts
import { Facet, Index } from "@faceteer/facet";

export const TaskFacet = new Facet({
  name: "TASK",
  PK: {
    keys: ["teamId"],
    prefix: "#TEAM",
  },
  SK: {
    keys: ["taskId"],
    prefix: "#TASK",
  },
  connection: {
    dynamoDb: dynamoDbClient,
    tableName: dynamoDbTableName,
  },
  validator: taskValidator,
}).addIndex({
  index: Index.GSI1,
  PK: {
    keys: ["teamId"],
    prefix: "#TEAM",
  },
  SK: {
    keys: ["dateDue"],
    prefix: "#TASK_DUE",
  },
  alias: "GSITeamDueDate",
});
```

Now we can use `GSI1` to query for our tasks ordered by their due date!

```ts
export async function getTeamTasks(teamId: string) {
  const queryResult = await TaskFacet.GSI1.query({ teamId }).list();
  return {
    tasks: queryResult.records,
    cursor: queryResult.cursor,
  };
}
```

Or we can get any tasks that are past due.

```ts
export async function getPastDueTasks(teamId: string) {
  const today = new Date();
  const queryResult = await TaskFacet.GSI1.query({ teamId }).lessThan({
    dateDue: today,
  });
  return {
    tasks: queryResult.records,
    cursor: queryResult.cursor,
  };
}
```

We can also use the alias `GSITeamDueDate` instead of `GSI1` for readability.

```diff
export async function getPastDueTasks(teamId: string) {
	const today = new Date();
-	const queryResult = await TaskFacet.GSI1.query({ teamId }).lessThan({
+	const queryResult = await TaskFacet.GSITeamDueDate.query({ teamId }).lessThan({
		dateDue: today,
	});
	return {
		tasks: queryResult.records,
		cursor: queryResult.cursor,
	};
}

```

This enables another required access pattern.

- [x] _"List tasks by team ordered by the date they were created"_
- [x] _"List tasks by team ordered by the date they are due"_
- [ ] _"List tasks by user and by status ordered by the date they were created"_
- [ ] _"List tasks by user and by status ordered by the date they are due"_

For the last two we'll need two more indexes.

```ts
import { Facet, Index } from "@faceteer/facet";

export const TaskFacet = new Facet({
  name: "TASK",
  PK: {
    keys: ["teamId"],
    prefix: "#TEAM",
  },
  SK: {
    keys: ["taskId"],
    prefix: "#TASK",
  },
  connection: {
    dynamoDb: dynamoDbClient,
    tableName: dynamoDbTableName,
  },
  validator: taskValidator,
})
  .addIndex({
    index: Index.GSI1,
    PK: {
      keys: ["teamId"],
      prefix: "#TEAM",
    },
    SK: {
      keys: ["dateDue"],
      prefix: "#TASK_DUE",
    },
    alias: "GSITeamDueDate",
  })
  .addIndex({
    index: Index.GSI2,
    PK: {
      keys: ["assignedUserId", "status"],
      prefix: "#USER_STATUS",
    },
    SK: {
      keys: ["dateCreated"],
      prefix: "#TASK_CREATED",
    },
    alias: "GSIUserStatusCreated",
  })
  .addIndex({
    index: Index.GSI3,
    PK: {
      keys: ["assignedUserId", "status"],
      prefix: "#USER_STATUS",
    },
    SK: {
      keys: ["dateDue"],
      prefix: "#TASK_DUE",
    },
    alias: "GSIUserStatusDue",
  });
```

Faceteer supports up to twenty indexes (`Index.GSI1` through `Index.GSI20`), matching DynamoDB's own per-table GSI limit. The fixed attribute names (`GSI1PK`/`GSI1SK`, `GSI2PK`/`GSI2SK`, ...) must be declared on your table's schema for any index you actually use.

- [x] _"List tasks by team ordered by the date they were created"_
- [x] _"List tasks by team ordered by the date they are due"_
- [x] _"List tasks by user and by status ordered by the date they were created"_
- [x] _"List tasks by user and by status ordered by the date they are due"_

### Projected reads

Every read returns a record with every attribute DynamoDB stored. When you only need a subset of fields, you can reduce network payload and JSON-parse cost with a DynamoDB `ProjectionExpression`. Faceteer exposes this as a `select` option on every read method.

Projected reads require a `pickValidator` on the facet. The full `validator` would reject a record with missing fields by design, so projection needs a factory that produces a sub-validator for any chosen subset of keys.

```ts
import { PickValidator } from "@faceteer/facet";

export const teamPickValidator: PickValidator<Team> = (keys) => {
  const mask: { [K in keyof Team]?: true } = {};
  for (const k of keys) mask[k as keyof Team] = true;
  const picked = teamSchema.pick(mask);
  return (input) => picked.parse(input) as Pick<Team, (typeof keys)[number]>;
};
```

Zod, Valibot, and arktype all expose `.pick()` or equivalent. AJV users typically cache compiled sub-validators by a canonical signature of the key tuple. See the tsdoc on `PickValidator` for a worked AJV example.

Configure the facet with both validators:

```ts
const TeamFacet = new Facet({
  name: "TEAM",
  validator: teamValidator,
  pickValidator: teamPickValidator,
  PK: { keys: ["teamId"], prefix: "#TEAM" },
  SK: { keys: ["teamId"], prefix: "#TEAM" },
  connection: { dynamoDb: dynamoDbClient, tableName: dynamoDbTableName },
});
```

#### On `get`

```ts
// Unprojected: the full record.
const team = await TeamFacet.get({ teamId });

// Projected: only the chosen fields plus the facet's PK/SK fields.
const slim = await TeamFacet.get({ teamId }, { select: ["teamName"] });
// slim: { teamId: string; teamName: string } | null
```

PK and SK fields are always included in the result even if omitted from `select`. They are load-bearing for identity: you need them to round-trip the result back into a `get`, `delete`, or `put`. Batch gets work the same way:

```ts
const slim = await TeamFacet.get(
  teamIds.map((teamId) => ({ teamId })),
  { select: ["teamName"] },
);
```

#### On `query`

Every `PartitionQuery` operator accepts `select` and narrows the return type the same way.

```ts
await TeamFacet.query({ teamId }).list({ select: ["teamName"] });

await TeamFacet.query({ teamId }).first({ select: ["teamName"] });

await TeamFacet.GSIByStatus
  .query({ teamId, status: "active" })
  .between(
    { createdAt: "2024-01-01" },
    { createdAt: "2024-02-01" },
    { select: ["teamName"] },
  );
```

On an index query, Faceteer auto-includes both the base-table PK/SK fields and the index's own partition-key and sort-key fields. Under the library's assumption that GSIs are created with `ProjectionType: ALL`, all four sets are always present on the index. The guarantee lets you feed a projected result straight back into `get`, `delete`, or `put` without a second round-trip to fetch the identity fields.

#### Type-level gate

Projection is unavailable at the type level on facets constructed without a `pickValidator`. Passing `select` on those facets is a compile error, not a runtime error.

```ts
const PlainFacet = new Facet({ /* no pickValidator */ });

// Type error: this overload requires pickValidator on the facet.
await PlainFacet.get({ teamId }, { select: ["teamName"] });

// Also a type error:
await PlainFacet.query({ teamId }).list({ select: ["teamName"] });
```

If you want projected reads, add a `pickValidator` to the facet options. If you want to opt out of validation for projected reads (trust-the-DB paths, or benchmarks), pass an identity pickValidator:

```ts
const identityPickValidator: PickValidator<Team> =
  (keys) => (input) => input as Pick<Team, (typeof keys)[number]>;
```

#### Cost

Projection reduces the data DynamoDB sends over the wire, which shrinks payload size and JSON-parse cost. It does not reduce read capacity: DynamoDB charges based on the size of the item it reads from storage, not what it projects to the response. Projection is a bandwidth and latency optimization, not an RCU one.

### Composite sort keys

Sort keys in Faceteer are composite strings built from a prefix and the fields you list in `SK.keys`. A facet configured with `SK: { keys: ['status', 'timestamp'], prefix: '#ESTIME' }` writes sort keys like `#ESTIME_queued_2024-01-01T00:00:00.000Z`.

DynamoDB stores and compares those sort keys as plain strings. `equals`, `beginsWith`, and `between` all operate on the composite string, not on the individual fields you passed in. That is usually what you want:

```ts
// Every record in the partition whose composite SK starts with '#ESTIME_queued_'.
await EmailFacet.byStatus
  .query({ userId })
  .beginsWith({ status: 'queued' });
```

`greaterThan`, `greaterThanOrEqual`, `lessThan`, and `lessThanOrEqual` also compare the entire composite string. They do not scope to a single value of the leading field. This trips people up:

```ts
// Anti-pattern. Bleeds across status values.
await EmailFacet.byStatus
  .query({ userId })
  .greaterThan({ status: 'queued', timestamp: '2024-01-01' });
```

DynamoDB asks for every record where `#ESTIME_queued_2024-01-01 < SK`. That includes records with `status = 'queued'` and a later timestamp, and also every record whose status sorts alphabetically after `queued`, such as `sent` or `spam`. The operator has no way to know you meant "queued only".

Two ways to range correctly over a trailing field:

1. Use `between` with both bounds pinned to the same leading value. Both strings share the `#ESTIME_queued_` prefix, so the range cannot cross into another status.

   ```ts
   await EmailFacet.byStatus
     .query({ userId })
     .between(
       { status: 'queued', timestamp: '2024-01-01' },
       { status: 'queued', timestamp: '2024-02-28' },
     );
   ```

   For an open upper bound, use a sentinel that sorts after every real value, for example `'\uffff'`.

2. Design the index so the scoping field is in the partition key. This is what the tasks example's `GSIUserStatusCreated` index does:

   ```ts
   // PK carries the scoping field; SK is free for a clean range.
   PK: { keys: ['assignedUserId', 'status'], prefix: '#USER_STATUS' },
   SK: { keys: ['dateCreated'],              prefix: '#TASK_CREATED' },
   ```

   With that shape, every query is implicitly scoped to one status, and `greaterThan({ dateCreated })` means exactly what it looks like it means.

**Decision rule.** If the field is only ever queried for one value at a time, put it in the partition key. If you want one query that spans values of the field and orders by a secondary attribute, put it as the leading entry of a composite sort key and scope with `beginsWith` or equal-bound `between`. If the field is only for ordering, leave it out of the keys and pick a different sort field.

### Query pattern reference

A direct lookup for common access patterns. Each assumes the facet has an index shaped appropriately for that pattern.

**"Every record in a partition, in sort-key order"**

```ts
await Facet.query({ partitionFields }).list();
```

**"Every record whose leading SK field equals a value"**

```ts
await Facet.query({ partitionFields }).beginsWith({ leadingField: value });
```

**"Range over a trailing SK field, scoped to one value of the leading SK field"**

```ts
await Facet.query({ partitionFields }).between(
  { leadingField: value, trailingField: start },
  { leadingField: value, trailingField: end },
);
```

**"Every record with a given field value, ordered by another field"**

Use an index with the scoping field in the PK and the ordering field in the SK.

```ts
// Index shape
PK: { keys: [...partitionFields, 'scopingField'], prefix: '#SCOPE' },
SK: { keys: ['orderingField'],                    prefix: '#ORDER' },

// Call
await Facet.byScopedOrder
  .query({ ...partitionFields, scopingField: value })
  .list();
```

**"Most recent record"**

```ts
await Facet.query({ partitionFields }).first({ scanForward: false });
```

**"The N newest records"**

```ts
await Facet.query({ partitionFields }).list({
  limit: N,
  scanForward: false,
});
```

**"Records in a partition, filtered by a non-key attribute"**

```ts
await Facet.query({ partitionFields }).list({
  filter: ['someField', '=', value],
});
```

`filter` runs server-side after the key condition. It reduces what you see, not the read capacity you pay for.

**"Does any record exist in this partition?"**

```ts
const exists = (await Facet.query({ partitionFields }).first()) !== null;
```

**"Iterate every record in a partition"**

```ts
let cursor: string | undefined;
do {
  const page = await Facet.query({ partitionFields }).list({
    limit: 100,
    cursor,
  });
  for (const record of page.records) {
    // handle record
  }
  cursor = page.cursor;
} while (cursor);
```

**"Every record, but only a subset of attributes"**

Requires `pickValidator` on the facet.

```ts
await Facet.query({ partitionFields }).list({ select: ["name", "status"] });
```

PK/SK fields are auto-included in the result, and on index queries the index's PK/SK fields are included too.

### Pagination

DynamoDB returns at most **1 MB** of evaluated data per page. When more data is available, the service returns a `LastEvaluatedKey`; Faceteer surfaces it as an opaque `cursor` string (CBOR, then base64). Treat the cursor as a token — don't try to parse it, and don't persist it beyond the current session: the encoding is tied to this library's version and the DynamoDB SDK's `AttributeValue` shape.

Two things commonly surprise new users:

1. `limit` caps how many items DynamoDB **evaluates**, not how many match. Filters run **after** key conditions, so a query with a filter can return an empty `records` array *and* a `cursor` — that just means "keep going".
2. You're done paginating when `cursor` comes back `undefined`, not when `records` is empty.

A typical paginate-to-the-end loop:

```ts
let cursor: string | undefined;
do {
  const page = await TaskFacet.query({ teamId }).list({ limit: 50, cursor });
  for (const task of page.records) {
    // ...handle task
  }
  cursor = page.cursor;
} while (cursor);
```

### TTL

Set `ttl` on the facet to the name of a field containing a unix timestamp, a numeric string, or a `Date`. Faceteer writes that value into DynamoDB's TTL attribute so the service can purge the record asynchronously. AWS's sweeper typically deletes expired items within 48 hours — filter out already-expired records client-side if freshness matters.

```ts
const SessionFacet = new Facet({
  name: "SESSION",
  PK: { keys: ["userId"], prefix: "#USER" },
  SK: { keys: ["sessionId"], prefix: "#SESSION" },
  connection: { dynamoDb: dynamoDbClient, tableName: dynamoDbTableName },
  validator: sessionValidator,
  ttl: "expiresAt",
});
```

### Write Sharding

If one partition key takes the lion's share of write traffic (think a single `teamId` with millions of tasks), DynamoDB will throttle that physical partition. Add a `shard` configuration to the key to spread writes across a fixed number of buckets:

```ts
.addIndex({
  index: Index.GSI1,
  PK: {
    keys: ["postStatus"],
    shard: { count: 4, keys: ["postId"] },
    prefix: "#STATUS",
  },
  SK: { keys: ["sendAt"], prefix: "#STATUS" },
  alias: "GSIStatusSendAt",
});
```

On write, Faceteer CRC-32 hashes the values of the `shard.keys` fields and prepends the hex shard id to the partition key. The id width is `(count - 1).toString(16).length` characters, so `count: 4` gives `"0".."3"` and `count: 256` gives `"00".."ff"`.

On read, you pass the shard number explicitly to `query(partition, shard)` (or via `options.shard` on an operator). Query every shard and merge client-side to get the full partition.

## FAQs

**Why do I have to pass every key field to `get`?**
DynamoDB identifies an item by the full `(PK, SK)` pair, and both are computed from the fields listed in `KeyConfiguration.keys`. Miss one and Faceteer can't build the composite key it needs to look the record up.

**Can I change a field that participates in PK or SK after writing?**
No. The composite key is the item's identity in DynamoDB; changing it means writing a new item and deleting the old one.

**Why does my query return fewer items than `limit`?**
Three likely reasons: you hit the 1 MB page ceiling, a `filter` removed results after the fact, or the partition just doesn't contain that many items. Check `cursor` — if it's defined there's more to read.

**Can two facets share the same GSI?**
Yes — this is called index overloading and it's a cornerstone of single-table design. Give each facet a distinct `prefix` on the index's `PK`/`SK` so your queries can target just the items you want.

**Are cursors durable across deploys or library upgrades?**
No. They're CBOR+base64 encodings of a DynamoDB `LastEvaluatedKey`. Treat them as session-local tokens; never store them somewhere a different build of your app might try to decode them.

**Does Faceteer retry on throttling?**
Only for the specific `UnprocessedItems`/`UnprocessedKeys` path inside batch writes and reads (5 retries for writes, 10 for reads, exponential backoff). Regular `ProvisionedThroughputExceededException` errors surface as rejected promises — wrap your calls with backoff if you expect sustained throttling.

**Does `put` validate my record before writing?**
Not by default; the validator runs on read. Pass `validateInput: true` to the facet constructor if you want validation to run on writes too. It has a per-call cost.

**Does `delete` support conditions?**
Only for single-item deletes: `facet.delete(record, { condition: [...] })`. `condition` passed alongside an array is silently ignored.

## Best Practices

- **Know your access patterns before you design the key.** Faceteer isn't built for ad-hoc queries. Every GSI costs writes, so pick the smallest set of indexes that covers the patterns you actually have.
- **Overload indexes with prefixes.** Two facets can share `GSI1` as long as their SK prefixes differ — that's the whole point of the `prefix` field on each `KeyConfiguration`. Differentiate by prefix, query by prefix.
- **Prefer key conditions over `filter`.** `equals`, `beginsWith`, and `between` prune reads on the server. `filter` runs server-side too but *after* the read is billed — it shrinks the response, not the cost.
- **Reach for `select` when payload size hurts.** `ProjectionExpression` cuts the wire payload but not the read-capacity cost. DynamoDB bills the full item size regardless. Worth using when you're rendering lists, hydrating caches, or returning over a slow link; not worth it when you need the full record anyway.
- **Avoid `greaterThan` and `lessThan` on composite sort keys.** They compare the full composite string, so a query like `greaterThan({ status: 'queued', timestamp: X })` bleeds into records whose status sorts after `queued`. Scope with `beginsWith`, or with `between` where both bounds pin the same leading value. See the "Composite sort keys" section above.
- **Shard hot partitions.** When one PK value attracts disproportionate traffic, add a `shard: { count, keys }` config. Keep `count` small to start (2, 4, 8); every shard is an extra query on read, so more shards is not free.
- **Use TTL for ephemeral data.** Sessions, OTPs, cache entries, and soft-delete markers all benefit. Remember AWS purges asynchronously — expired items can linger in query results for a while.
- **Keep indexes sparse.** Faceteer only writes values of primitive / `Date` fields into composite keys, so leaving an indexed field `undefined` keeps that record out of the index entirely. Use this to make cheap "only open orders" or "only active users" indexes.
- **Treat PK and SK as immutable.** Any field that contributes to a composite key can't change after write. Plan upfront, or plan the migration.
- **Respect the batch limits.** 25 items per write batch, 100 per get batch, and the 1 MB response ceiling applies to every individual request. Faceteer chunks for you, but very large operations still happen serially across many round-trips.
- **Wrap calls with retry logic for throttling.** Faceteer's internal retry only covers the "unprocessed items" path. Other throttles surface as rejected promises.
- **Use `condition` for optimistic concurrency.** Guard creations with `['field', 'not_exists']`, guard updates with version checks, guard deletes against races. Reminder: `condition` only applies to single-item calls.
- **Validate on read.** Schemas drift; the validator is your last line of defense before bad data reaches your business logic. Opt into `validateInput: true` only when you specifically want write-time validation.
