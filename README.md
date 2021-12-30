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

We will be creating a

### Install

Install with npm:

```
npm i @faceteer/facet --save
```

### Access Patterns

Knowing and identifying your access patterns for your application is critical for a single-table design. Faceteer is not well suited for ad-hoc queries.

In our example we will be creating an application that keeps track of tasks for a team of people.

Our application should support multiple teams, teams should have have multiple users, and team tasks should be either unassigned or assigned to an individual user.

The access patterns we need to support are:

- _"Get a team by it's id"_
- _"Get a user by their id"_
- _"Get a user by their email address"_
- _"Get a task by it's id"_
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

Faceteer requires validation when reading records from the Dynamo DB table.

This is done using a validator function that is passed into a facet when constructing it.

The function should be able to take an unkown input and return an object that matches the model for the facet or throw an error if the input is invalid.

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

```ts
import { Facet } from "@faceteer/facet";
import { teamValidator, userValidator, taskValidator } from "./models";
import { DynamoDB } from "aws-sdk";

const dynamoDbClient = new DynamoDB();
const dynamoDbTableName = "ExampleTableName";

// Facet containing our teams
const TeamFacet = new Facet({
  PK: {
    keys: ["teamId"],
    prefix: "#TEAM",
  },
  SK: {
    keys: ["teamId"],
    prefix: "#TEAM",
  },
  connection: {
    tableName: "TableName",
    dynamoDb: new DynamoDB(),
  },
  validator: teamValidator,
});

// Facet containing our users
const UserFacet = new Facet({
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

We can also put several records at once. Faceteer will handle batching the put requests.

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
      handleFailure(putFailures);
    }
  }

  // Return the users that were successfully created
  return putResult.put;
}
```

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

One of our access patterns was _"List users by team ordered by the date created"_, but our sort key is ordered by the `userId`.

One option would be to include the user's `dateCreated` as a part of the sort key for a user.

```diff
const UserFacet = new Facet({
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

The first restriction is probably fine for this use case since it's unlikely that we'll ever change the users created date, but it is clunky to identify a user by when they were created.

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
  alias: "byTeamDueDate",
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

We can also use the alias `byTeamDueDate` instead of `GSI1` for readability.

```diff
export async function getPastDueTasks(teamId: string) {
	const today = new Date();
-	const queryResult = await TaskFacet.GSI1.query({ teamId }).lessThan({
+	const queryResult = await TaskFacet.byTeamDueDate.query({ teamId }).lessThan({
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
    alias: "byTeamDueDate",
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
    alias: "byUserStatusCreated",
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
    alias: "byUserStatusDue",
  });
```

- [x] _"List tasks by team ordered by the date they were created"_
- [x] _"List tasks by team ordered by the date they are due"_
- [x] _"List tasks by user and by status ordered by the date they were created"_
- [x] _"List tasks by user and by status ordered by the date they are due"_

### Pagination

## FAQs

## Best Practices

```

```
