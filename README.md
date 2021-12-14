# Faceteer

[![codecov](https://codecov.io/gh/faceteer/facet/branch/main/graph/badge.svg?token=ORKKZWL5N6)](https://codecov.io/gh/faceteer/facet)
[![Test](https://github.com/faceteer/facet/actions/workflows/test.yml/badge.svg)](https://github.com/faceteer/facet/actions/workflows/test.yml)
[![npm version](https://img.shields.io/npm/v/@faceteer/facet)](https://www.npmjs.com/package/@faceteer/facet)

Faceteer is meant to make it easier to work with a single table DynamoDB architecture.

The structure and records that are kept in a Dynamo DB table are more so designed around how you access the data instead of the structure of the data itself.

If you're not familiar with the Dynamo DB single table concepts, this video is a good starting point.

[Amazon DynamoDB deep dive](https://www.youtube.com/watch?v=6yqfmXiZTlM)

## What is a Facet?

DynamoDB does not natively have the concept of a `Facet`. Instead, a `Facet` is a pattern used when designing a single table DynamoDB schema.

In a single table DynamoDB schema your partition key and sort keys are named `PK` and `SK`. The partition and sort keys for GSIs will also be named generically as `GSI1PK`, `GSISK1`, `GSI2PK`, `GSI2SK`, etc.

By computing the partition and sort keys for a model, many different data models can use a single DynamoDB table. We can also store related data within the same partition.

The `Facet` is simply a data model with instructions on how to build the partition and sort keys for the model.

### Example

Let's say we're building an application that tracks reservations for meeting rooms for our organization.

There are many building that have meeting rooms, and the meeting rooms have a unique number in the building.

First lets create the models for a `Building` and a `Room`.

```ts
export interface Building {
  id: number;
  name: string;
  city: string;
  state: string;
}

export interface Room {
  id: number;
  buildingId: number;
  floor: number;
  capacity: number;
}
```

The actual records stored in DynamoDB under our single table pattern might look like this:

```ts
{
  PK: "#BUILDING_1",
  SK: "#BUILDING_1",
  id: 1,
  name: "The Strand",
  city: "Brooklyn",
  state: "NY",
}

{
  PK: "#BUILDING_1",
  SK: "#ROOM_7",
  id: 7,
  buildingId: 1,
  floor: 12,
  capacity: 8,
}
```
