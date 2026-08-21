/**
 * Creates (or resets) the helpdesk table on DynamoDB Local with the
 * PK/SK schema and GSI1-GSI3 that the facets in src/facets.ts expect.
 *
 * Run with: npx tsx setup-table.ts
 */
import { dynamoDb, tableName } from './src/config.js';

const gsis = ['GSI1', 'GSI2', 'GSI3'];

async function main() {
	try {
		await dynamoDb.deleteTable({ TableName: tableName });
	} catch {
		// table did not exist
	}

	await dynamoDb.createTable({
		TableName: tableName,
		BillingMode: 'PAY_PER_REQUEST',
		AttributeDefinitions: [
			{ AttributeName: 'PK', AttributeType: 'S' },
			{ AttributeName: 'SK', AttributeType: 'S' },
			...gsis.flatMap((gsi) => [
				{ AttributeName: `${gsi}PK`, AttributeType: 'S' as const },
				{ AttributeName: `${gsi}SK`, AttributeType: 'S' as const },
			]),
		],
		KeySchema: [
			{ AttributeName: 'PK', KeyType: 'HASH' },
			{ AttributeName: 'SK', KeyType: 'RANGE' },
		],
		GlobalSecondaryIndexes: gsis.map((gsi) => ({
			IndexName: gsi,
			KeySchema: [
				{ AttributeName: `${gsi}PK`, KeyType: 'HASH' },
				{ AttributeName: `${gsi}SK`, KeyType: 'RANGE' },
			],
			Projection: { ProjectionType: 'ALL' },
		})),
	});

	console.log(`Table ${tableName} created.`);
}

await main();
