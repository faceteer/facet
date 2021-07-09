/* eslint-disable max-classes-per-file */
import {
	ExpressionAttributeNameMap,
	ExpressionAttributeValueMap,
} from 'aws-sdk/clients/dynamodb';
import { ConditionBeginsWith } from './begins-with/begins-with';
import { ConditionBetween } from './between/between';
import { BuiltCondition } from './built-condition';
import {
	ConditionComparison,
	ConditionOperations,
} from './comparison/comparison';
import { ConditionValue } from './condition-value';
import { ConditionContains } from './contains/contains';
import { ConditionExists } from './exists/exists';
import { ConditionIn } from './in/in';
import { ConditionNotExists } from './not-exists/not-exists';

type LogicalEvaluation = 'AND' | 'OR';

type ConditionType =
	| ConditionBeginsWith
	| ConditionBetween
	| ConditionComparison
	| ConditionContains
	| ConditionExists
	| ConditionNotExists
	| ConditionIn;

const addToConditionGroup = Symbol('Add to Condition Group');

/**
 * Represents a group of conditions for a Dynamo DB statement
 */
export class ConditionGroup {
	/**
	 * Contains all conditions for a conditional statement, including
	 * nested groups that may exist
	 */
	private conditions: ConditionStatement[] = [];

	constructor(conditionOrGroup: ConditionType | ConditionGroup, not: boolean) {
		this.conditions.push({
			condition: conditionOrGroup,
			not,
		});
	}

	/**
	 * This method will add a condition or another
	 * condition group to this condition group
	 *
	 * It uses a symbol because it's only used internally by this module
	 * @param logicalEvaluation
	 * @param conditionOrGroup
	 */
	[addToConditionGroup](
		logicalEvaluation: LogicalEvaluation,
		conditionOrGroup: ConditionType | ConditionGroup,
		not: boolean,
	) {
		this.conditions.push({
			logicalEvaluation,
			condition: conditionOrGroup,
			not,
		});
	}

	/**
	 * Start the next condition group
	 * @param key
	 * @param logicalEvaluation
	 * @param not
	 */
	private next(
		keyOrGroup: string | ConditionGroup,
		logicalEvaluation: LogicalEvaluation,
		not: boolean,
	): ConditionBuilder | ConditionGroup {
		/**
		 * If this is an existing condition group we'll
		 * go ahead and add it here
		 */
		if (keyOrGroup instanceof ConditionGroup) {
			this[addToConditionGroup](logicalEvaluation, keyOrGroup, not);
			return this;
		}

		return NextCondition(keyOrGroup, this, logicalEvaluation);
	}

	/**
	 * If this condition and the
	 * previous one are true this is true
	 *
	 * @param key
	 * @param options
	 */
	and(key: string): ConditionBuilder;
	/**
	 * If this condition and the
	 * previous one are true this is true
	 *
	 * @param group
	 * @param options
	 */
	and(group: ConditionGroup): ConditionGroup;
	and(keyOrGroup: string | ConditionGroup): ConditionBuilder | ConditionGroup {
		return this.next(keyOrGroup, 'AND', false);
	}

	/**
	 * If this condition and the
	 * previous one are true this is false
	 *
	 * @param key
	 * @param options
	 */
	andNot(key: string): ConditionBuilder;
	/**
	 * If this condition and the
	 * previous one are true this is false
	 *
	 * @param group
	 * @param options
	 */
	andNot(group: ConditionGroup): ConditionGroup;
	andNot(
		keyOrGroup: string | ConditionGroup,
	): ConditionBuilder | ConditionGroup {
		return this.next(keyOrGroup, 'AND', true);
	}

	/**
	 * If this condition or the
	 * previous one are true this is true
	 *
	 * @param key
	 * @param options
	 */
	or(key: string): ConditionBuilder;
	/**
	 * If this condition or the
	 * previous one are true this is true
	 *
	 * @param group
	 * @param options
	 */
	or(group: ConditionGroup): ConditionGroup;
	or(keyOrGroup: string | ConditionGroup): ConditionBuilder | ConditionGroup {
		return this.next(keyOrGroup, 'OR', false);
	}

	/**
	 * If this condition or the
	 * previous one are true this is true
	 *
	 * @param key
	 * @param options
	 */
	orNot(key: string): ConditionBuilder;
	/**
	 * If this condition or the
	 * previous one are true this is true
	 *
	 * @param group
	 * @param options
	 */
	orNot(group: ConditionGroup): ConditionGroup;
	orNot(
		keyOrGroup: string | ConditionGroup,
	): ConditionBuilder | ConditionGroup {
		return this.next(keyOrGroup, 'OR', false);
	}

	build(prefix: string[] = []): BuiltCondition {
		let statementCounter = 0;
		const conditionStatements: string[] = [];
		const attributeNames: ExpressionAttributeNameMap = {};
		const attributeValues: ExpressionAttributeValueMap = {};

		for (const condition of this.conditions) {
			/**
			 * If the condition expression has a logical statement
			 * like AND or OR, then we'll prefix it
			 */
			if (condition.logicalEvaluation) {
				conditionStatements.push(condition.logicalEvaluation);
			}
			/**
			 * If th condition has a not value then we'll
			 * prefix the condition with NOT
			 */
			if (condition.not) {
				conditionStatements.push('NOT');
			}

			/**
			 * We'll now build the condition or the condition group
			 */
			const builtCondition = condition.condition.build([
				...prefix,
				statementCounter.toString(16),
			]);
			/**
			 * We increment the counter to make sure all name and value
			 * placeholders are unique
			 */
			statementCounter += 1;
			Object.assign(attributeNames, builtCondition.attributeNames);
			Object.assign(attributeValues, builtCondition.attributeValues);
			/**
			 * If the condition is a condition group we'll build
			 * it and add it's attributes to the current attributes
			 */
			if (condition.condition instanceof ConditionGroup) {
				conditionStatements.push(`(${builtCondition.statement})`);
			} else {
				/**
				 * Otherwise we'll just add the condition to the string
				 */
				conditionStatements.push(builtCondition.statement);
			}
		}

		return {
			attributeNames,
			attributeValues,
			statement: conditionStatements.join(' '),
		};
	}

	toString(): string {
		let conditionString = '';
		/**
		 * Build the condition expression string for all
		 * conditions in this group
		 */
		for (const condition of this.conditions) {
			/**
			 * If the condition expression has a logical statement
			 * like AND or OR, then we'll prefix it
			 */
			if (condition.logicalEvaluation) {
				conditionString += ` ${condition.logicalEvaluation}`;
			}
			if (condition.not) {
				conditionString += ` NOT`;
			}
			/**
			 * If the condition is a condition group, we'll put it
			 * in parenthesis and add it to the string
			 */
			if (condition.condition instanceof ConditionGroup) {
				conditionString += ` (${condition.condition.toString()})`;
			} else {
				/**
				 * Otherwise we'll just add the condition to the string
				 */
				conditionString += ` ${condition.condition.toString()}`;
			}
		}

		return conditionString;
	}
}

/**
 * Intermediary class used when building a condition.
 */
class ConditionBuilder {
	private readonly key: string;

	private invert = false;

	private readonly existingGroup?: ExistingConditionGroupInfo;

	constructor(key: string, existingGroup?: ExistingConditionGroupInfo) {
		this.key = key;
		this.existingGroup = existingGroup;
	}

	/**
	 * Add a condition to the existing condition
	 * group or create a new condition group
	 * @param condition
	 */
	private addCondition(condition: ConditionType) {
		if (this.existingGroup) {
			this.existingGroup.conditionGroup[addToConditionGroup](
				this.existingGroup.logicalEvaluation,
				condition,
				this.invert,
			);
			return this.existingGroup.conditionGroup;
		}
		return new ConditionGroup(condition, this.invert);
	}

	/**
	 * Compare a key to a value
	 * @param operation
	 * @param value
	 */
	private compare(operation: ConditionOperations, value: ConditionValue) {
		return this.addCondition(
			new ConditionComparison({
				condition: operation,
				key: this.key,
				value,
			}),
		);
	}

	not() {
		this.invert = !this.invert;
		return this;
	}

	/**
	 * `true` if the attribute specified is equal to the value
	 * @param value
	 */
	equals(value: ConditionValue) {
		return this.compare('=', value);
	}

	/**
	 * `true` if the attribute specified is not equal to the value
	 * @param value
	 */
	notEquals(value: ConditionValue) {
		return this.compare('<>', value);
	}

	/**
	 * `true` if the attribute specified is less than the value
	 * @param value
	 */
	lessThan(value: ConditionValue) {
		return this.compare('<', value);
	}

	/**
	 * `true` if the attribute specified is less than or equal to the value
	 * @param value
	 */
	lessThanOrEquals(value: ConditionValue) {
		return this.compare('<=', value);
	}

	/**
	 * `true` if the attribute specified is greater than the value
	 * @param value
	 */
	greaterThan(value: ConditionValue) {
		return this.compare('>', value);
	}

	/**
	 * `true` if the attribute specified is greater than or equal to the value
	 * @param value
	 */
	greaterThanOrEquals(value: ConditionValue) {
		return this.compare('>=', value);
	}

	/**
	 * `true` if the attribute specified by path begins
	 * with a particular substring
	 * @param value
	 */
	beginsWith(value: ConditionValue) {
		return this.addCondition(
			new ConditionBeginsWith({
				key: this.key,
				value,
			}),
		);
	}

	/**
	 * `true` if the attribute specified is greater than or equal to the start
	 * and less than or equal to the end
	 * @param start
	 * @param end
	 */
	between(start: ConditionValue, end: ConditionValue) {
		return this.addCondition(
			new ConditionBetween({
				key: this.key,
				start,
				end,
			}),
		);
	}

	/**
	 * `true` if the attribute specified contains a particular substring
	 * @param start
	 * @param end
	 */
	contains(value: ConditionValue) {
		return this.addCondition(
			new ConditionContains({
				key: this.key,
				value,
			}),
		);
	}

	/**
	 * `true` if the specified attribute exists
	 */
	exists() {
		return this.addCondition(
			new ConditionExists({
				key: this.key,
			}),
		);
	}

	/**
	 * `true` if the specified attribute does not exist
	 */
	notExists() {
		return this.addCondition(
			new ConditionNotExists({
				key: this.key,
			}),
		);
	}

	/**
	 * `true` if the specified attribute is equal to any value
	 * in the array
	 */
	in(inArray: ConditionValue[]) {
		return this.addCondition(
			new ConditionIn({
				key: this.key,
				in: inArray,
			}),
		);
	}
}

/**
 * Options for for creating a new condition group
 */
export interface ConditionOptions {
	/**
	 * Whether to invert the condition and
	 * put `NOT` before it
	 */
	not?: boolean;
}
/**
 * Create a condition statement for Dynamo DB
 *
 * @param key
 * @param options
 * @returns
 */
export function Condition(key: string): ConditionBuilder;
/**
 * Create a condition statement for Dynamo DB
 *
 * @param group
 * @param options
 * @returns
 */
export function Condition(
	group: ConditionGroup,
	options?: ConditionOptions,
): ConditionGroup;
export function Condition(
	keyOrGroup: string | ConditionGroup,
	{ not = false }: ConditionOptions = {},
): ConditionBuilder | ConditionGroup {
	if (keyOrGroup instanceof ConditionGroup) {
		return new ConditionGroup(keyOrGroup, not);
	}
	return new ConditionBuilder(keyOrGroup);
}

/**
 * Generate the next condition in a chain
 * @param key
 * @param conditionGroup
 * @param logicalEvaluation
 * @param not
 * @returns
 */
function NextCondition(
	key: string,
	conditionGroup: ConditionGroup,
	logicalEvaluation: LogicalEvaluation,
) {
	return new ConditionBuilder(key, {
		conditionGroup,
		logicalEvaluation,
	});
}

/**
 * A condition or a group of conditions with
 * an optional logical operator before it.
 */
interface ConditionStatement {
	not: boolean;
	logicalEvaluation?: LogicalEvaluation;
	condition: ConditionType | ConditionGroup;
}

/**
 * Used to keep track if whether or not
 * this condition we're building has an existing
 * group or if it has a new group
 */
interface ExistingConditionGroupInfo {
	logicalEvaluation: LogicalEvaluation;
	conditionGroup: ConditionGroup;
}
