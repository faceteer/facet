/**
 *	Wait a certain amount of time
 * @param milliseconds
 */
export function wait(milliseconds: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
