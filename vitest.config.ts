import { defineConfig } from 'vitest/config';

export default defineConfig({
	test: {
		globals: true,
		include: ['**/*.test.ts'],
		fileParallelism: false,
		reporters: ['default', 'junit'],
		outputFile: 'junit.xml',
		coverage: {
			provider: 'v8',
			reporter: ['text', 'lcov'],
		},
	},
});
