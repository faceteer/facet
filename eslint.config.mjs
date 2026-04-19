import js from '@eslint/js';
import { defineConfig } from 'eslint/config';
import tseslint from 'typescript-eslint';
import importX from 'eslint-plugin-import-x';
import eslintConfigPrettier from 'eslint-config-prettier/flat';

export default defineConfig(
	{
		ignores: [
			'lib/**/*.js',
			'lib/**/*.d.ts',
			'index.js',
			'index.d.ts',
			'coverage/**',
			'docs/**',
			'_scratch/**',
			'*.tsbuildinfo',
		],
	},
	js.configs.recommended,
	tseslint.configs.strictTypeChecked,
	tseslint.configs.stylisticTypeChecked,
	{
		languageOptions: {
			parserOptions: {
				projectService: {
					allowDefaultProject: ['eslint.config.mjs'],
				},
				tsconfigRootDir: import.meta.dirname,
			},
		},
		plugins: {
			'import-x': importX,
		},
		rules: {
			'import-x/no-cycle': 'error',
			'import-x/no-extraneous-dependencies': 'error',
			'@typescript-eslint/restrict-template-expressions': [
				'error',
				{ allowNumber: true },
			],
		},
	},
	{
		files: ['**/*.test.ts'],
		rules: {
			'@typescript-eslint/require-await': 'off',
			'@typescript-eslint/no-misused-promises': 'off',
			'@typescript-eslint/no-unused-vars': [
				'error',
				{ varsIgnorePattern: '^_' },
			],
		},
	},
	eslintConfigPrettier,
);
