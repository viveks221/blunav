import globals from 'globals';
import pluginJs from '@eslint/js';

/** @type {import('eslint').Linter.Config[]} */
export default [
    {
        ignores: [
            'node_modules/',
            'dist/',
            'coverage/',
            '.git/',
            '**/*.min.js',
            'src/database/migrations/**',
            'docker/**',
        ],
    },
    { languageOptions: { globals: { ...globals.node, ...globals.jest } } },
    pluginJs.configs.recommended,
    {
        rules: {
            'no-unused-vars': ['error', { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }],
        },
    },
];
