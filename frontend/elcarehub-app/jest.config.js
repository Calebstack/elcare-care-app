const path = require('path')

/**
 * Fully explicit jest@30 config — bypasses next/jest's createJestConfig wrapper
 * which has async-resolution incompatibilities with jest@30 when run from a
 * npm workspace root.
 *
 * SWC transform is applied directly via next/jest's own transform package.
 */
module.exports = {
  testEnvironment: 'jest-environment-jsdom',

  setupFilesAfterEnv: ['<rootDir>/jest.setup.js'],

  testPathIgnorePatterns: [
    path.resolve(__dirname, 'e2e'),
    path.resolve(__dirname, 'tests/e2e'),
  ],

  moduleNameMapper: {
    // Path alias
    '^@/(.*)$': '<rootDir>/src/$1',
    // Workspace package not linked at frontend level — mock for tests
    '^@elcarehub/config$': '<rootDir>/src/__mocks__/configMock.js',
    // Static file mocks
    '\\.(jpg|jpeg|png|gif|webp|svg|ico)$': '<rootDir>/src/__mocks__/fileMock.js',
    '\\.(css|less|scss|sass)$': '<rootDir>/src/__mocks__/fileMock.js',
  },

  transform: {
    // Use next/babel for tsx/ts/js transformation (avoids needing SWC separately)
    '^.+\\.(js|jsx|ts|tsx)$': ['babel-jest', {
      presets: [
        ['next/babel', { 'preset-env': { targets: { node: 'current' } } }],
      ],
    }],
  },

  transformIgnorePatterns: [
    '/node_modules/(?!(.*\\.mjs$))',
  ],

  collectCoverage: process.env.CI === 'true' || process.env.COLLECT_COVERAGE === 'true',
  collectCoverageFrom: [
    'src/**/*.{js,jsx,ts,tsx}',
    '!src/**/*.d.ts',
    '!src/**/__tests__/**',
    '!src/app/**/layout.tsx',
    '!src/app/**/loading.tsx',
    '!src/app/**/error.tsx',
    '!src/app/**/not-found.tsx',
  ],
  coverageDirectory: 'coverage',
  coverageReporters: ['text', 'text-summary', 'lcov', 'json-summary'],
  coverageThreshold: {
    // ── Global thresholds (ratchet — do not lower without explicit review) ────
    // Raised from 60/50/55/60 → 65/55/60/65 as new tests land (Issue #10).
    // See docs/COVERAGE_POLICY.md for the ratchet process.
    global:                               { statements: 65, branches: 55, functions: 60, lines: 65 },
    // ── Per-file thresholds for high-risk financial paths ─────────────────────
    './src/components/CheckoutModal.tsx': { statements: 90, branches: 75, functions: 85, lines: 90 },
    './src/components/ListingCard.tsx':   { statements: 90, branches: 75, functions: 85, lines: 90 },
    './src/hooks/useMarketplace.ts':      { statements: 55, branches: 45, functions: 50, lines: 55 },
    './src/lib/contract.ts':              { statements: 15, branches: 10, functions: 10, lines: 15 },
    // Issue #7: IPFS utilities are now comprehensively tested
    './src/lib/ipfs.ts':                  { statements: 80, branches: 65, functions: 80, lines: 80 },
    // Work item C additions
    './src/lib/disclosures.ts':           { statements: 80, branches: 70, functions: 75, lines: 80 },
    './src/lib/support.ts':               { statements: 80, branches: 70, functions: 75, lines: 80 },
  },
};
