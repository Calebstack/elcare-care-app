const { withSentryConfig } = require("@sentry/nextjs");
const { readFileSync } = require("fs");
const { join } = require("path");

// Read version metadata from versions.toml at build time
function loadVersions() {
  try {
    const tomlPath = join(__dirname, "../../versions.toml");
    const toml = readFileSync(tomlPath, "utf-8");
    const get = (key) => {
      const match = toml.match(new RegExp(`^${key}\\s*=\\s*"?([^"\\n]+)"?`, "m"));
      return match ? match[1].trim() : "";
    };
    return {
      APP_VERSION: get("components\\.frontend\\.version") || process.env.npm_package_version || "0.1.0",
      INDEXER_API_VERSION: get("components\\.indexer\\.api_version") || "1.0.0",
      EVENT_SCHEMA_VERSION: get("components\\.event_schema\\.version") || "1",
    };
  } catch {
    return {
      APP_VERSION: process.env.npm_package_version || "0.1.0",
      INDEXER_API_VERSION: "1.0.0",
      EVENT_SCHEMA_VERSION: "1",
    };
  }
}

const versions = loadVersions();

// ── Security header helpers ───────────────────────────────────────────────────

/**
 * Build the Content-Security-Policy value from runtime environment variables.
 *
 * The policy starts in report-only mode (see the `headers()` function below).
 * To switch to enforcement: replace 'Content-Security-Policy-Report-Only' with
 * 'Content-Security-Policy' in the headers array after reviewing collected
 * violations at /api/csp-report.
 *
 * Allowed external origins and their ownership:
 *   IPFS / Pinata      — NFT metadata and image hosting (operator-owned)
 *   Stellar RPC        — Soroban RPC + Horizon REST (Stellar Foundation)
 *   Sentry             — Error monitoring (third-party; DSN set at build time)
 *   PostHog            — Analytics (third-party; key set at build time)
 *   Magic.link         — Wallet abstraction (third-party; key set at build time)
 *   Indexer            — This project's own event-indexer API
 *
 * Note on script-src:
 *   'unsafe-inline' is required by browser wallet extensions (Freighter, xBull, etc.)
 *   that inject inline scripts. 'wasm-unsafe-eval' is required by the Stellar SDK's
 *   WASM crypto module. These cannot be removed without breaking wallet connectivity.
 *   Nonce-based enforcement is the long-term goal; see docs/guides/security-headers.md.
 */
function buildCsp() {
  const indexerUrl = process.env.NEXT_PUBLIC_INDEXER_URL || "";
  const stellarRpc = process.env.NEXT_PUBLIC_STELLAR_RPC_URL || "https://soroban-testnet.stellar.org";
  const stellarHorizon = process.env.NEXT_PUBLIC_STELLAR_HORIZON_URL || "https://horizon-testnet.stellar.org";
  const posthogHost = process.env.NEXT_PUBLIC_POSTHOG_HOST || "https://app.posthog.com";
  const pinataGateway = process.env.NEXT_PUBLIC_PINATA_GATEWAY || "https://gateway.pinata.cloud";
  const isProd = process.env.NODE_ENV === "production";

  const connectSrc = [
    "'self'",
    indexerUrl,
    stellarRpc,
    stellarHorizon,
    "https://*.stellar.org",
    "wss://*.stellar.org",
    posthogHost,
    "https://eu.posthog.com",
    "https://*.sentry.io",
    "https://o*.ingest.sentry.io",
    pinataGateway,
    "https://*.pinata.cloud",
    "https://*.mypinata.cloud",
    "https://ipfs.io",
    "https://*.magic.link",
    "https://fortmatic.com",
  ]
    .filter(Boolean)
    .join(" ");

  const imgSrc = [
    "'self'",
    "data:",
    "blob:",
    pinataGateway,
    "https://*.pinata.cloud",
    "https://*.mypinata.cloud",
    "https://ipfs.io",
    "https://images.unsplash.com",
  ].join(" ");

  const directives = [
    "default-src 'self'",
    // unsafe-eval: Next.js dev HMR + Stellar WASM; unsafe-inline: wallet extensions
    `script-src 'self' 'unsafe-eval' 'unsafe-inline' 'wasm-unsafe-eval' blob:${!isProd ? " http://localhost:*" : ""}`,
    "style-src 'self' 'unsafe-inline'",
    `img-src ${imgSrc}`,
    "font-src 'self' data:",
    `connect-src ${connectSrc}`,
    "frame-src 'none'",
    "object-src 'none'",
    "base-uri 'self'",
    "form-action 'self'",
    "worker-src 'self' blob:",
    "manifest-src 'self'",
    // CSP violation reports are collected by /api/csp-report (bounded in-memory
    // buffer in dev; wire to an external sink in production).
    "report-uri /api/csp-report",
  ];

  return directives.join("; ");
}

/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: {
    outputFileTracingRoot: require("path").resolve(__dirname, "../../"),
  },
  reactStrictMode: true,
  async headers() {
    const isProd = process.env.NODE_ENV === "production";
    const csp = buildCsp();

    const securityHeaders = [
      // Prevent the browser from guessing content types (MIME sniffing attacks).
      { key: "X-Content-Type-Options", value: "nosniff" },
      // Deny all framing to prevent clickjacking.
      { key: "X-Frame-Options", value: "DENY" },
      // Disable DNS prefetching to reduce information leakage.
      { key: "X-DNS-Prefetch-Control", value: "off" },
      // Only send the origin (no path/query) in the Referer header to third parties.
      { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
      // Restrict browser features to only what the app actually uses.
      {
        key: "Permissions-Policy",
        value: "camera=(), microphone=(), geolocation=(), payment=(), usb=(), interest-cohort=()",
      },
      // CSP in report-only mode: violations are logged to /api/csp-report without
      // blocking anything. Review reports, then switch to 'Content-Security-Policy'
      // to enforce. See docs/guides/security-headers.md for the migration checklist.
      { key: "Content-Security-Policy-Report-Only", value: csp },
    ];

    if (isProd) {
      // HSTS: tell browsers to use HTTPS for 2 years. Only safe when TLS is
      // terminated at the load balancer/CDN before this Next.js instance.
      securityHeaders.push({
        key: "Strict-Transport-Security",
        value: "max-age=63072000; includeSubDomains; preload",
      });
    }

    return [{ source: "/(.*)", headers: securityHeaders }];
  },
  env: {
    NEXT_PUBLIC_APP_VERSION: versions.APP_VERSION,
    NEXT_PUBLIC_INDEXER_API_VERSION: versions.INDEXER_API_VERSION,
    NEXT_PUBLIC_EVENT_SCHEMA_VERSION: versions.EVENT_SCHEMA_VERSION,
  },
  images: {
    remotePatterns: [
      {
        protocol: "https",
        hostname: "**.pinata.cloud",
      },
      {
        protocol: "https",
        hostname: "ipfs.io",
      },
      {
        protocol: "https",
        hostname: "**.mypinata.cloud",
      },
      {
        protocol: "https",
        hostname: "images.unsplash.com",
      },
    ],
  },
  webpack: (config, { isServer }) => {
    if (!isServer) {
      // Stub out Node-only modules that @stellar/stellar-sdk pulls in
      // transitively (sodium-native, libsodium-wrappers, etc.).
      // Without these stubs, the client bundle emits critical warnings and
      // includes dead code that inflates the bundle size.
      config.resolve.fallback = {
        ...config.resolve.fallback,
        // Standard Node builtins
        fs: false,
        net: false,
        tls: false,
        // Stellar SDK native crypto modules — not available in the browser;
        // the SDK falls back to its wasm/js implementation automatically.
        "sodium-native": false,
        "libsodium-wrappers": false,
        // Other optional native deps pulled by stellar-base / stellar-sdk
        crypto: false,
      };
    }
    return config;
  },
  // Suppress the expected "Can't resolve 'sodium-native'" critical warnings
  // that Next.js surfaces from @stellar/stellar-sdk's optional native crypto.
  // These are intentional — the browser bundle uses the wasm fallback instead.
  //
  // Note: Next 15 exposes `ignoreDuringBuilds` under experimental — once stable
  // we can replace the webpack fallback stubs with a cleaner filterWarnings rule.
};

module.exports = withSentryConfig(nextConfig, {
  // For all available options, see:
  // https://github.com/getsentry/sentry-webpack-plugin#options

  org: process.env.SENTRY_ORG,
  project: process.env.SENTRY_PROJECT,

  // Only print logs for uploading source maps in CI
  silent: !process.env.CI,

  // For all available options, see:
  // https://docs.sentry.io/platforms/javascript/guides/nextjs/manual-setup/

  // Upload a larger set of source maps for prettier stack traces (increases build time)
});
