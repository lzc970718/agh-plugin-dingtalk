# Validation — 2026-11-06 — 0.3.2

Windows x64, Node v24.20.0 and pnpm 10.34.5, through the Harness fixed toolchain entry.

- 8 files / 69 tests and TypeScript validation cover the standalone adapter.
- The full simulated reply path uses the real DingTalk gateway implementation and adapter with an injected Stream transport and HTTP server responses: app credentials only, authenticated callback robotCode, incoming DM -> existing bound AGH session -> projected assistant response -> DingTalk batchSend endpoint.
- Requests use the observed robotCode and the sender's staffId, contain a non-empty title and the assistant response, exclude pre-binding history, and do not create channel-owned sessions or invoke card APIs when no template is configured.
- Explicit robotCode overrides callbacks; changing credentials clears learned identity and rejects callbacks from the retired credential version. Existing card, attachment, reconnect and callback tests remain covered.
- No public AGH API, new dependencies or credential grants are introduced. No live DingTalk message or user installation was changed. Real platform delivery requires manual verification after updating the plugin.

## Marketplace release 0.3.8 (2026-11-09)

- Executed on Node v24.20.0 with the AGH repository toolchain entry.
- prepareSdk, typecheck, tests and the independent runtime build passed.
- Unit tests: 89 passed. Legacy 5.7 error-message assertions in DingTalk/Feishu were aligned with the existing 5.8 implementation requirement.
- Release files are explicitly staged; generated SDK paths, node_modules, backup directories, local logs and prior archives are excluded.
- This release changes distribution metadata, portable development configuration, documentation and version stamps; it does not add platform capabilities or establish new live-account/macOS/Linux acceptance.
