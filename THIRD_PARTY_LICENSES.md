# Third-Party License Inventory

`THIRD_PARTY_LICENSES.csv` is generated from the Go dependency graph with `github.com/google/go-licenses/v2@v2.0.1`.

The generation and policy check are automated by `scripts/check-licenses.sh`. The check rejects dependencies classified as unknown, restricted, or forbidden. Dependencies whose module archives do not expose a machine-detectable license use the explicit, reviewed entries in `third_party/license-overrides.csv`; no automatic fallback is permitted.

Review the generated inventory, every override, and the linked license sources before a public release.

This inventory is technical due-diligence material, not legal advice or a legal conclusion.
