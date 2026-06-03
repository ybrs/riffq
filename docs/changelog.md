# Changelog

This file captures notable updates to the project and documentation. Replace the placeholder entries with real releases as they happen.

## Unreleased

- Added `Server.handle_shutdown(callback)` (also on `RiffqServer`): a callback
  invoked once when the server shuts down. riffq now catches **SIGTERM** in
  addition to SIGINT inside its Rust runtime and runs the callback before
  `start()` returns, enabling clean flush/checkpoint on `kill`/`docker stop`/k8s.
- Added MkDocs-based documentation scaffolding with API reference generation.
- Created starter guides for onboarding, FAQs, and release tracking.

## 0.1.0 - Initial release

- Bootstrapped the project with Rust and Python integration.
