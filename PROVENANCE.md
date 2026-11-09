# Provenance

This independent Apache-2.0 DingTalk adapter derives from the original Agnes Harness
`packages/channels/src/adapters/dingtalk` implementation and its focused adapter tests.
Platform rendering, reconnect backoff and throttling helpers remain with the adapter.
The generic forwarding runner, SDK client, native module, credential-file reader and daemon
discovery are not shipped in the 0.2.0 package. Runtime contracts are imported from the Host's
public `@agnes/plugin-runtime/channel-contract` namespace to preserve kind token identity.

Bundled npm dependencies retain their license texts in NOTICE, generated from esbuild's actual
input graph. The source retains the original verification document; fake-gateway success must
not be represented as live DingTalk verification.

## DingTalk brand artwork

`assets/dingtalk.png` was downloaded unchanged on 2026-11-06 from the URL supplied by the user:
https://app-center.cdn.bcebos.com/appcenter/sts/pcfile/1131328208/2116a9e74ac5990c068e0c5030e4faad.png

This source is recorded for provenance only and is never fetched by the plugin at runtime. DingTalk brand rights remain with their respective owner; the project code license does not relicense the logo.
