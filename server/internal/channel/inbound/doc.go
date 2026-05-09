// Package inbound implements the channel-layer inbound event pipeline.
//
// The pipeline is the chokepoint every InboundEvent flows through after
// an adapter (e.g. Feishu, T5) emits it on its Events() channel. It is
// organised as an ordered list of Steps:
//
//	normalize → dedup → identity-bind → slash_expand → intent-recog → dispatch → reply
//
// Each Step decides whether to Continue (advance to the next Step) or
// Skip (terminate the pipeline cleanly). Errors abort the pipeline and
// propagate to the caller.
//
// intent-recog is a resolver chain: command-expanded text, deterministic
// rules, then an optional chat semantic resolver.
package inbound
