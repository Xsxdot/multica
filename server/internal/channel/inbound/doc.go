// Package inbound implements the channel-layer inbound event pipeline.
//
// The pipeline is the chokepoint every InboundEvent flows through after
// an adapter (e.g. Feishu, T5) emits it on its Events() channel. It is
// organised as an ordered list of Steps:
//
//	normalize → dedup → identity-bind → intent-recog → dispatch → reply
//
// Each Step decides whether to Continue (advance to the next Step) or
// Skip (terminate the pipeline cleanly). Errors abort the pipeline and
// propagate to the caller.
//
// In M1 (T6) the intent-recog and dispatch steps are placeholders that
// will be replaced in T9–T11; the orchestration here does not need to
// change when those land.
package inbound
