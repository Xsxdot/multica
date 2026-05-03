// Package inbound implements the channel-layer inbound event pipeline.
//
// Red phase: the package contains only this doc file so the test files
// are compiled by `go test`. All symbols referenced from the tests
// (Pipeline, Step, Decision, NewDedupStep, DedupStore, …) are defined
// during the Green phase. Without this stub, `go test` would short-circuit
// with "no non-test Go files" before reaching the missing-symbol errors
// the tests are designed to surface.
package inbound
