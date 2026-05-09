package feishu

import (
	"context"
	"testing"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func TestRealClient_StopThenLateEvent_DropsWithoutPanic(t *testing.T) {
	t.Parallel()

	rc := NewRealClient("cli_test", "secret", "", "")
	if err := rc.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	rc.handleMessageReceive(context.Background(), &larkim.P2MessageReceiveV1{
		EventV2Base: &larkevent.EventV2Base{
			Header: &larkevent.EventHeader{EventID: "evt-after-stop"},
		},
	})

	select {
	case _, ok := <-rc.Subscribe():
		if ok {
			t.Fatal("Subscribe returned an event after Stop")
		}
	default:
		t.Fatal("Subscribe channel should be closed after Stop")
	}
}
