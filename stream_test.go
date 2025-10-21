package nominal

import (
	"testing"
)

func TestGetChannelStream_ReturnsSameInstance(t *testing.T) {
	client, err := NewClient(WithAPIKey("test-key"))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	stream, err := client.NewStream("ri.nominal.main.dataset.test")
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	// Get a channel stream twice with same parameters
	cs1 := stream.FloatStream("temperature", Tags{"sensor": "A1"})
	cs2 := stream.FloatStream("temperature", Tags{"sensor": "A1"})

	// They should be the exact same instance
	if cs1 != cs2 {
		t.Error("FloatStream should return the same instance for identical parameters")
	}

	// Different tags should return different instance
	cs3 := stream.FloatStream("temperature", Tags{"sensor": "A2"})
	if cs1 == cs3 {
		t.Error("FloatStream should return different instances for different tags")
	}

	// Different type should return different instance (even if same channel+tags)
	cs4 := stream.IntStream("temperature", Tags{"sensor": "A1"})
	if cs4 == nil {
		t.Error("IntStream should return a valid instance")
	}
}
