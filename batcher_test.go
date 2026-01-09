package nominal_streaming

import (
	"math"
	"strings"
	"testing"
)

// =============================================================================
// Tests for timestamp conversion (validates the negative nanoseconds fix)
// =============================================================================

func TestTimestampConversion_NegativeNanoseconds(t *testing.T) {
	tests := []struct {
		name        string
		nanos       NanosecondsUTC
		wantSeconds int64
		wantNanos   int32
	}{
		{"positive timestamp", 1704067200000000000, 1704067200, 0},
		{"positive with fractional", 1704067200123456789, 1704067200, 123456789},
		{"negative -1 nanosecond", -1, -1, 999999999},
		{"negative -1 second", -1_000_000_000, -1, 0},
		{"negative -1 day", -86400_000_000_000, -86400, 0},
		{"negative -1.5 seconds", -1_500_000_000, -2, 500000000},
		{"zero", 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := nanosecondsToTimestampProto(tt.nanos)

			// Verify nanos is in valid protobuf range [0, 999999999]
			if ts.Nanos < 0 || ts.Nanos > 999999999 {
				t.Errorf("INVALID PROTOBUF: nanos=%d outside [0, 999999999]", ts.Nanos)
			}

			if ts.Seconds != tt.wantSeconds {
				t.Errorf("seconds = %d, want %d", ts.Seconds, tt.wantSeconds)
			}
			if ts.Nanos != tt.wantNanos {
				t.Errorf("nanos = %d, want %d", ts.Nanos, tt.wantNanos)
			}
		})
	}
}

// =============================================================================
// Tests for NaN/Inf validation (validates the fix)
// =============================================================================

func TestConvertFloatBatchToProto_RejectsNaN(t *testing.T) {
	batch := floatBatch{
		Channel:    "test",
		Tags:       map[string]string{},
		Timestamps: []NanosecondsUTC{1, 2, 3},
		Values:     []float64{1.0, math.NaN(), 3.0},
	}

	_, err := convertFloatBatchToProto(batch)
	if err == nil {
		t.Error("expected error for NaN value")
	}
	if err != nil && !strings.Contains(err.Error(), "NaN") {
		t.Errorf("expected error to mention NaN, got: %v", err)
	}
}

func TestConvertFloatBatchToProto_RejectsInf(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{"+Inf", math.Inf(1)},
		{"-Inf", math.Inf(-1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := floatBatch{
				Channel:    "test",
				Tags:       map[string]string{},
				Timestamps: []NanosecondsUTC{1},
				Values:     []float64{tt.value},
			}

			_, err := convertFloatBatchToProto(batch)
			if err == nil {
				t.Error("expected error for Inf value")
			}
			if err != nil && !strings.Contains(err.Error(), "Inf") {
				t.Errorf("expected error to mention Inf, got: %v", err)
			}
		})
	}
}

func TestConvertFloatArrayBatchToProto_RejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		values [][]float64
	}{
		{"NaN in array", [][]float64{{1.0, math.NaN(), 3.0}}},
		{"+Inf in array", [][]float64{{1.0, 2.0}, {3.0, math.Inf(1)}}},
		{"-Inf in array", [][]float64{{math.Inf(-1)}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := floatArrayBatch{
				Channel:    "test",
				Tags:       map[string]string{},
				Timestamps: make([]NanosecondsUTC, len(tt.values)),
				Values:     tt.values,
			}
			for i := range batch.Timestamps {
				batch.Timestamps[i] = NanosecondsUTC(i + 1)
			}

			_, err := convertFloatArrayBatchToProto(batch)
			if err == nil {
				t.Error("expected error for invalid float value")
			}
		})
	}
}

// =============================================================================
// Tests for batch conversion edge cases
// =============================================================================

func TestConvertBatchToProto_TimestampValueMismatch(t *testing.T) {
	t.Run("float batch", func(t *testing.T) {
		batch := floatBatch{
			Channel:    "test",
			Tags:       map[string]string{},
			Timestamps: []NanosecondsUTC{1, 2, 3},
			Values:     []float64{1.0, 2.0}, // Mismatch
		}
		_, err := convertFloatBatchToProto(batch)
		if err == nil {
			t.Error("expected error for mismatch")
		}
	})

	t.Run("int batch", func(t *testing.T) {
		batch := intBatch{
			Channel:    "test",
			Tags:       map[string]string{},
			Timestamps: []NanosecondsUTC{1, 2},
			Values:     []int64{1, 2, 3}, // Mismatch
		}
		_, err := convertIntBatchToProto(batch)
		if err == nil {
			t.Error("expected error for mismatch")
		}
	})

	t.Run("string batch", func(t *testing.T) {
		batch := stringBatch{
			Channel:    "test",
			Tags:       map[string]string{},
			Timestamps: []NanosecondsUTC{1},
			Values:     []string{"a", "b"}, // Mismatch
		}
		_, err := convertStringBatchToProto(batch)
		if err == nil {
			t.Error("expected error for mismatch")
		}
	})
}
