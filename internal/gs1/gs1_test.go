package gs1_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/veritrace-platform/telemetry-stream-service/internal/gs1"
)

// vectors is testdata/gs1-check-digit.json, a copy of veritrace/docs/contracts/test-vectors/gs1-check-digit.json.
type vectors struct {
	Valid   []vector `json:"valid"`
	Invalid []vector `json:"invalid"`
}

type vector struct {
	Key    string `json:"key"`
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

func loadVectors(t *testing.T) vectors {
	t.Helper()
	raw, err := os.ReadFile("testdata/gs1-check-digit.json")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	return v
}

func TestCheckDigitVectors(t *testing.T) {
	for _, v := range loadVectors(t).Valid {
		payload, want := v.Key[:len(v.Key)-1], int(v.Key[len(v.Key)-1]-'0')
		if got := gs1.CheckDigit(payload); got != want {
			t.Errorf("CheckDigit(%s) = %d, want %d", payload, got, want)
		}
	}
}

func TestValidSSCCVectors(t *testing.T) {
	v := loadVectors(t)
	checked := 0
	for _, vec := range v.Valid {
		if vec.Type == "SSCC" {
			checked++
			if !gs1.ValidSSCC(vec.Key) {
				t.Errorf("ValidSSCC(%s) = false", vec.Key)
			}
		}
	}
	for _, vec := range v.Invalid {
		// Telemetry does not know company prefixes, so only format failures apply.
		if vec.Type == "SSCC" && vec.Reason != "PREFIX_MISMATCH" {
			checked++
			if gs1.ValidSSCC(vec.Key) {
				t.Errorf("ValidSSCC(%s) = true, want false (%s)", vec.Key, vec.Reason)
			}
		}
	}
	if checked == 0 {
		t.Fatal("the vectors hold no SSCC")
	}
}

func TestValidSSCCRejectsOtherKeys(t *testing.T) {
	for _, key := range []string{"", "08930001000018", "8930001001015", "0893000100000000180", "08930001000000001٨"} {
		if gs1.ValidSSCC(key) {
			t.Errorf("ValidSSCC(%q) = true", key)
		}
	}
}

func TestCheckSSCCReasons(t *testing.T) {
	for sscc, want := range map[string]string{
		"089300010000000018":  "",
		"08930001000000001":   gs1.ReasonLength,
		"0893000100000000180": gs1.ReasonLength,
		"08930001000000001X":  gs1.ReasonNonNumeric,
		"089300010000000019":  gs1.ReasonCheckDigit,
	} {
		if got := gs1.CheckSSCC(sscc); got != want {
			t.Errorf("CheckSSCC(%s) = %q, want %q", sscc, got, want)
		}
	}
	if gs1.Message(gs1.ReasonCheckDigit) == gs1.Message(gs1.ReasonLength) {
		t.Error("Message() does not tell reasons apart")
	}
}
