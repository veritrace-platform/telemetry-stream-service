// Package gs1 validates the SSCCs that readings name, following veritrace/docs/domain/gs1-identifiers.md. Core
// issues SSCCs and validates every GS1 key; telemetry only checks the SSCC format.
package gs1

// ssccLength is the number of digits of an SSCC, check digit included.
const ssccLength = 18

// CheckDigit returns the GS1 Modulo 10 check digit of payload, a key without its check digit. Weights 3 and 1
// alternate from the rightmost digit. payload must contain only ASCII digits.
func CheckDigit(payload string) int {
	sum := 0
	weight := 3
	for i := len(payload) - 1; i >= 0; i-- {
		sum += int(payload[i]-'0') * weight
		weight = 4 - weight
	}
	return (10 - sum%10) % 10
}

// ValidSSCC reports whether sscc has 18 digits and a correct check digit.
func ValidSSCC(sscc string) bool {
	if len(sscc) != ssccLength {
		return false
	}
	for i := range len(sscc) {
		if sscc[i] < '0' || sscc[i] > '9' {
			return false
		}
	}
	return CheckDigit(sscc[:ssccLength-1]) == int(sscc[ssccLength-1]-'0')
}
