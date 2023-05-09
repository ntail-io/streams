package uuidutil

import "github.com/google/uuid"

func After(before uuid.UUID, after uuid.UUID) bool {
	return Cmp(before, after) == 1
}

func Before(before uuid.UUID, after uuid.UUID) bool {
	return Cmp(before, after) == -1
}

// UUIDCmp compares two UUIDs.
// It returns -1 if a < b, 0 if a == b, and 1 if a > b.
func Cmp(a, b uuid.UUID) int {
	if a.Time() < b.Time() {
		return -1
	}
	if a.Time() > b.Time() {
		return 1
	}
	if a.ClockSequence() < b.ClockSequence() {
		return -1
	}
	if a.ClockSequence() > b.ClockSequence() {
		return 1
	}
	return 0
}
