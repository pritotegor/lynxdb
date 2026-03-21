//go:build !linux && !darwin

package compaction

// AdviseSequential is a no-op on platforms where fadvise/fcntl hints are not available.
func AdviseSequential(_ uintptr) {}

// AdviseDontNeed is a no-op on platforms where fadvise/fcntl hints are not available.
func AdviseDontNeed(_ uintptr) {}
