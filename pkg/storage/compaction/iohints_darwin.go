//go:build darwin

package compaction

import "syscall"

// AdviseSequential hints the OS to read-ahead aggressively for compaction.
// On macOS, uses F_RDAHEAD via fcntl to enable read-ahead on the file descriptor.
func AdviseSequential(fd uintptr) {
	// F_RDAHEAD with arg 1 enables read-ahead for the file descriptor.
	_, _, _ = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_RDAHEAD, 1)
}

// AdviseDontNeed hints the OS to drop pages from the page cache after compaction
// read, preventing cold compaction data from polluting the cache used by queries.
// On macOS, uses F_NOCACHE to disable caching for the file descriptor.
func AdviseDontNeed(fd uintptr) {
	// F_NOCACHE with arg 1 disables the unified buffer cache for this fd,
	// causing pages to be evicted once no longer actively referenced.
	_, _, _ = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_NOCACHE, 1)
}
