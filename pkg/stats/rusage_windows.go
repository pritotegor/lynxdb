//go:build windows

package stats

import (
	"syscall"
	"time"
	"unsafe"
)

// CPUSnapshot holds a point-in-time CPU usage snapshot obtained via
// the Win32 GetProcessTimes API.
type CPUSnapshot struct {
	utime syscall.Filetime
	stime syscall.Filetime
}

var (
	kernel32        = syscall.NewLazyDLL("kernel32.dll")
	getProcessTimes = kernel32.NewProc("GetProcessTimes")
)

// TakeCPUSnapshot captures the current process CPU usage via GetProcessTimes.
func TakeCPUSnapshot() CPUSnapshot {
	handle, _ := syscall.GetCurrentProcess()
	var creation, exit, kernel, user syscall.Filetime
	r1, _, _ := getProcessTimes.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(&creation)),
		uintptr(unsafe.Pointer(&exit)),
		uintptr(unsafe.Pointer(&kernel)),
		uintptr(unsafe.Pointer(&user)),
	)
	if r1 == 0 {
		return CPUSnapshot{}
	}
	return CPUSnapshot{
		utime: user,
		stime: kernel,
	}
}

// ApplyCPUStats computes the CPU time delta between before and after snapshots
// and populates the QueryStats fields.
func ApplyCPUStats(st *QueryStats, before, after CPUSnapshot) {
	st.CPUTimeUser = ftDelta(before.utime, after.utime)
	st.CPUTimeSys = ftDelta(before.stime, after.stime)
}

// ftDelta computes the duration between two FILETIME values.
// FILETIME is expressed in 100-nanosecond intervals.
func ftDelta(before, after syscall.Filetime) time.Duration {
	b := int64(before.HighDateTime)<<32 | int64(before.LowDateTime)
	a := int64(after.HighDateTime)<<32 | int64(after.LowDateTime)
	d := a - b
	if d < 0 {
		return 0
	}
	// Each FILETIME tick is 100 nanoseconds.
	return time.Duration(d * 100)
}
