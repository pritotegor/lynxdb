//go:build windows

package main

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

// diskFreeBytes returns the number of bytes available to the calling user
// on the filesystem containing path, using the Win32 GetDiskFreeSpaceExW API.
func diskFreeBytes(path string) (uint64, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var freeBytesAvailable uint64
	err = windows.GetDiskFreeSpaceEx(
		pathPtr,
		(*uint64)(unsafe.Pointer(&freeBytesAvailable)),
		nil,
		nil,
	)
	if err != nil {
		return 0, err
	}
	return freeBytesAvailable, nil
}
