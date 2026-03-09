package rpc

import "fmt"

// ProtocolVersion is the current cluster protocol version.
// Incremented when backward-incompatible changes are made to the gRPC API.
const ProtocolVersion uint32 = 1

// CheckVersion validates that a remote node's protocol version is compatible.
// Currently requires exact match; future versions may support ranges.
func CheckVersion(remote uint32) error {
	if remote != ProtocolVersion {
		return fmt.Errorf("rpc.CheckVersion: incompatible protocol version %d (local: %d)", remote, ProtocolVersion)
	}

	return nil
}
