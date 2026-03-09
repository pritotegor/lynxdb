package rpc

// Tracing interceptors for gRPC are configured via OTel's otelgrpc stats handler.
// Server-side: passed as grpc.StatsHandler(otelgrpc.NewServerHandler()) in NewServer.
// Client-side: passed as grpc.WithStatsHandler(otelgrpc.NewClientHandler()) in NewClientPool.
//
// This file exists as documentation and a hook point for future custom interceptors
// (e.g., request-id propagation, rate limiting, auth token injection).
