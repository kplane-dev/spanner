# spanner

Spanner-backed `storage.Interface` implementation for Kubernetes API servers.

This package provides a drop-in replacement for the etcd3 storage backend, using Google Cloud Spanner as the persistence layer. It implements the `k8s.io/apiserver/pkg/storage.Interface` contract including `Get`, `List`, `Watch`, `Create`, `Delete`, and `GuaranteedUpdate`.

## Features

- Full `storage.Interface` implementation backed by Cloud Spanner
- Watch support via a polling-based change broadcast mechanism
- Compatible with the Cloud Spanner emulator for local development and testing
- Shared Spanner client across resource stores to avoid session pool explosion

## Usage

```go
cfg := spannerstore.SpannerConfig{
    Project:  "my-project",
    Instance: "my-instance",
    Database: "my-database",
}
client, err := cfg.NewClient(ctx)
store := spannerstore.NewStore(client, codec, newFunc, newListFunc, prefix, resourcePrefix, transformer, nil)
```

For local development, set `EmulatorHost` to use the Cloud Spanner emulator:

```go
cfg.EmulatorHost = "localhost:9010"
```

## Dependencies

This module depends on the [kplane-dev/kubernetes](https://github.com/kplane-dev/kubernetes) fork for `k8s.io/apimachinery` and `k8s.io/apiserver` types.
