package spanner

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
)

// BackendFactory is the type expected by kplane-dev/storage.DecoratorConfig.BackendFactory.
// Duplicated here to avoid importing kplane-dev/storage (which would create a cycle).
type BackendFactory func(
	config *storagebackend.ConfigForResource,
	newFunc, newListFunc func() runtime.Object,
	resourcePrefix string,
) (storage.Interface, factory.DestroyFunc, error)

// NewBackendFactory returns a BackendFactory that creates Spanner-backed
// storage.Interface instances. Each call to the returned factory creates a
// new store sharing the same Spanner client.
func NewBackendFactory(cfg SpannerConfig) BackendFactory {
	return func(
		config *storagebackend.ConfigForResource,
		newFunc, newListFunc func() runtime.Object,
		resourcePrefix string,
	) (storage.Interface, factory.DestroyFunc, error) {
		ctx := context.Background()

		client, err := cfg.NewClient(ctx)
		if err != nil {
			return nil, nil, err
		}

		transformer := config.Transformer
		if transformer == nil {
			transformer = identity.NewEncryptCheckTransformer()
		}

		s := NewStore(
			client,
			config.Codec,
			newFunc,
			newListFunc,
			config.Prefix,
			resourcePrefix,
			transformer,
			config.WrapDecodedObject,
		)

		destroyFunc := func() {
			client.Close()
		}

		return s, destroyFunc, nil
	}
}
