// Package store defines the database repository interface and error types.
package store

import (
	"context"

	v1alpha1 "github.com/dcm-project/environment-agent/api/container/v1alpha1"
)

// DatabaseRepository defines the storage interface for container CRUD operations
// and backing infrastructure health checks.
type DatabaseRepository interface {
	Create(ctx context.Context, spec v1alpha1.ContainerSpec, id string) (*v1alpha1.Container, error)
	Get(ctx context.Context, containerID string) (*v1alpha1.Container, error)
	List(ctx context.Context, maxPageSize int32, pageToken string) (*v1alpha1.ContainerList, error)
	Delete(ctx context.Context, containerID string) error
	CheckHealth(ctx context.Context) error
}
