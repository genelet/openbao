package physical

import (
	"context"
)

type Mountable interface {
	Flush(ctx context.Context) error
	CreateIfNotExists(ctx context.Context, ns string) error
	DropIfExists(ctx context.Context, ns string) error
	ExistingMount(ctx context.Context, ns string, longest ...bool) (bool, error)
	AddMount(ctx context.Context, ns, typ string) error
	RemoveMount(ctx context.Context, ns string, typ ...string) error
	ListMounts(ctx context.Context, ns ...string) ([]string, error)
}
