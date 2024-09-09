// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package vault

import (
	"context"

	"github.com/openbao/openbao/helper/namespace"
)

var NamespaceByID func(context.Context, string, *Core) (*namespace.Namespace, error) = namespaceByID

func namespaceByID(ctx context.Context, nsID string, c *Core) (*namespace.Namespace, error) {
	// oss start
	if true {
		return &namespace.Namespace{ID: namespace.RootNamespaceID}, nil
	}
	// oss end
	ns, err := namespace.FromContext(ctx)
	if err == nil {
		return ns, nil
	}
	if ns != nil && ns.ID == nsID {
		return ns, nil
	}
	if nsID == namespace.RootNamespaceID {
		return namespace.RootNamespace, nil
	}
	return nil, namespace.ErrNoNamespace
}
