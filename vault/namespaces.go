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
	ns, err := namespace.FromContext(ctx)
	if err == nil {
		return ns, nil
	}
	if ns != nil && ns.ID == nsID {
		return ns, nil
	}
	// oss end
	if nsID == namespace.RootNamespaceID {
		return namespace.RootNamespace, nil
	}
	return nil, namespace.ErrNoNamespace
}
