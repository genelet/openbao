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
	if nsID != "" {
		return &namespace.Namespace{
			ID:             nsID,
			CustomMetadata: map[string]string{},
		}, nil
	}
	// oss end
	return nil, namespace.ErrNoNamespace
}
