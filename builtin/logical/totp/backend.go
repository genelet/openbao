// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package totp

import (
	"context"
	"strings"
	"time"

	"github.com/openbao/openbao/helper/namespace"
	cache "github.com/openbao/openbao/physical/pcache"
	"github.com/openbao/openbao/sdk/v2/framework"
	"github.com/openbao/openbao/sdk/v2/logical"
)

const operationPrefixTOTP = "totp"

func Factory(ctx context.Context, conf *logical.BackendConfig) (logical.Backend, error) {
	b, err := Backend()
	if err != nil {
		return nil, err
	}
	if err := b.Setup(ctx, conf); err != nil {
		return nil, err
	}
	return b, nil
}

// oss start
// func Backend() *backend {
func Backend() (*backend, error) {
	// oss end
	var b backend
	b.Backend = &framework.Backend{
		Help: strings.TrimSpace(backendHelp),

		PathsSpecial: &logical.Paths{
			SealWrapStorage: []string{
				"key/",
			},
		},

		Paths: []*framework.Path{
			pathListKeys(&b),
			pathKeys(&b),
			pathCode(&b),
		},

		Secrets:     []*framework.Secret{},
		BackendType: logical.TypeLogical,
	}

	// oss start
	// b.usedCodes = cache.New(0, 30*time.Second)
	var err error
	b.usedCodes, err = cache.New(namespace.RootNamespaceID, operationPrefixTOTP, cache.MarkerDefaultExpiration, 30*time.Second)
	if err != nil {
		return nil, err
	}

	return &b, nil
	// oss end
}

type backend struct {
	*framework.Backend

	usedCodes cache.Cache
}

const backendHelp = `
The TOTP backend dynamically generates time-based one-time use passwords.
`
