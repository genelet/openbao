// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package vault

import (
	"github.com/openbao/openbao/helper/namespace"
)

func (ts *TokenStore) baseView(_ *namespace.Namespace) *BarrierView {
	return ts.baseBarrierView
}

func (ts *TokenStore) idView(_ *namespace.Namespace) *BarrierView {
	return ts.idBarrierView
}

func (ts *TokenStore) accessorView(_ *namespace.Namespace) *BarrierView {
	return ts.accessorBarrierView
}

func (ts *TokenStore) parentView(_ *namespace.Namespace) *BarrierView {
	return ts.parentBarrierView
}

func (ts *TokenStore) rolesView(_ *namespace.Namespace) *BarrierView {
	return ts.rolesBarrierView
}
