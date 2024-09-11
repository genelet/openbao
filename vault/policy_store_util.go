// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package vault

import (
	"context"
	//"github.com/openbao/openbao/helper/namespace"
)

func (ps *PolicyStore) getACLView() *BarrierView {
	return ps.aclView
}

func (ps *PolicyStore) getBarrierView() *BarrierView {
	return ps.getACLView()
}

func (ps *PolicyStore) loadACLPolicyNamespaces(ctx context.Context, policyName, policyText string) error {
	return ps.loadACLPolicyInternal(ctx, policyName, policyText)
}
