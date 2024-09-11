// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package vault

import (
	"context"
	"fmt"
	"sort"

	"github.com/openbao/openbao/helper/namespace"
	"github.com/openbao/openbao/sdk/v2/logical"
)

// Capabilities is used to fetch the capabilities of the given token on the
// given path
func (c *Core) Capabilities(ctx context.Context, token, path string) ([]string, error) {
	c.Logger().Trace("666666666", "token", token, "path", path)
	if path == "" {
		return nil, &logical.StatusBadRequest{Err: "missing path"}
	}

	if token == "" {
		return nil, &logical.StatusBadRequest{Err: "missing token"}
	}

	te, err := c.tokenStore.Lookup(ctx, token)
	if err != nil {
		return nil, err
	}
	if te == nil {
		return nil, &logical.StatusBadRequest{Err: "invalid token"}
	}

	//tokenNS, err := NamespaceByID(ctx, te.NamespaceID, c)
	//if err != nil {
	//	return nil, err
	//}
	//if tokenNS == nil {
	//	return nil, namespace.ErrNoNamespace
	//}
	// need namespace to for helper's identity entity
	ns, err := namespace.FromContext(ctx)
	if err != nil {
		return nil, err
	}

	var policyCount int
	policyNames := make(map[string][]string)
	//policyNames[tokenNS.ID] = te.Policies
	policyNames[ns.ID] = te.Policies
	policyCount += len(te.Policies)

	entity, identityPolicies, err := c.fetchEntityAndDerivedPolicies(ctx, te.EntityID, te.NoIdentityPolicies)
	c.Logger().Trace("55555", "t-entry", fmt.Sprintf("%#v", te), "ident-entity", fmt.Sprintf("%#v", entity), "identityPolicies", fmt.Sprintf("%#v", identityPolicies))
	if err != nil {
		return nil, err
	}
	if entity != nil && entity.Disabled {
		c.logger.Warn("permission denied as the entity on the token is disabled")
		return nil, logical.ErrPermissionDenied
	}
	if te.EntityID != "" && entity == nil {
		c.logger.Warn("permission denied as the entity on the token is invalid")
		return nil, logical.ErrPermissionDenied
	}

	for nsID, nsPolicies := range identityPolicies {
		policyNames[nsID] = append(policyNames[nsID], nsPolicies...)
		policyCount += len(nsPolicies)
	}

	// Add capabilities of the inline policy if it's set
	policies := make([]*Policy, 0)
	if te.InlinePolicy != "" {
		inlinePolicy, err := ParseACLPolicy(te.InlinePolicy)
		if err != nil {
			return nil, err
		}
		policies = append(policies, inlinePolicy)
		policyCount++
	}

	if policyCount == 0 {
		return []string{DenyCapability}, nil
	}

	// Construct the corresponding ACL object. ACL construction should be
	// performed on the token's namespace.
	//tokenCtx := namespace.ContextWithNamespace(ctx, tokenNS)
	tokenCtx := ctx
	c.Logger().Trace("7777777777", "policyNames", policyNames, "policies", policies)
	acl, err := c.policyStore.ACL(tokenCtx, entity, policyNames, policies...)
	if err != nil {
		return nil, err
	}

	capabilities := acl.Capabilities(ctx, path)
	sort.Strings(capabilities)
	return capabilities, nil
}
