// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package vault

import (
	"context"
	"testing"

	"github.com/openbao/openbao/helper/namespace"
	//"github.com/openbao/openbao/sdk/v2/logical"
	//"github.com/stretchr/testify/require"
)

func TestNamespaceStore_Root(t *testing.T) {
	t.Run("root", func(t *testing.T) {
		t.Parallel()

		core, _, _ := TestCoreUnsealed(t)
		ps := core.namespaceStore
		testNamespaceRoot(t, ps, namespace.RootNamespace, true)
	})
}

func testNamespaceRoot(t *testing.T, ps *NamespaceStore, ns *namespace.Namespace, expectFound bool) {
	// Get should return a special policy
	ctx := namespace.ContextWithNamespace(context.Background(), ns)
	p, err := ps.GetNamespace(ctx, "root")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Handle whether a root token is expected
	if expectFound {
		if p == nil {
			t.Fatalf("bad: %v", p)
		}
		if p.ID != "root" {
			t.Fatalf("bad: %v", p)
		}
	} else {
		if p != nil {
			t.Fatal("expected nil root namespace")
		}
		// Create root policy for subsequent modification and deletion failure
		// tests
		p = &namespace.Namespace{
			ID:             "root",
			CustomMetadata: map[string]string{},
		}
	}

	// Set should fail
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.SetNamespace(ctx, p.ID, p.CustomMetadata)
	if err.Error() != `cannot update "root" namespace` {
		t.Fatalf("err: %v", err)
	}

	// Delete should fail
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.DeleteNamespace(ctx, "root")
	if err.Error() != `cannot delete "root" namespace` {
		t.Fatalf("err: %v", err)
	}
}

func mockNamespaceWithCore(t *testing.T, disableCache bool) (*Core, *NamespaceStore) {
	conf := &CoreConfig{
		DisableCache: disableCache,
	}
	core, _, _ := TestCoreUnsealedWithConfig(t, conf)
	ps := core.namespaceStore

	return core, ps
}

func TestNamespaceStore_CRUD(t *testing.T) {
	t.Run("root-ns", func(t *testing.T) {
		t.Run("cached", func(t *testing.T) {
			_, ps := mockNamespaceWithCore(t, false)
			testNamespaceStoreCRUD(t, ps, namespace.RootNamespace)
		})

		t.Run("no-cache", func(t *testing.T) {
			_, ps := mockNamespaceWithCore(t, true)
			testNamespaceStoreCRUD(t, ps, namespace.RootNamespace)
		})
	})
}

func testNamespaceStoreCRUD(t *testing.T, ps *NamespaceStore, ns *namespace.Namespace) {
	// Get should return nothing
	ctx := namespace.ContextWithNamespace(context.Background(), ns)
	p, err := ps.GetNamespace(ctx, "Dev")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if p != nil {
		t.Fatalf("bad: %v", p)
	}

	// Delete should be no-op
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.DeleteNamespace(ctx, "deV")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// List should be blank
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	out, err := ps.ListNamespaces(ctx)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("bad: %v", out)
	}

	// Set should work
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.SetNamespace(ctx, aclNamespace, map[string]string{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Get should work
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	p, err = ps.GetNamespace(ctx, "dEv")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if p.ID != "dev" {
		t.Fatalf("bad: %v", p)
	}

	// List should contain two elements
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	out, err = ps.ListNamespaces(ctx)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("bad: %v", out)
	}

	// Delete should be clear the entry
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.DeleteNamespace(ctx, "Dev")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// List should contain one element
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	out, err = ps.ListNamespaces(ctx)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(out) != 1 || out[0] != "default" {
		t.Fatalf("bad: %v", out)
	}

	// Get should fail
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	p, err = ps.GetNamespace(ctx, "deV")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if p != nil {
		t.Fatalf("bad: %v", p)
	}
}

/*
func TestNamespaceStore_Predefined(t *testing.T) {
	t.Run("root-ns", func(t *testing.T) {
		_, ps := mockNamespaceWithCore(t, false)
		testNamespaceStorePredefined(t, ps, namespace.RootNamespace)
	})
}

// Test predefined policy handling
func testNamespaceStorePredefined(t *testing.T, ps *NamespaceStore, ns *namespace.Namespace) {
	// List should be two elements
	ctx := namespace.ContextWithNamespace(context.Background(), ns)
	out, err := ps.ListNamespaces(ctx, NamespaceTypeACL)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// This shouldn't contain response-wrapping since it's non-assignable
	if len(out) != 1 || out[0] != "default" {
		t.Fatalf("bad: %v", out)
	}

	// Response-wrapping policy checks
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	pCubby, err := ps.GetNamespace(ctx, "response-wrapping", NamespaceTypeToken)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if pCubby == nil {
		t.Fatal("nil cubby policy")
	}
	if pCubby.Raw != responseWrappingNamespace {
		t.Fatalf("bad: expected\n%s\ngot\n%s\n", responseWrappingNamespace, pCubby.Raw)
	}
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.SetNamespace(ctx, pCubby)
	if err == nil {
		t.Fatalf("expected err setting %s", pCubby.Name)
	}
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.DeleteNamespace(ctx, pCubby.Name, NamespaceTypeACL)
	if err == nil {
		t.Fatalf("expected err deleting %s", pCubby.Name)
	}

	// Root policy checks, behavior depending on namespace
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	pRoot, err := ps.GetNamespace(ctx, "root", NamespaceTypeToken)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ns == namespace.RootNamespace {
		if pRoot == nil {
			t.Fatal("nil root policy")
		}
	} else {
		if pRoot != nil {
			t.Fatal("expected nil root policy")
		}
		pRoot = &Namespace{
			Name: "root",
		}
	}
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.SetNamespace(ctx, pRoot)
	if err == nil {
		t.Fatalf("expected err setting %s", pRoot.Name)
	}
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.DeleteNamespace(ctx, pRoot.Name, NamespaceTypeACL)
	if err == nil {
		t.Fatalf("expected err deleting %s", pRoot.Name)
	}
}

func TestNamespaceStore_ACL(t *testing.T) {
	t.Run("root-ns", func(t *testing.T) {
		_, ps := mockNamespaceWithCore(t, false)
		testNamespaceStoreACL(t, ps, namespace.RootNamespace)
	})
}

func testNamespaceStoreACL(t *testing.T, ps *NamespaceStore, ns *namespace.Namespace) {
	ctx := namespace.ContextWithNamespace(context.Background(), ns)
	policy, _ := ParseACLNamespace(ns, aclNamespace)
	err := ps.SetNamespace(ctx, policy)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	policy, _ = ParseACLNamespace(ns, aclNamespace2)
	err = ps.SetNamespace(ctx, policy)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	acl, err := ps.ACL(ctx, nil, map[string][]string{ns.ID: {"dev", "ops"}})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	testLayeredACL(t, acl, ns)
}

func TestDefaultNamespace(t *testing.T) {
	ctx := namespace.ContextWithNamespace(context.Background(), namespace.RootNamespace)

	policy, err := ParseACLNamespace(namespace.RootNamespace, defaultNamespace)
	if err != nil {
		t.Fatal(err)
	}
	acl, err := NewACL(ctx, []*Namespace{policy})
	if err != nil {
		t.Fatal(err)
	}

	for name, tc := range map[string]struct {
		op            logical.Operation
		path          string
		expectAllowed bool
	}{
		"lookup self":            {logical.ReadOperation, "auth/token/lookup-self", true},
		"renew self":             {logical.UpdateOperation, "auth/token/renew-self", true},
		"revoke self":            {logical.UpdateOperation, "auth/token/revoke-self", true},
		"check own capabilities": {logical.UpdateOperation, "sys/capabilities-self", true},

		"read arbitrary path":     {logical.ReadOperation, "foo/bar", false},
		"login at arbitrary path": {logical.UpdateOperation, "auth/foo", false},
	} {
		t.Run(name, func(t *testing.T) {
			request := new(logical.Request)
			request.Operation = tc.op
			request.Path = tc.path

			result := acl.AllowOperation(ctx, request, false)
			if result.RootPrivs {
				t.Fatal("unexpected root")
			}
			if tc.expectAllowed != result.Allowed {
				t.Fatalf("Expected %v, got %v", tc.expectAllowed, result.Allowed)
			}
		})
	}
}

// TestNamespaceStore_GetNonEGPNamespaceType has five test cases:
//   - happy-acl: we store a policy in the policy type map and
//     then look up its type successfully.
//   - not-in-map-acl: ensure that GetNonEGPNamespaceType fails
//     returning a nil and an error when the policy doesn't exist in the map.
//   - unknown-policy-type: ensures that GetNonEGPNamespaceType fails returning a nil
//     and an error when the policy type in the type map is a value that
//     does not map to a NamespaceType.
func TestNamespaceStore_GetNonEGPNamespaceType(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		namespaceStoreKey    string
		namespaceStoreValue  any
		paramNamespace       string
		paramNamespaceName   string
		paramNamespaceType   NamespaceType
		isErrorExpected      bool
		expectedErrorMessage string
	}{
		"happy-acl": {
			namespaceStoreKey:   "1AbcD/policy1",
			namespaceStoreValue: NamespaceTypeACL,
			paramNamespace:      "1AbcD",
			paramNamespaceName:  "policy1",
			paramNamespaceType:  NamespaceTypeACL,
		},
		"not-in-map-acl": {
			namespaceStoreKey:    "2WxyZ/policy2",
			namespaceStoreValue:  NamespaceTypeACL,
			paramNamespace:       "1AbcD",
			paramNamespaceName:   "policy1",
			isErrorExpected:      true,
			expectedErrorMessage: "policy does not exist in type map",
		},
		"unknown-policy-type": {
			namespaceStoreKey:    "1AbcD/policy1",
			namespaceStoreValue:  7,
			paramNamespace:       "1AbcD",
			paramNamespaceName:   "policy1",
			isErrorExpected:      true,
			expectedErrorMessage: "unknown policy type for: 1AbcD/policy1",
		},
	}

	for name, tc := range tests {
		name := name
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, ps := mockNamespaceWithCore(t, false)
			ps.policyTypeMap.Store(tc.namespaceStoreKey, tc.namespaceStoreValue)
			got, err := ps.GetNonEGPNamespaceType(tc.paramNamespace, tc.paramNamespaceName)
			if tc.isErrorExpected {
				require.Error(t, err)
				require.Nil(t, got)
				require.EqualError(t, err, tc.expectedErrorMessage)

			}
			if !tc.isErrorExpected {
				require.NoError(t, err)
				require.NotNil(t, got)
				require.Equal(t, tc.paramNamespaceType, *got)
			}
		})
	}
}
*/
