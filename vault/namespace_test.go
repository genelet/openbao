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

	if err = ps.invalidate(ctx, ""); err != nil {
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

	err = ps.SetNamespace(ctx, "Dev", map[string]string{})
	if err != nil {
		t.Fatalf("err: %v", err)
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
	if len(out) != 1 || out[0] != "root" {
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

	if err = ps.invalidate(ctx, ""); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestNamespaceStore_Tree(t *testing.T) {
	_, ps := mockNamespaceWithCore(t, true)

	ns := &namespace.Namespace{
		ID:             "root",
		CustomMetadata: map[string]string{},
	}

	// Get should return nothing
	ctx := namespace.ContextWithNamespace(context.Background(), ns)
	err := ps.SetNamespace(ctx, "pname", map[string]string{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	ns.ID = "root/pname"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.SetNamespace(ctx, "cname", map[string]string{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	ns.ID = "root/pname/cname"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.SetNamespace(ctx, "dname", map[string]string{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	ns.ID = "root/pname/cname/dname"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.SetNamespace(ctx, "ename", map[string]string{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = ps.DeleteNamespace(ctx, "ename")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	ns.ID = "root/pname/cname"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.DeleteNamespace(ctx, "dname")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	ns.ID = "root/pname"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.DeleteNamespace(ctx, "cname")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	ns.ID = "root"
	ctx = namespace.ContextWithNamespace(context.Background(), ns)
	err = ps.DeleteNamespace(ctx, "pname")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if err = ps.invalidate(ctx, ""); err != nil {
		t.Fatalf("err: %v", err)
	}
}
