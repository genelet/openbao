package initiliaze

import (
	"testing"

	"github.com/openbao/openbao/api/v2"
)

func TestGraphRoot(t *testing.T) {
	client, ctx, err := getClient()
	if err != nil {
		t.Fatal(err)
	}

	sys := client.Sys()

	path := "graph"
	err = sys.MountWithContext(ctx, path, &api.MountInput{
		Type: "graph",
	})
	if err != nil {
		t.Fatal(err)
	}
	mountsRspn, err := sys.ListMountsWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for k, rspn := range mountsRspn {
		if !grep([]string{"graph/", "cubbyhole/", "identity/", "sys/"}, k) {
			t.Errorf("Mount response: %s => %+v", k, rspn)
		}
	}

	err = sys.UnmountWithContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	mountsRspn, err = sys.ListMountsWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for k, rspn := range mountsRspn {
		if !grep([]string{"cubbyhole/", "identity/", "sys/"}, k) {
			t.Errorf("Mount response: %s => %+v", k, rspn)
		}
	}

	err = sys.MountWithContext(ctx, path, &api.MountInput{
		Type: "graph",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = sys.UnmountWithContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGraphNamespace(t *testing.T) {
	client, ctx, err := getClient()
	if err != nil {
		t.Fatal(err)
	}

	sys := client.Sys()
	logical := client.Logical()

	rootNS := "pname"
	_, err = logical.WriteWithContext(ctx, "sys/namespaces/"+rootNS, nil)
	if err != nil {
		t.Fatal(err)
	}
	client.SetNamespace(rootNS)

	path := "graph"
	err = sys.MountWithContext(ctx, path, &api.MountInput{
		Type: "graph",
	})
	if err != nil {
		t.Fatal(err)
	}
	mountsRspn, err := sys.ListMountsWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for k, rspn := range mountsRspn {
		if !grep([]string{"graph/", "cubbyhole/", "identity/", "sys/"}, k) {
			t.Errorf("Mount response: %s => %+v", k, rspn)
		}
	}

	err = sys.UnmountWithContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	mountsRspn, err = sys.ListMountsWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for k, rspn := range mountsRspn {
		if !grep([]string{"cubbyhole/", "identity/", "sys/"}, k) {
			t.Errorf("Mount response: %s => %+v", k, rspn)
		}
	}

	err = sys.MountWithContext(ctx, path, &api.MountInput{
		Type: "graph",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = sys.UnmountWithContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	client.SetNamespace("")
	_, err = logical.DeleteWithContext(ctx, "sys/namespaces/"+rootNS)
	if err != nil {
		t.Fatal(err)
	}
}
