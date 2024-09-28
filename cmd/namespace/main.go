package main

import (
	"context"
	"log"
	"os"

	"github.com/openbao/openbao/api/v2"
)

func main() {
	bs, err := os.ReadFile("/home/peter/Working/root.token")
	if err != nil {
		panic(err)
	}
	token := string(bs)

	config := api.DefaultConfig()
	client, err := api.NewClient(config)
	if err != nil {
		panic(err)
	}
	client.SetToken(token)

	ctx := context.Background()
	logical := client.Logical()

	rootNS := ""
	for _, ns := range []string{"pname", "cname", "dname", "ename"} {
		client.SetNamespace(rootNS)
		_, err = logical.WriteWithContext(ctx, "sys/namespaces/"+ns, nil)
		if err != nil {
			panic(err)
		}
		rootNS += "/" + ns
	}
	for _, ns := range []string{"ename", "dname", "cname", "pname"} {
		rootNS = rootNS[:len(rootNS)-len(ns)-1]
		client.SetNamespace(rootNS)
		_, err = logical.DeleteWithContext(ctx, "sys/namespaces/"+ns)
		if err != nil {
			panic(err)
		}
	}

	client.SetNamespace("")
	rootNS = "pname"
	_, err = logical.WriteWithContext(ctx, "sys/namespaces/"+rootNS, nil)
	if err != nil {
		panic(err)
	}

	client.SetNamespace(rootNS)
	sys := client.Sys()

	// mount graph
	path := "graph"
	err = sys.MountWithContext(ctx, path, &api.MountInput{
		Type: "graph",
	})
	if err != nil {
		panic(err)
	}

	mountsRspn, err := sys.ListMountsWithContext(ctx)
	if err != nil {
		panic(err)
	}
	log.Printf("Mount response: %+v", mountsRspn)

	err = sys.UnmountWithContext(ctx, path)
	if err != nil {
		panic(err)
	}

	mountsRspn, err = sys.ListMountsWithContext(ctx)
	if err != nil {
		panic(err)
	}
	log.Printf("Mount response: %+v", mountsRspn)

	// mount graph
	err = sys.MountWithContext(ctx, path, &api.MountInput{
		Type: "graph",
	})
	if err != nil {
		panic(err)
	}

	// mount kv2
	path = "secret"
	err = sys.MountWithContext(ctx, path, &api.MountInput{
		Type: "kv",
		Options: map[string]string{
			"version": "2",
		},
	},
	)
	if err != nil {
		panic(err)
	}

	mountsRspn, err = sys.ListMountsWithContext(ctx)
	if err != nil {
		panic(err)
	}
	log.Printf("Mount list in pname: %+v", mountsRspn)

	client.SetNamespace("")
	mountsRspn, err = sys.ListMountsWithContext(ctx)
	if err != nil {
		panic(err)
	}
	log.Printf("Mount list in root: %+v", mountsRspn)
}
