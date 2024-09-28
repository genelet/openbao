package main

import (
	"context"
	"log"
	"os"

	"github.com/openbao/openbao/api/v2"
)

func main() {
	config := api.DefaultConfig()
	client, err := api.NewClient(config)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	sys := client.Sys()
	initResp, err := sys.InitWithContext(ctx, &api.InitRequest{
		SecretShares:    1,
		SecretThreshold: 1,
	})
	if err != nil {
		panic(err)
	}
	log.Printf("Init response: %+v", initResp)

	unsealKey := initResp.KeysB64[0]
	keyf, err := os.Create("/home/peter/Working/unseal.key")
	if err != nil {
		panic(err)
	}
	defer keyf.Close()
	_, err = keyf.WriteString(unsealKey)
	if err != nil {
		panic(err)
	}

	token := initResp.RootToken
	tokenf, err := os.Create("/home/peter/Working/root.token")
	if err != nil {
		panic(err)
	}
	defer tokenf.Close()
	_, err = tokenf.WriteString(token)
	if err != nil {
		panic(err)
	}

	unsealResp, err := sys.UnsealWithOptionsWithContext(ctx, &api.UnsealOpts{
		Key: unsealKey,
	})
	if err != nil {
		panic(err)
	}
	log.Printf("Unseal response: %+v", unsealResp)

	client.SetToken(initResp.RootToken)
	err = sys.RegisterPluginWithContext(ctx, &api.RegisterPluginInput{
		Name:    "graph",
		Type:    api.PluginTypeSecrets,
		SHA256:  "0d5c56288dd4746f4f6c003e286b3d08a1d91c9e053009e116fc007f5cae2251",
		Command: "graph",
	})
	if err != nil {
		panic(err)
	}

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

	err = sys.MountWithContext(ctx, path, &api.MountInput{
		Type: "graph",
	})
	if err != nil {
		panic(err)
	}

	// mount kv
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

	// list mounts in root
	mountsRspn, err = sys.ListMountsWithContext(ctx)
	if err != nil {
		panic(err)
	}
	log.Printf("Mount response: %+v", mountsRspn)
}
