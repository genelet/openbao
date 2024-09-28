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

	path := "secret"
	kv2 := client.KVv2(path)

	name := "mysecret"
	var kvSecret *api.KVSecret
	kvSecret, err = kv2.Put(ctx, name, map[string]interface{}{
		"username": "myadmin",
		"password": "123456",
	})
	if err != nil {
		panic(err)
	}
	log.Printf("KV secret: %+v", kvSecret)

	kvSecret, err = kv2.Get(ctx, name)
	if err != nil {
		panic(err)
	}
	log.Printf("KV secret: %+v", kvSecret)
	os.Exit(0)
	err = kv2.Delete(ctx, name)
	if err != nil {
		panic(err)
	}

	_, err = kv2.Put(ctx, name, map[string]interface{}{
		"username": "myadmin7",
		"password": "1234567",
	})
	if err != nil {
		panic(err)
	}

	kvSecret, err = kv2.Get(ctx, name)
	if err != nil {
		log.Printf("KV secret: %+v", err)
	}
	log.Printf("KV secret: %+v", kvSecret)
}
