package initiliaze

import (
	"testing"

	"github.com/openbao/openbao/api/auth/approle/v2"
	"github.com/openbao/openbao/api/v2"
)

func TestApproleRoot(t *testing.T) {
	client, ctx, err := getClient()
	if err != nil {
		t.Fatal(err)
	}

	sys := client.Sys()

	path := "approle"
	err = sys.EnableAuthWithOptionsWithContext(ctx, path, &api.EnableAuthOptions{
		Type: "approle",
	})
	if err != nil {
		t.Fatal(err)
	}
	mountsRspn, err := sys.ListAuthWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for k, rspn := range mountsRspn {
		if !grep([]string{"token/", "approle/"}, k) {
			t.Errorf("Mount response: %s => %+v", k, rspn)
		}
	}

	err = sys.DisableAuthWithContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	mountsRspn, err = sys.ListAuthWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for k, rspn := range mountsRspn {
		if !grep([]string{"token/"}, k) {
			t.Errorf("Mount response: %s => %+v", k, rspn)
		}
	}

	err = sys.EnableAuthWithOptionsWithContext(ctx, path, &api.EnableAuthOptions{
		Type: "approle",
	})
	if err != nil {
		t.Fatal(err)
	}

	logical := client.Logical()
	_, err = logical.WriteWithContext(ctx, "auth/"+path+"/role/myrole", map[string]interface{}{
		"policies": []string{"default"},
	})
	if err != nil {
		t.Fatal(err)
	}
	secret, err := logical.WriteWithContext(ctx, "auth/"+path+"/role/myrole/secret-id", nil)
	if err != nil {
		t.Fatal(err)
	}
	secretID := secret.Data["secret_id"].(string)
	secret, err = logical.ReadWithContext(ctx, "auth/"+path+"/role/myrole/role-id")
	if err != nil {
		t.Fatal(err)
	}
	roleID := secret.Data["role_id"].(string)

	auth, err := approle.NewAppRoleAuth(roleID, &approle.SecretID{FromString: secretID})
	if err != nil {
		t.Fatal(err)
	}
	secret, err = auth.Login(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if secret.Auth == nil {
		t.Fatal("No auth data")
	}
	if secret.Auth.ClientToken == "" {
		t.Errorf("Auth data: %+v", secret.Auth)
	}

	_, err = logical.WriteWithContext(ctx, "auth/"+path+"/role/myrole/secret-id/destroy", map[string]interface{}{
		"secret_id": secretID,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = logical.DeleteWithContext(ctx, "auth/"+path+"/role/myrole")
	if err != nil {
		t.Fatal(err)
	}
	secret, err = logical.ListWithContext(ctx, "auth/"+path+"/role")
	if err != nil {
		t.Fatal(err)
	}
	if secret != nil {
		t.Errorf("List response: %+v", secret)
	}

	err = sys.DisableAuthWithContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
}

func TestApproleNamespace(t *testing.T) {
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

	path := "approle"
	err = sys.EnableAuthWithOptionsWithContext(ctx, path, &api.EnableAuthOptions{
		Type: "approle",
	})
	if err != nil {
		t.Fatal(err)
	}
	mountsRspn, err := sys.ListAuthWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for k, rspn := range mountsRspn {
		if !grep([]string{"token/", "approle/"}, k) {
			t.Errorf("Mount response: %s => %+v", k, rspn)
		}
	}

	err = sys.DisableAuthWithContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	mountsRspn, err = sys.ListAuthWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for k, rspn := range mountsRspn {
		if !grep([]string{"token/"}, k) {
			t.Errorf("Mount response: %s => %+v", k, rspn)
		}
	}

	err = sys.EnableAuthWithOptionsWithContext(ctx, path, &api.EnableAuthOptions{
		Type: "approle",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = logical.WriteWithContext(ctx, "auth/"+path+"/role/myrole", map[string]interface{}{
		"policies": []string{"default"},
	})
	if err != nil {
		t.Fatal(err)
	}
	secret, err := logical.WriteWithContext(ctx, "auth/"+path+"/role/myrole/secret-id", nil)
	if err != nil {
		t.Fatal(err)
	}
	secretID := secret.Data["secret_id"].(string)
	secret, err = logical.ReadWithContext(ctx, "auth/"+path+"/role/myrole/role-id")
	if err != nil {
		t.Fatal(err)
	}
	roleID := secret.Data["role_id"].(string)

	auth, err := approle.NewAppRoleAuth(roleID, &approle.SecretID{FromString: secretID})
	if err != nil {
		t.Fatal(err)
	}
	secret, err = auth.Login(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if secret.Auth == nil {
		t.Fatal("No auth data")
	}
	if secret.Auth.ClientToken == "" {
		t.Errorf("Auth data: %+v", secret.Auth)
	}

	_, err = logical.WriteWithContext(ctx, "auth/"+path+"/role/myrole/secret-id/destroy", map[string]interface{}{
		"secret_id": secretID,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = logical.DeleteWithContext(ctx, "auth/"+path+"/role/myrole")
	if err != nil {
		t.Fatal(err)
	}
	secret, err = logical.ListWithContext(ctx, "auth/"+path+"/role")
	if err != nil {
		t.Fatal(err)
	}
	if secret != nil {
		t.Errorf("List response: %+v", secret)
	}

	err = sys.DisableAuthWithContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	client.SetNamespace("")
	_, err = logical.DeleteWithContext(ctx, "sys/namespaces/"+rootNS)
	if err != nil {
		t.Fatal(err)
	}
}

func TestApproleMix(t *testing.T) {
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
	clone, err := client.Clone()
	if err != nil {
		t.Fatal(err)
	}
	clone.SetNamespace(rootNS)
	clone.SetToken(client.Token())
	sysNS := clone.Sys()
	logicalNS := clone.Logical()

	path := "approle"

	var secret *api.Secret
	var roleID, secretID, roleNS, secretNS string

	if err = sys.EnableAuthWithOptionsWithContext(ctx, path, &api.EnableAuthOptions{
		Type: "approle",
	}); err == nil {
		if _, err = logical.WriteWithContext(ctx, "auth/"+path+"/role/myrole", map[string]interface{}{
			"policies": []string{"default"},
		}); err == nil {
			if secret, err = logical.WriteWithContext(ctx, "auth/"+path+"/role/myrole/secret-id", nil); err == nil {
				secretID = secret.Data["secret_id"].(string)
				if secret, err = logical.ReadWithContext(ctx, "auth/"+path+"/role/myrole/role-id"); err == nil {
					roleID = secret.Data["role_id"].(string)
				}
			}
		}
	}
	if err != nil {
		t.Fatal(err)
	}

	if err = sysNS.EnableAuthWithOptionsWithContext(ctx, path, &api.EnableAuthOptions{
		Type: "approle",
	}); err == nil {
		if _, err = logicalNS.WriteWithContext(ctx, "auth/"+path+"/role/yourrole", map[string]interface{}{
			"policies": []string{"default"},
		}); err == nil {
			if secret, err = logicalNS.WriteWithContext(ctx, "auth/"+path+"/role/yourrole/secret-id", nil); err == nil {
				secretNS = secret.Data["secret_id"].(string)
				if secret, err = logicalNS.ReadWithContext(ctx, "auth/"+path+"/role/yourrole/role-id"); err == nil {
					roleNS = secret.Data["role_id"].(string)
				}
			}
		}
	}
	if err != nil {
		t.Fatal(err)
	}

	auth, err := approle.NewAppRoleAuth(roleID, &approle.SecretID{FromString: secretID})
	if err != nil {
		t.Fatal(err)
	}
	secret, err = auth.Login(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if secret.Auth == nil || secret.Auth.ClientToken == "" {
		t.Errorf("Auth data: %+v", secret.Auth)
	}
	secret, err = auth.Login(ctx, clone)
	if err == nil {
		t.Errorf("error should exist, but we got nil. secret id: %#v", secret)
	}

	authNS, err := approle.NewAppRoleAuth(roleNS, &approle.SecretID{FromString: secretNS})
	if err != nil {
		t.Fatal(err)
	}
	secret, err = authNS.Login(ctx, clone)
	if err != nil {
		t.Fatal(err)
	}
	if secret.Auth == nil || secret.Auth.ClientToken == "" {
		t.Errorf("Auth data: %+v", secret.Auth)
	}
	secret, err = authNS.Login(ctx, client)
	if err == nil {
		t.Errorf("error should exist, but we got nil. secret id: %#v", secret)
	}

	if _, err = logicalNS.WriteWithContext(ctx, "auth/"+path+"/role/yourrole/secret-id/destroy", map[string]interface{}{
		"secret_id": secretNS,
	}); err == nil {
		if _, err = logicalNS.DeleteWithContext(ctx, "auth/"+path+"/role/yourrole"); err == nil {
			if secret, err = logicalNS.ListWithContext(ctx, "auth/"+path+"/role"); err == nil {
				if secret != nil {
					t.Errorf("List response: %+v", secret)
				}
				err = sysNS.DisableAuthWithContext(ctx, path)
			}
		}
	}
	if err != nil {
		t.Fatal(err)
	}

	if _, err = logical.WriteWithContext(ctx, "auth/"+path+"/role/myrole/secret-id/destroy", map[string]interface{}{
		"secret_id": secretID,
	}); err == nil {
		if _, err = logical.DeleteWithContext(ctx, "auth/"+path+"/role/myrole"); err == nil {
			if secret, err = logical.ListWithContext(ctx, "auth/"+path+"/role"); err == nil {
				if secret != nil {
					t.Errorf("List response: %+v", secret)
				}
				err = sys.DisableAuthWithContext(ctx, path)
			}
		}
	}
	if err != nil {
		t.Fatal(err)
	}

	client.SetNamespace("")
	_, err = logical.DeleteWithContext(ctx, "sys/namespaces/"+rootNS)
	if err != nil {
		t.Fatal(err)
	}
}
