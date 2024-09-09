curl --header "x-vault-token: ${VAULT_TOKEN}" --header "x-vault-namespace:"  --request GET $VAULT_ADDR/v1/sys/mounts | jq

curl --header "x-vault-token: ${VAULT_TOKEN}" --header "x-vault-namespace:"  --request POST --data '{"type": "kv-v2"}'   $VAULT_ADDR/v1/sys/mounts/secrets

curl --header "x-vault-token: ${VAULT_TOKEN}" --header "x-vault-namespace:"  --request DELETE $VAULT_ADDR/v1/sys/mounts/secrets

curl --header "x-vault-token: ${VAULT_TOKEN}" --header "x-vault-namespace:"  --request POST   --data @physical/tdengine/payload.json   $VAULT_ADDR/v1/secrets/config

with namespace:
curl --header "x-vault-token: ${VAULT_TOKEN}" --header "x-vault-namespace:pname"  --request GET $VAULT_ADDR/v1/sys/mounts | jq

curl --header "x-vault-token: ${VAULT_TOKEN}" --header "x-vault-namespace:pname"  --request POST --data '{"type": "kv-v2"}'   $VAULT_ADDR/v1/sys/mounts/secrets

curl --header "x-vault-token: ${VAULT_TOKEN}" --header "x-vault-namespace:pname"  --request DELETE $VAULT_ADDR/v1/sys/mounts/secrets

curl --header "x-vault-token: ${VAULT_TOKEN}" --header "x-vault-namespace:pname"  --request POST   --data @physical/tdengine/payload.json   $VAULT_ADDR/v1/secrets/config

