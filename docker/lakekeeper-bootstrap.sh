#!/bin/sh
# Idempotent Lakekeeper setup: bootstrap the server once, then create the warehouse if missing.
set -eu
API=http://lakekeeper:8181/management/v1

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/bootstrap" \
  -H 'Content-Type: application/json' --data '{"accept-terms-of-use": true}')
case "$code" in
  2*) echo "bootstrapped" ;;
  400|409) echo "already bootstrapped" ;;
  *) echo "bootstrap failed: HTTP $code" >&2; exit 1 ;;
esac

if curl -sf "$API/warehouse" | grep -q "\"name\":\"$WAREHOUSE\""; then
  echo "warehouse $WAREHOUSE exists"
  exit 0
fi

curl -sf -X POST "$API/warehouse" -H 'Content-Type: application/json' --data @- <<JSON
{
  "warehouse-name": "$WAREHOUSE",
  "storage-profile": {
    "type": "s3",
    "bucket": "$S3_BUCKET",
    "key-prefix": "warehouse",
    "endpoint": "http://seaweedfs:8333",
    "region": "us-east-1",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": false,
    "remote-signing-enabled": false
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "access-key-id": "$S3_ACCESS_KEY",
    "secret-access-key": "$S3_SECRET_KEY"
  }
}
JSON
echo "warehouse $WAREHOUSE created"
