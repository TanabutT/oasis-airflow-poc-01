#!/usr/bin/env bash
# Prints fernet key + JWT secret in env-file format.
set -euo pipefail
FERNET=$(python3 -c 'import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())')
JWT=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
echo "AIRFLOW_FERNET_KEY=${FERNET}"
echo "AIRFLOW_JWT_SECRET=${JWT}"
