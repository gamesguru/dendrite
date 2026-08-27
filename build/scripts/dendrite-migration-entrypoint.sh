#!/bin/sh
set -eu

data_dir=/data
config_file="$data_dir/homeserver.yaml"

mkdir -p "$data_dir"
cd "$data_dir"

if [ ! -f "$data_dir/matrix_key.pem" ]; then
  generate-keys \
    --private-key "$data_dir/matrix_key.pem" \
    --server "${SERVER_NAME:-hs1}" \
    --tls-cert "$data_dir/server.crt" \
    --tls-key "$data_dir/server.key"
fi

set -- \
  --ci \
  --server "${SERVER_NAME:-hs1}" \
  --dir "$data_dir"

if [ -n "${DATABASE_URL:-}" ]; then
  set -- "$@" --db "$DATABASE_URL"
fi

generate-config "$@" >"$config_file"

exec homeserver \
  --really-enable-open-registration \
  --config "$config_file" \
  --tls-cert "$data_dir/server.crt" \
  --tls-key "$data_dir/server.key"
