#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Start a Fineract image against a throwaway PostgreSQL and check that it serves requests.
#
# The image runs the way `docker run` would run it: its own user, no mounts, no compose healthcheck
# (the hardened image has no shell for one). Fineract's defaults are used apart from the database
# host. The check passes when the actuator reports UP and an authenticated API call on the default
# tenant answers.
#
# Usage: smoke-test-docker-image.sh <image> [timeout-seconds]

set -euo pipefail

IMAGE=${1:?usage: smoke-test-docker-image.sh <image> [timeout-seconds]}
TIMEOUT=${2:-600}
# Keep in step with config/docker/compose/postgresql.yml.
POSTGRES_IMAGE=postgres:18.3
NAME=fineract-smoke-$$

cleanup() {
    local status=$?
    if [ "$status" -ne 0 ]; then
        echo "::group::Fineract container log"
        docker logs --tail 300 "$NAME-app" 2>&1 || true
        echo "::endgroup::"
    fi
    docker rm -f "$NAME-app" "$NAME-db" >/dev/null 2>&1 || true
    docker network rm "$NAME" >/dev/null 2>&1 || true
    exit "$status"
}
trap cleanup EXIT

docker network create "$NAME" >/dev/null

# root/postgres are Fineract's default database credentials.
docker run -d --name "$NAME-db" --network "$NAME" --network-alias db \
    -e POSTGRES_USER=root -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=fineract_tenants \
    "$POSTGRES_IMAGE" >/dev/null

# Over TCP, so the temporary server the entrypoint runs during initialisation does not count.
deadline=$((SECONDS + 120))
until docker exec "$NAME-db" pg_isready -q -h 127.0.0.1 -U root -d fineract_tenants; do
    [ "$SECONDS" -lt "$deadline" ] || { echo "PostgreSQL did not become ready" >&2; exit 1; }
    sleep 2
done
docker exec "$NAME-db" psql -q -U root -d fineract_tenants -c 'CREATE DATABASE fineract_default'

docker run -d --name "$NAME-app" --network "$NAME" -p 127.0.0.1::8443 \
    -e FINERACT_HIKARI_JDBC_URL=jdbc:postgresql://db:5432/fineract_tenants \
    -e FINERACT_DEFAULT_TENANTDB_HOSTNAME=db \
    "$IMAGE" >/dev/null

echo "Image user: $(docker inspect --format '{{.Config.User}}' "$IMAGE")"
PORT=$(docker port "$NAME-app" 8443/tcp | head -n 1 | sed 's/.*://')
BASE_URL="https://127.0.0.1:$PORT/fineract-provider"

deadline=$((SECONDS + TIMEOUT))
until curl -ksf "$BASE_URL/actuator/health" | grep -q '"status":"UP"'; do
    if [ "$(docker inspect --format '{{.State.Running}}' "$NAME-app")" != "true" ]; then
        echo "Fineract container stopped (exit code $(docker inspect --format '{{.State.ExitCode}}' "$NAME-app"))" >&2
        exit 1
    fi
    [ "$SECONDS" -lt "$deadline" ] || { echo "Fineract did not report UP within ${TIMEOUT}s" >&2; exit 1; }
    sleep 5
done
echo "Actuator health is UP"

# mifos/password is the seeded demo user of the default tenant.
offices=$(curl -ksf -u mifos:password -H 'Fineract-Platform-TenantId: default' "$BASE_URL/api/v1/offices")
echo "$offices" | grep -q '"name":"Head Office"' || { echo "Unexpected /offices response: $offices" >&2; exit 1; }
echo "Authenticated API call answered: GET /api/v1/offices returned the Head Office"

echo "Smoke test passed for $IMAGE"
