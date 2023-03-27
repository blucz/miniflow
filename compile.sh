#!/usr/bin/env bash

exec deno compile --allow-env --check --allow-run --allow-read --allow-write miniflow.ts $*
