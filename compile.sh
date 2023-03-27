#!/usr/bin/env bash

exec deno compile --check --allow-run --allow-read --allow-write miniflow.ts $*
