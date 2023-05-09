#!/bin/bash

## Prerequisites
## 1. Install protoc
## 2. Install protoc-gen-go
## 3. Install protoc-gen-go-grpc
## 4. Install protoc-gen-go-vtproto

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $DIR

set -euo pipefail

COLOR_RESET='\033[0m'       # Text Reset
GREEN='\033[1;92m'      # Green

GEN_FILES=$(find . -type f -name "*.go")
PROTO_FILES=$(find * -type f -name "*.proto")

for f in $GEN_FILES; do
  echo "Removing generated go file $f."
  rm $f
done

PREFIX=$(go list -m)/proto

GO_OPTS_M=""
for f in $PROTO_FILES; do
  GO_OPTS_M="$GO_OPTS_M --go-grpc_opt=M$f=$PREFIX/${f%/*}"
  GO_OPTS_M="$GO_OPTS_M --go_opt=M$f=$PREFIX/${f%/*}"
done

#for f in $PROTO_FILES; do
  echo "Generating go files" # for $f..."
#    --doc_out="doc/" \
  protoc \
    --go_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_out=. \
    --go-grpc_opt=paths=source_relative \
    --go-vtproto_out=. \
    --go-vtproto_opt=paths=source_relative,features=marshal+unmarshal+size \
    $GO_OPTS_M $PROTO_FILES
#done

echo "Adding generated files to git..."
git add .
echo -e "${GREEN}Finished generating go protobuf files.$COLOR_RESET"
