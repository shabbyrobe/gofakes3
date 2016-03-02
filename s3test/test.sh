#!/bin/bash

set -o errexit
set -o nounset

S3PROXY_BIN="go run ${PWD}/cmd/main.go"
S3PROXY_PORT="9000"
S3TEST_D="${PWD}/s3test/s3-tests"
export S3TEST_CONF="${PWD}/s3test/s3-tests.conf"

# configure s3-tests
pushd $S3TEST_D
./bootstrap
popd

# launch S3Proxy using HTTP and a fixed port
sed "s,^\(s3proxy.endpoint\)=.*,\1=http://127.0.0.1:${S3PROXY_PORT}," \
        < s3test/s3proxy.conf | grep -v secure-endpoint > target/s3proxy.conf
$S3PROXY_BIN --properties target/s3proxy.conf &
S3PROXY_PID=$!

# wait for S3Proxy to start
for i in $(seq 30);
do
    if exec 3<>"/dev/tcp/localhost/${S3PROXY_PORT}";
    then 
        exec 3<&-  # Close for read
        exec 3>&-  # Close for write
        break
    fi
    sleep 1
done

# execute s3-tests
pushd $S3TEST_D
./virtualenv/bin/nosetests -a '!fails_on_s3proxy'
EXIT_CODE=$?
popd

# clean up and return s3-tests exit code
kill $S3PROXY_PID
exit $EXIT_CODE
