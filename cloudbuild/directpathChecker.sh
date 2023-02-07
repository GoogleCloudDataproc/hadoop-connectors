curl -L \
https://github.com/GoogleCloudPlatform/grpc-gcp-tools/releases/latest/download/dp_check -o dp_check

chmod a+x dp_check

./dp_check --service storage.googleapis.com \
    -check_grpclb=false -check_xds=true 2> /dev/null