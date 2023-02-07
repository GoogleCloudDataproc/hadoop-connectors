curl -L \
https://github.com/GoogleCloudPlatform/grpc-gcp-tools/releases/latest/download/dp_check -o dp_check

chmod a+x dp_check

./dp_check --service storage.googleapis.com \
    --xds_expect_fallback_configured=false -check_grpclb=false -check_xds=true -ipv4_only