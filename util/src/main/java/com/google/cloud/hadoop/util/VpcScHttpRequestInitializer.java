package com.google.cloud.hadoop.util;


import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;

import java.io.IOException;

public class VpcScHttpRequestInitializer implements HttpRequestInitializer {

    private final HttpRequestInitializer initializer;
    private final String host;

    public VpcScHttpRequestInitializer(String host, HttpRequestInitializer initializer) {
        this.host = host;
        this.initializer = initializer;
    }

    @Override
    public void initialize(HttpRequest request) throws IOException {
        initializer.initialize(request);
        request.getHeaders().set("Host", host);
    }
}
