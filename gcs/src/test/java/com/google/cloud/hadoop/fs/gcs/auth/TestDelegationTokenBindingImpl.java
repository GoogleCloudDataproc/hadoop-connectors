/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.hadoop.fs.gcs.auth;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * A test delegation token binding implementation
 */
public class TestDelegationTokenBindingImpl extends AbstractDelegationTokenBinding {

  public TestDelegationTokenBindingImpl() {
    super(TestTokenIdentifierImpl.KIND);
  }

  @Override
  public AccessTokenProvider deployUnbonded() throws IOException {
    return new TestAccessTokenProviderImpl();
  }

  @Override
  public AccessTokenProvider bindToTokenIdentifier(AbstractGCPTokenIdentifier retrievedIdentifier) throws IOException {
    return deployUnbonded();
  }

  @Override
  public AbstractGCPTokenIdentifier createTokenIdentifier(Text renewer) throws IOException {
    return new TestTokenIdentifierImpl(getFileSystem().getUri(), new Text("owner_name"), "Test");
  }

  @Override
  public AbstractGCPTokenIdentifier createTokenIdentifier() throws IOException {
    return createEmptyIdentifier();
  }

  @Override
  public AbstractGCPTokenIdentifier createEmptyIdentifier() {
    return new TestTokenIdentifierImpl();
  }

  public static class TestAccessTokenProviderImpl implements AccessTokenProvider {

    public static final String TOKEN_CONFIG_PROPERTY_NAME = "test.token.value";

    private Configuration config = null;

    @Override
    public AccessToken getAccessToken() {
      return new AccessToken(config.get(TOKEN_CONFIG_PROPERTY_NAME),
          System.currentTimeMillis() + 60000);
    }

    @Override
    public void refresh() throws IOException {
      //
    }

    @Override
    public void setConf(Configuration configuration) {
      this.config = configuration;
    }

    @Override
    public Configuration getConf() {
      return config;
    }
  }
}