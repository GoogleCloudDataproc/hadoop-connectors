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

import org.apache.hadoop.io.Text;

import java.net.URI;

/**
 * A test delegation token identifier implementation
 */
public class TestTokenIdentifierImpl extends AbstractGCPTokenIdentifier {

  public static final Text KIND = new Text("GCPDelegationToken/Test");

  public TestTokenIdentifierImpl() {
    super(KIND);
  }

  public TestTokenIdentifierImpl(URI uri, Text owner, String origin) {
    super(KIND, owner, owner, owner, uri, origin);
  }
}

