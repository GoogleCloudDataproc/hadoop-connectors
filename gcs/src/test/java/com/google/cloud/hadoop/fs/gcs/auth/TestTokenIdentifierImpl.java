/*
 * Copyright 2019 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs.auth;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

/** A test delegation token identifier implementation */
public class TestTokenIdentifierImpl extends DelegationTokenIdentifier {

  public static final Text KIND = new Text("GCPDelegationToken/Test");

  public TestTokenIdentifierImpl() {
    super(KIND);
  }

  public TestTokenIdentifierImpl(Text owner, Text renewer, Text realUser, Text service) {
    super(KIND, owner, renewer, realUser);
  }
}
