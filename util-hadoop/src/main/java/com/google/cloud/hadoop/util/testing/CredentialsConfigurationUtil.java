/*
 * Copyright 2014 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util.testing;

import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AUTHENTICATION_TYPE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.BASE_KEY_PREFIX;

import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AuthenticationType;
import org.apache.hadoop.conf.Configuration;

/** Utility methods for creating Configuration objects for use in testing. */
public class CredentialsConfigurationUtil {

  public static void addTestConfigurationSettings(Configuration configuration) {
    configuration.setEnum(
        BASE_KEY_PREFIX + AUTHENTICATION_TYPE_SUFFIX.getKey(), AuthenticationType.UNAUTHENTICATED);
  }

  public static Configuration getTestConfiguration() {
    Configuration configuration = new Configuration();
    addTestConfigurationSettings(configuration);
    return configuration;
  }
}
