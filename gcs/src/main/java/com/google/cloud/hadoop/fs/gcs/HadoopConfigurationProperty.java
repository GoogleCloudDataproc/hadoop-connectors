/*
 * Copyright 2020 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.fs.gcs;

/** Hadoop configuration property for Google Cloud Storage Connector */
public class HadoopConfigurationProperty<T>
    extends com.google.cloud.hadoop.util.HadoopConfigurationProperty<T> {

  public HadoopConfigurationProperty(String key) {
    super(key);
  }

  public HadoopConfigurationProperty(String key, T defaultValue, String... deprecatedKeys) {
    super(key, defaultValue, deprecatedKeys);
  }
}
