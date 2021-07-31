/*
 * Copyright 2020 Google LLC. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.hadoop.conf.Configuration;

/** Hadoop configuration property */
public class HadoopConfigurationProperty<T> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final Joiner COMMA_JOINER = Joiner.on(',');
  private static final Splitter COMMA_SPLITTER = Splitter.on(',');

  private final String key;
  private final List<String> deprecatedKeys;
  private final T defaultValue;

  private List<String> keyPrefixes = ImmutableList.of("");

  public HadoopConfigurationProperty(String key) {
    this(key, null);
  }

  public HadoopConfigurationProperty(String key, T defaultValue, String... deprecatedKeys) {
    this.key = key;
    this.deprecatedKeys =
        deprecatedKeys == null ? ImmutableList.of() : ImmutableList.copyOf(deprecatedKeys);
    this.defaultValue = defaultValue;
  }

  public HadoopConfigurationProperty<T> withPrefixes(List<String> keyPrefixes) {
    this.keyPrefixes = ImmutableList.copyOf(keyPrefixes);
    return this;
  }

  public String getKey() {
    return key;
  }

  public List<String> getDeprecatedKeys() {
    return deprecatedKeys;
  }

  public T getDefault() {
    return defaultValue;
  }

  public T get(Configuration config, BiFunction<String, T, T> getterFn) {
    String lookupKey = getLookupKey(config, key, deprecatedKeys, (c, k) -> c.get(k) != null);
    return logProperty(lookupKey, getterFn.apply(lookupKey, defaultValue));
  }

  public String getPassword(Configuration config) {
    checkState(defaultValue == null || defaultValue instanceof String, "Not a string property");
    String lookupKey = getLookupKey(config, key, deprecatedKeys, (c, k) -> c.get(k) != null);
    char[] value;
    try {
      value = config.getPassword(lookupKey);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return logProperty(lookupKey, value == null ? (String) defaultValue : String.valueOf(value));
  }

  public Collection<String> getStringCollection(Configuration config) {
    checkState(
        defaultValue == null || defaultValue instanceof Collection, "Not a collection property");
    String lookupKey = getLookupKey(config, key, deprecatedKeys, (c, k) -> c.get(k) != null);
    String valueString =
        config.get(
            lookupKey,
            defaultValue == null ? null : COMMA_JOINER.join((Collection<?>) defaultValue));
    List<String> value = COMMA_SPLITTER.splitToList(nullToEmpty(valueString));
    return logProperty(lookupKey, value);
  }

  @SuppressWarnings("unchecked")
  public Map<String, String> getPropsWithPrefix(Configuration config) {
    checkState(defaultValue instanceof Map, "Not a map property");
    String lookupKey =
        getLookupKey(config, key, deprecatedKeys, (c, k) -> !getPropsWithPrefix(c, k).isEmpty());
    Map<String, String> propsWithPrefix = getPropsWithPrefix(config, lookupKey);
    return logProperty(
        lookupKey,
        propsWithPrefix.isEmpty() ? (Map<String, String>) defaultValue : propsWithPrefix);
  }

  private Map<String, String> getPropsWithPrefix(Configuration config, String confPrefix) {
    Map<String, String> configMap = new HashMap<>();
    for (Map.Entry<String, String> property : config) {
      String name = property.getKey();
      if (name.startsWith(confPrefix)) {
        String value = property.getValue();
        String keyName = name.substring(confPrefix.length());
        configMap.put(keyName, value);
      }
    }
    return configMap;
  }

  private String getLookupKey(
      Configuration config,
      String key,
      List<String> deprecatedKeys,
      BiFunction<Configuration, String, Boolean> checkFn) {
    for (String prefix : keyPrefixes) {
      String prefixedKey = prefix + key;
      if (checkFn.apply(config, prefixedKey)) {
        return prefixedKey;
      }
      for (String deprecatedKey : deprecatedKeys) {
        String prefixedDeprecatedKey = prefix + deprecatedKey;
        if (checkFn.apply(config, prefixedDeprecatedKey)) {
          logger.atWarning().log(
              "Using deprecated key '%s', use '%s' key instead.",
              prefixedDeprecatedKey, prefixedKey);
          return prefixedDeprecatedKey;
        }
      }
    }
    return keyPrefixes.get(0) + key;
  }

  private static <S> S logProperty(String key, S value) {
    logger.atFine().log("%s = %s", key, value);
    return value;
  }
}
