package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

/** Hadoop configuration property */
public class HadoopConfigurationProperty<T> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final Joiner COMMA_JOINER = Joiner.on(',');
  private static final Splitter COMMA_SPLITTER = Splitter.on(',');

  /** Key for all the hadoop properties. */
  public static final String PROPERTIES_KEY = "properties";
  public static final String PROPERTIES_DELIMITER = ".";

  private final String key;
  private final List<String> deprecatedKeys;
  private final T defaultValue;
  private Boolean isPrefix;

  private List<String> keyPrefixes = ImmutableList.of("");

  public HadoopConfigurationProperty(String key) {
    this(key, null);
  }

  public HadoopConfigurationProperty(Boolean isPrefix, String prefix) {
    this(prefix, null);
    this.isPrefix = isPrefix;
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
    String lookupKey = getLookupKey(config, key, deprecatedKeys);
    return logProperty(lookupKey, getterFn.apply(lookupKey, defaultValue));
  }

  public String getPassword(Configuration config) {
    checkState(defaultValue == null || defaultValue instanceof String, "Not a string property");
    String lookupKey = getLookupKey(config, key, deprecatedKeys);
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
    String lookupKey = getLookupKey(config, key, deprecatedKeys);
    String valueString =
        config.get(
            lookupKey,
            defaultValue == null ? null : COMMA_JOINER.join((Collection<?>) defaultValue));
    List<String> value = COMMA_SPLITTER.splitToList(nullToEmpty(valueString));
    return logProperty(lookupKey, value);
  }

  public Map<String, String> getHttpRequestHeaders(Configuration config) throws IOException {
    if (!isPrefix) {
      return null;
    }
    Map<String, String> httpHeaders = new HashMap<String,String>();
    // Retrieve all hadoop properties as string and then parse it using JsonParser.
    StringWriter out = new StringWriter();
    Configuration.dumpConfiguration(config, out);
    String allProperties = out.toString();
    JsonParser jsonParser = new JsonParser();
    JsonObject jsonObject = jsonParser.parse(allProperties).getAsJsonObject();
    JsonArray jsonArray = jsonObject.get(PROPERTIES_KEY).getAsJsonArray();
    Iterator<JsonElement> iterator = jsonArray.iterator();

    while (iterator.hasNext()) {
      JsonObject headerJsonObject = iterator.next().getAsJsonObject();
      String headerKey = headerJsonObject.get("key").getAsString();
      String headerValue = headerJsonObject.get("value").getAsString();
      // Strip the prefix off from the HTTP header property key. Header key starts from
      // |key.length() + PROPERTIES_DELIMITER.length()|.
      if (headerKey.startsWith(key)) {
        headerKey = headerKey.substring(key.length() + PROPERTIES_DELIMITER.length());
        httpHeaders.put(headerKey, headerValue);
      }
    }

    return httpHeaders;
  }

  private String getLookupKey(Configuration config, String key, List<String> deprecatedKeys) {
    for (String prefix : keyPrefixes) {
      String prefixedKey = prefix + key;
      if (config.get(prefixedKey) != null) {
        return prefixedKey;
      }
      for (String deprecatedKey : deprecatedKeys) {
        String prefixedDeprecatedKey = prefix + deprecatedKey;
        if (config.get(prefixedDeprecatedKey) != null) {
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
