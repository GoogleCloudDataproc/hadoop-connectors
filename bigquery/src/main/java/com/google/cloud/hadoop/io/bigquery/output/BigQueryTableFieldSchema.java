/*
 * Copyright 2018 Google LLC
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
package com.google.cloud.hadoop.io.bigquery.output;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toList;

import com.google.api.services.bigquery.model.TableFieldSchema;
import java.util.List;

/**
 * Wrapper for BigQuery {@link TableFieldSchema}.
 *
 * <p>This class is used to avoid client code to depend on BigQuery API classes, so that there is no
 * potential conflict between different versions of BigQuery API libraries in the client.
 *
 * @see TableFieldSchema
 */
public class BigQueryTableFieldSchema {
  private final TableFieldSchema fieldSchema;

  public BigQueryTableFieldSchema() {
    this.fieldSchema = new TableFieldSchema();
  }

  BigQueryTableFieldSchema(TableFieldSchema fieldSchema) {
    checkNotNull(fieldSchema);
    this.fieldSchema = fieldSchema;
  }

  /** @see TableFieldSchema#getMode() */
  public String getMode() {
    return fieldSchema.getMode();
  }

  /** @see TableFieldSchema#setMode(String) */
  public BigQueryTableFieldSchema setMode(String mode) {
    fieldSchema.setMode(mode);
    return this;
  }

  /** @see TableFieldSchema#getName() */
  public String getName() {
    return fieldSchema.getName();
  }

  /** @see TableFieldSchema#setName(String) */
  public BigQueryTableFieldSchema setName(String name) {
    fieldSchema.setName(name);
    return this;
  }

  /** @see TableFieldSchema#getType() */
  public String getType() {
    return fieldSchema.getType();
  }

  /** @see TableFieldSchema#setType(String) */
  public BigQueryTableFieldSchema setType(String type) {
    fieldSchema.setType(type);
    return this;
  }

  /**
   * Gets the nested schema fields if the type property is set to RECORD.
   *
   * @see TableFieldSchema#getFields()
   */
  public List<BigQueryTableFieldSchema> getFields() {
    return get().getFields().stream().map(BigQueryTableFieldSchema::new).collect(toList());
  }

  /**
   * Sets the nested schema fields if the type property is set to RECORD.
   *
   * @see TableFieldSchema#setFields(List)
   */
  public BigQueryTableFieldSchema setFields(java.util.List<BigQueryTableFieldSchema> fields) {
    fieldSchema.setFields(fields.stream().map(BigQueryTableFieldSchema::get).collect(toList()));
    return this;
  }

  @Override
  public int hashCode() {
    return fieldSchema.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof BigQueryTableFieldSchema)) {
      return false;
    }
    BigQueryTableFieldSchema another = (BigQueryTableFieldSchema) object;
    return fieldSchema.equals(another.fieldSchema);
  }

  TableFieldSchema get() {
    return fieldSchema;
  }

  static BigQueryTableFieldSchema wrap(TableFieldSchema fieldSchema) {
    return new BigQueryTableFieldSchema(fieldSchema);
  }
}
