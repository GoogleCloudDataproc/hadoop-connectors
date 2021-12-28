/*
 * Copyright 2017 Google LLC
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
package com.google.cloud.hadoop.io.bigquery;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.Table;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * This class represents the logical "export" of BigQuery federated data source stored in Google
 * Cloud Storage.
 *
 * <p>It extends {@link UnshardedExportToCloudStorage} to share the {@link
 * org.apache.hadoop.mapreduce.lib.input.FileInputFormat} delegating logic.
 */
public class NoopFederatedExportToCloudStorage extends UnshardedExportToCloudStorage {

  protected final List<String> gcsPaths;

  public NoopFederatedExportToCloudStorage(
      Configuration configuration,
      ExportFileFormat fileFormat,
      BigQueryHelper bigQueryHelper,
      String projectId,
      Table table,
      @Nullable InputFormat<LongWritable, Text> delegateInputFormat) {
    super(
        configuration,
        getCommaSeparatedGcsPathList(table),
        fileFormat,
        bigQueryHelper,
        projectId,
        table,
        delegateInputFormat);
    checkNotNull(table.getExternalDataConfiguration());
    String inputType = fileFormat.getFormatIdentifier();
    String tableType = table.getExternalDataConfiguration().getSourceFormat();
    checkArgument(
        inputType.equals(tableType),
        "MapReduce fileFormat '%s' does not match BigQuery sourceFormat '%s'. Use the "
            + "appropriate InputFormat.",
        inputType,
        tableType);
    gcsPaths = table.getExternalDataConfiguration().getSourceUris();
  }

  @VisibleForTesting
  static String getCommaSeparatedGcsPathList(Table table) {
    checkNotNull(table.getExternalDataConfiguration());
    for (String uri : table.getExternalDataConfiguration().getSourceUris()) {
      checkArgument(uri.startsWith("gs://"), "Invalid GCS resource: '%s'", uri);
    }
    // FileInputFormat accepts a comma separated list of potentially globbed paths.
    return Joiner.on(",").join(table.getExternalDataConfiguration().getSourceUris());
  }

  @Override
  public void prepare() {
    // No-op
  }

  @Override
  public void beginExport() {
    // No-op
  }

  @Override
  public void waitForUsableMapReduceInput() {
    // No-op
  }

  @Override
  public List<String> getExportPaths() {
    return gcsPaths;
  }

  @Override
  public void cleanupExport() {
    // No-op
  }
}
