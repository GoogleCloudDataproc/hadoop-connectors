package com.google.cloud.hadoop.fs.gcs;

import com.google.api.client.http.HttpRequest;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

public class InstrumentatedGoogleHadoopFileSystem extends GoogleHadoopFileSystemBase
    implements IOStatisticsSource {

  GhfsInstrumentation instrumentation;

  InstrumentatedGoogleHadoopFileSystem() {
    super();
  }

  InstrumentatedGoogleHadoopFileSystem(GoogleCloudStorageFileSystem gcsfs) {
    super(gcsfs);
  }

  @Override
  public void initialize(URI path, Configuration config) throws IOException {
    super.initialize(path, config);
    this.instrumentation = new GhfsInstrumentation(path);
  }

  @Override
  protected String getHomeDirectorySubpath() {
    return null;
  }

  @Override
  public Path getHadoopPath(URI gcsPath) {
    return null;
  }

  @Override
  public URI getGcsPath(Path hadoopPath) {
    return null;
  }

  @Override
  public Path getDefaultWorkingDirectory() {
    return null;
  }

  @Override
  public Path getFileSystemRoot() {
    return null;
  }

  @Override
  public String getScheme() {
    return null;
  }

  @Override
  protected void configureBuckets(GoogleCloudStorageFileSystem gcsFs) throws IOException {}

  @Override
  public void close() throws IOException {
    if (!this.LazyFs) {
      setHttpStatistics();
    }
    super.close();
  }

  /**
   * Get the instrumentation's IOStatistics.
   *
   * @return
   */
  @Override
  public IOStatistics getIOStatistics() {
    return instrumentation != null ? instrumentation.getIOStatistics() : null;
  }

  public GhfsInstrumentation getInstrumentation() {
    return this.instrumentation;
  }

  /** Set the Value for http get and head request related statistics keys */
  private void setHttpStatistics() {
    if (!isClosed()) {
      if (getGcsFs() != null) {
        long get_failures =
            getGcsFs()
                .getGcs()
                .getStatistics(GoogleCloudStorageStatistics.ACTION_HTTP_GET_REQUEST_FAILURES);
        if (get_failures > 0L) {
          instrumentation.incrementFailureStatistics(
              GhfsStatistic.ACTION_HTTP_GET_REQUEST.getSymbol(), get_failures);
        }
      }
    }
    if (this.gcsRequestsTracker != null) {
      ImmutableList<HttpRequest> httpRequests = this.gcsRequestsTracker.getAllRequests();
      for (HttpRequest req : httpRequests) {
        if (req.getRequestMethod() == "GET") {
          instrumentation.incrementCounter(GhfsStatistic.ACTION_HTTP_GET_REQUEST, 1);
        } else if (req.getRequestMethod() == "HEAD") {
          instrumentation.incrementCounter(GhfsStatistic.ACTION_HTTP_HEAD_REQUEST, 1);
        }
      }
    }
  }
}
