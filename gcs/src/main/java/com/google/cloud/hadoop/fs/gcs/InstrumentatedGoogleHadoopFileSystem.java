/*
 * Copyright 2013 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

public class InstrumentatedGoogleHadoopFileSystem extends GoogleHadoopFileSystemBase
    implements IOStatisticsSource {

  /** Instrumentation to track Statistics */
  private GhfsInstrumentation instrumentation;

  /** Storage Statistics Bonded to the instrumentation. */
  private GhfsStorageStatistics storageStatistics;

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  InstrumentatedGoogleHadoopFileSystem() {}

  InstrumentatedGoogleHadoopFileSystem(GoogleCloudStorageFileSystem gcsfs) {
    super(gcsfs);
  }

  @Override
  public void initialize(URI path, Configuration config) throws IOException {
    super.initialize(path, config);
    instrumentation = new GhfsInstrumentation(path);
    storageStatistics =
        (GhfsStorageStatistics)
            GlobalStorageStatistics.INSTANCE.put(
                GhfsStorageStatistics.NAME, () -> new GhfsStorageStatistics(getIOStatistics()));
  }

  @Override
  protected void configureBuckets(GoogleCloudStorageFileSystem gcsFs) {}

  @Override
  public FSDataInputStream open(Path hadoopPath, int bufferSize) throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_OPEN);
    return super.open(hadoopPath, bufferSize);
  }

  @Override
  public FSDataOutputStream create(
      Path hadoopPath,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_CREATE);
    FSDataOutputStream response =
        super.create(
            hadoopPath, permission, overwrite, bufferSize, replication, blockSize, progress);
    instrumentation.fileCreated();
    return response;
  }

  @Override
  public FSDataOutputStream createNonRecursive(
      Path hadoopPath,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {

    entryPoint(GhfsStatistic.INVOCATION_CREATE_NON_RECURSIVE);
    return super.createNonRecursive(
        hadoopPath, permission, flags, bufferSize, replication, blockSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_RENAME);
    return super.rename(src, dst);
  }

  @Override
  public boolean delete(Path hadoopPath, boolean recursive) throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_DELETE);
    boolean response;
    try {
      response = super.delete(hadoopPath, recursive);
      instrumentation.fileDeleted(1);
    } catch (IOException e) {
      incrementStatistic(GhfsStatistic.FILES_DELETE_REJECTED);
      throw e;
    }
    return response;
  }

  @Override
  public FileStatus[] listStatus(Path hadoopPath) throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_LIST_STATUS);
    entryPoint(GhfsStatistic.INVOCATION_LIST_FILES);
    return super.listStatus(hadoopPath);
  }

  @Override
  public boolean mkdirs(Path hadoopPath, FsPermission permission) throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_MKDIRS);
    boolean response = super.mkdirs(hadoopPath, permission);
    instrumentation.directoryCreated();
    return response;
  }

  @Override
  public FileStatus getFileStatus(Path hadoopPath) throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_GET_FILE_STATUS);
    return super.getFileStatus(hadoopPath);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_GLOB_STATUS);
    return super.globStatus(pathPattern, filter);
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_GET_DELEGATION_TOKEN);
    return super.getDelegationToken(renewer);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst)
      throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_COPY_FROM_LOCAL_FILE);
    super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
      throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_COPY_FROM_LOCAL_FILE);
    super.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  @Override
  public FileChecksum getFileChecksum(Path hadoopPath) throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_GET_FILE_CHECKSUM);
    return super.getFileChecksum(hadoopPath);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_EXISTS);
    return super.exists(f);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
    entryPoint(GhfsStatistic.INVOCATION_LIST_LOCATED_STATUS);
    return super.listLocatedStatus(f);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {

    return trackDuration(
        instrumentation,
        GhfsStatistic.INVOCATION_XATTR_GET_NAMED.getSymbol(),
        () -> super.getXAttr(path, name));
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return trackDuration(
        instrumentation,
        GhfsStatistic.INVOCATION_XATTR_GET_MAP.getSymbol(),
        () -> super.getXAttrs(path));
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
    return trackDuration(
        instrumentation,
        GhfsStatistic.INVOCATION_XATTR_GET_NAMED_MAP.getSymbol(),
        () -> super.getXAttrs(path, names));
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return trackDuration(
        instrumentation,
        GhfsStatistic.INVOCATION_OP_XATTR_LIST.getSymbol(),
        () -> super.listXAttrs(path));
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

  /**
   * Entry point to an operation. Increments the statistic; verifies the FS is active.
   *
   * @param operation The operation to increment
   */
  public void entryPoint(GhfsStatistic operation) {
    if (isClosed()) {
      return;
    }
    incrementStatistic(operation);
  }

  /**
   * Increment a statistic by 1. This increments both the and storage statistics.
   *
   * @param statistic The operation to increment
   */
  protected void incrementStatistic(GhfsStatistic statistic) {
    incrementStatistic(statistic, 1);
  }

  /**
   * Increment a statistic by a specific value. This increments both the instrumentation and storage
   * statistics.
   *
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  protected void incrementStatistic(GhfsStatistic statistic, long count) {
    instrumentation.incrementCounter(statistic, count);
  }

  /**
   * Get the storage statistics of this filesystem.
   *
   * @return the storage statistics
   */
  @Override
  public GhfsStorageStatistics getStorageStatistics() {
    return storageStatistics;
  }

  /** Get the instrumentation's IOStatistics. */
  @Override
  public IOStatistics getIOStatistics() {
    if (instrumentation == null) {
      return null;
    }
    setHttpStatistics();
    return instrumentation.getIOStatistics();
  }

  public GhfsInstrumentation getInstrumentation() {
    return instrumentation;
  }

  /** Set the GCS statistic keys */
  private void setHttpStatistics() {
    try {
      getGcsFs()
          .getGcs()
          .getStatistics()
          .forEach(
              (k, v) -> {
                GhfsStatistic statisticKey = GhfsStatistic.fromSymbol("ACTION_" + k);
                checkNotNull(statisticKey, "statistic key for %s must not be null", k);
                clearStats(statisticKey.getSymbol());
                incrementStatistic(statisticKey, v);
              });
    } catch (Exception e) {
      logger.atWarning().withCause(e).log("Error while getting GCS statistics");
    }
  }

  private void clearStats(String key) {
    instrumentation.getIOStatistics().getCounterReference(key).set(0L);
  }
}
