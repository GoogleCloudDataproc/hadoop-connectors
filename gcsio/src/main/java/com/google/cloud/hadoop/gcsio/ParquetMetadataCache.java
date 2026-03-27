/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetMetadataCache {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetMetadataCache.class);
  private static final String INDEX_DB_NAME = "parquet_metadata_index.db";

  private final String baseGcsPath;
  private final Path localCacheDir;
  private final Storage storage;
  private final List<String> indexFiles;

  // Cache for SQLite connections to local index files
  private final LoadingCache<String, Connection> connectionCache;

  public ParquetMetadataCache(String baseGcsPath, String localCacheDirPrefix) {
    this.baseGcsPath = baseGcsPath.endsWith("/") ? baseGcsPath : baseGcsPath + "/";
    this.localCacheDir =
        Paths.get(System.getProperty("java.io.tmpdir"), localCacheDirPrefix, "parquet_cache");
    this.storage = StorageOptions.getDefaultInstance().getService();

    try {
      System.out.println(this.localCacheDir);
      Files.createDirectories(this.localCacheDir);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create local cache directory: " + this.localCacheDir, e);
    }

    this.indexFiles = loadIndexFiles();
    LOG.info("Loaded index files: {}", indexFiles);

    this.connectionCache =
        CacheBuilder.newBuilder()
            .maximumSize(100) // Maximum number of connections to cache
            .build(
                new CacheLoader<String, Connection>() {
                  @Override
                  public Connection load(String localDbPath) throws SQLException {
                    return DriverManager.getConnection("jdbc:sqlite:" + localDbPath);
                  }
                });
  }

  private List<String> loadIndexFiles() {
    String bucketName = getBucketName(baseGcsPath);
    String prefix = getPathWithoutBucket(baseGcsPath + "**" + INDEX_DB_NAME);

    List<String> indices = new ArrayList<>();
    try {
      storage
          .list(bucketName, Storage.BlobListOption.matchGlob(prefix))
          .iterateAll()
          .forEach(
              blob -> {
                if (blob.getName().endsWith(INDEX_DB_NAME)) {
                  indices.add("gs://" + bucketName + "/" + blob.getName());
                }
              });
    } catch (Exception e) {
      LOG.error("Error listing index files in {}: {}", baseGcsPath, e.getMessage(), e);
    }
    return ImmutableList.copyOf(indices);
  }

  private Optional<String> findNearestIndex(String objectGcsPath) {
    if (!objectGcsPath.startsWith(baseGcsPath)) {
      return Optional.empty();
    }

    String bucketName = getBucketName(objectGcsPath);
    String relativePath = objectGcsPath.substring(("gs://" + bucketName + "/").length());

    Path currentPath = Paths.get(relativePath).getParent();
    while (currentPath != null && !currentPath.toString().isEmpty()) {
      String indexGcsPath = "gs://" + bucketName + "/" + currentPath.resolve(INDEX_DB_NAME);
      if (indexFiles.contains(indexGcsPath)) {
        return Optional.of(indexGcsPath);
      }
      if (("gs://" + bucketName + "/" + currentPath).length() <= baseGcsPath.length()) {
        break;
      }
      currentPath = currentPath.getParent();
    }
    // Check base path itself
    String baseIndex = baseGcsPath + INDEX_DB_NAME;
    if (indexFiles.contains(baseIndex)) {
      return Optional.of(baseIndex);
    }

    return Optional.empty();
  }

  private Path downloadIndex(String indexGcsPath) throws IOException {
    String bucketName = getBucketName(indexGcsPath);
    String blobName = getPathWithoutBucket(indexGcsPath);

    Path localPath = localCacheDir.resolve(bucketName).resolve(blobName);

    if (!Files.exists(localPath)) {
      LOG.info("Downloading {} to {}", indexGcsPath, localPath);
      Files.createDirectories(localPath.getParent());
      Blob blob = storage.get(bucketName, blobName);
      if (blob != null && blob.exists()) {
        blob.downloadTo(localPath);
        LOG.info("Successfully downloaded {} to {}", indexGcsPath, localPath);
      } else {
        throw new IOException("Index file not found in GCS: " + indexGcsPath);
      }
    } else {
      LOG.debug("Index file already cached: {}", localPath);
    }
    return localPath;
  }

  public Optional<ParquetObjectMetadata> getMetadata(String objectGcsPath) {
    Optional<String> indexGcsPathOpt = findNearestIndex(objectGcsPath);
    if (!indexGcsPathOpt.isPresent()) {
      LOG.warn("No index file found for {}", objectGcsPath);
      return Optional.empty();
    }

    String indexGcsPath = indexGcsPathOpt.get();
    try {
      Path localIndexPatn = downloadIndex(indexGcsPath);
      Connection conn = connectionCache.get(localIndexPatn.toString());

      String indexDir = indexGcsPath.substring(0, indexGcsPath.lastIndexOf('/'));
      String objectName = BlobId.fromGsUtilUri(objectGcsPath).getName();
      //      String objectName = objectGcsPath.substring(indexDir.length() + 1);

      String query =
          "SELECT file_size, footer_length, raw_metadata FROM metadata_index WHERE object_name = ?";
      try (PreparedStatement pstmt = conn.prepareStatement(query)) {
        pstmt.setString(1, objectName);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
          long fileSize = rs.getLong("file_size");
          int footerLength = rs.getInt("footer_length");
          byte[] rawMetadata = rs.getBytes("raw_metadata");
          return Optional.of(
              new ParquetObjectMetadata(objectName, fileSize, footerLength, rawMetadata));
        } else {
          LOG.info("Metadata not found for {} in {}", objectName, indexGcsPath);
          return Optional.empty();
        }
      }
    } catch (IOException e) {
      LOG.error("Error downloading index file {}: {}", indexGcsPath, e.getMessage(), e);
    } catch (SQLException e) {
      LOG.error("SQLite error accessing {}: {}", indexGcsPath, e.getMessage(), e);
    } catch (ExecutionException e) {
      LOG.error("Error getting connection from cache for {}: {}", indexGcsPath, e.getMessage(), e);
    }
    return Optional.empty();
  }

  private String getBucketName(String gcsPath) {
    return gcsPath.substring(5).split("/")[0];
  }

  private String getPathWithoutBucket(String gcsPath) {
    String noGsPrefix = gcsPath.substring(5);
    return noGsPrefix.substring(noGsPrefix.indexOf('/') + 1);
  }

  public static class ParquetObjectMetadata {
    private final String objectName;
    private final long fileSize;
    private final int footerLength;
    private final byte[] rawMetadata;

    public ParquetObjectMetadata(
        String objectName, long fileSize, int footerLength, byte[] rawMetadata) {
      this.objectName = objectName;
      this.fileSize = fileSize;
      this.footerLength = footerLength;
      this.rawMetadata = rawMetadata;
    }

    public String getObjectName() {
      return objectName;
    }

    public long getFileSize() {
      return fileSize;
    }

    public int getFooterLength() {
      return footerLength;
    }

    public byte[] getRawMetadata() {
      return rawMetadata;
    }
  }

  // Example Usage
  public static void main(String[] args) {
    String basePath = "gs://gcs-hyd-iceberg-benchmark-warehouse/partitioned_warehouse/tpcds_sf1000";
    ParquetMetadataCache cache = new ParquetMetadataCache(basePath, "gcsio");

    String testObject =
        basePath
            + "/catalog_sales/data/cs_sold_date_sk=2450818/00000-37100-ac344383-7b67-478a-9b3f-da8e0ce699a4-0-00001.parquet";
    Optional<ParquetObjectMetadata> metadata = cache.getMetadata(testObject);

    if (metadata.isPresent()) {
      ParquetObjectMetadata meta = metadata.get();
      System.out.println("Metadata for " + testObject);
      System.out.println("  File Size: " + meta.getFileSize());
      System.out.println("  Footer Length: " + meta.getFooterLength());
      System.out.println(
          "  Raw Metadata Length: "
              + (meta.getRawMetadata() != null ? meta.getRawMetadata().length : 0));
    } else {
      System.out.println("Could not retrieve metadata for " + testObject);
    }

    String testObjectNoIndex = basePath + "/non_existent_table/data.parquet";
    metadata = cache.getMetadata(testObjectNoIndex);
    if (!metadata.isPresent()) {
      System.out.println("As expected, could not retrieve metadata for " + testObjectNoIndex);
    }
  }
}
