/*
 * Copyright 2013 Google Inc.
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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.BLOCK_SIZE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.DELEGATION_TOKEN_BINDING_CLASS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_APPLICATION_NAME_SUFFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CLOUD_LOGGING_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_FILE_CHECKSUM_TYPE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_GLOB_ALGORITHM;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OPERATION_TRACE_LOG_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_WORKING_DIRECTORY;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.PERMISSIONS_TO_REPORT;
import static com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.CLOUD_PLATFORM_SCOPE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.flogger.LazyArgs.lazy;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.fs.gcs.auth.GcsDelegationTokens;
import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.FeatureHeaderGenerator;
import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageStatistics;
import com.google.cloud.hadoop.gcsio.ListFileOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.TrackedFeatures;
import com.google.cloud.hadoop.gcsio.UpdatableItemInfo;
import com.google.cloud.hadoop.gcsio.UriPaths;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.AccessTokenProvider.AccessTokenType;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AccessTokenProviderCredentials;
import com.google.cloud.hadoop.util.ITraceFactory;
import com.google.cloud.hadoop.util.InvocationIdContext;
import com.google.cloud.hadoop.util.LoggingFormatter;
import com.google.cloud.hadoop.util.PropertyUtil;
import com.google.cloud.hadoop.util.TraceFactory;
import com.google.cloud.hadoop.util.interceptors.LoggingInterceptor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ascii;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.flogger.GoogleLogger;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.LambdaUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

/**
 * GoogleHadoopFileSystem is rooted in a single bucket at initialization time; in this case, Hadoop
 * paths no longer correspond directly to general GCS paths, and all Hadoop operations going through
 * this FileSystem will never touch any GCS bucket other than the bucket on which this FileSystem is
 * rooted.
 *
 * <p>This implementation sacrifices a small amount of cross-bucket interoperability in favor of
 * more straightforward FileSystem semantics and compatibility with existing Hadoop applications. In
 * particular, it is not subject to bucket-naming constraints, and files are allowed to be placed in
 * root.
 */
public class GoogleHadoopFileSystem extends FileSystem implements IOStatisticsSource {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /** URI scheme for GoogleHadoopFileSystem */
  public static final String SCHEME = GoogleCloudStorageFileSystem.SCHEME;

  /** Default value of replication factor. */
  static final short REPLICATION_FACTOR_DEFAULT = 3;
  /** Default PathFilter that accepts all paths. */
  private static final PathFilter DEFAULT_FILTER = path -> true;
  /** A resource file containing GCS related build properties. */
  private static final String PROPERTIES_FILE = "gcs.properties";
  /** The key in the PROPERTIES_FILE that contains the version built. */
  private static final String VERSION_PROPERTY = "gcs.connector.version";
  /** The version returned when one cannot be found in properties. */
  static final String UNKNOWN_VERSION = "0.0.0";

  // Request only object fields that are used in Hadoop FileStatus:
  // https://cloud.google.com/storage/docs/json_api/v1/objects#resource-representations
  private static final String OBJECT_FIELDS = "bucket,name,size,updated";
  private static final ListFileOptions LIST_OPTIONS =
      ListFileOptions.DEFAULT.toBuilder().setFields(OBJECT_FIELDS).build();
  private static final String XATTR_KEY_PREFIX = "GHFS_XATTR_";
  // Use empty array as null value because GCS API already uses null value to remove metadata key
  private static final byte[] XATTR_NULL_VALUE = new byte[0];
  private static final ThreadFactory DAEMON_THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("ghfs-thread-%d").setDaemon(true).build();

  /** Current version. */
  static final String VERSION;
  /** Identifies this version of the {@link GoogleHadoopFileSystem} library. */
  static final String GHFS_ID;

  static final String GETFILESTATUS_FILETYPE_HINT = "fs.gs.getfilestatus.filetype.hint";

  static {
    // Add formatter to root logger.
    LoggingFormatter.addFormatter(Logger.getLogger(""));
    VERSION =
        PropertyUtil.getPropertyOrDefault(
            GoogleHadoopFileSystem.class, PROPERTIES_FILE, VERSION_PROPERTY, UNKNOWN_VERSION);
    logger.atFine().log("GHFS version: %s", VERSION);
    GHFS_ID = String.format("GHFS/%s", VERSION);
  }

  /** The URI the File System is passed in initialize. */
  URI initUri;
  /** Delegation token support */
  GcsDelegationTokens delegationTokens = null;
  /**
   * Default block size. Note that this is the size that is reported to Hadoop FS clients. It does
   * not modify the actual block size of an underlying GCS object, because GCS JSON API does not
   * allow modifying or querying the value. Modifying this value allows one to control how many
   * mappers are used to process a given file.
   */
  private long defaultBlockSize = BLOCK_SIZE.getDefault();

  @VisibleForTesting GlobAlgorithm globAlgorithm = GCS_GLOB_ALGORITHM.getDefault();

  // The bucket the file system is rooted in used for default values of:
  // -- working directory
  // -- user home directories (only for Hadoop purposes).
  private Path fsRoot;
  /** Instrumentation to track Statistics */
  private GhfsInstrumentation instrumentation;
  /** Storage Statistics Bonded to the instrumentation. */
  private GhfsGlobalStorageStatistics globalStorageStatistics;

  private GhfsStorageStatistics storageStatistics;
  // Thread-pool used for background tasks.
  private ExecutorService backgroundTasksThreadPool =
      Executors.newCachedThreadPool(DAEMON_THREAD_FACTORY);
  private GcsFileChecksumType checksumType = GCS_FILE_CHECKSUM_TYPE.getDefault();
  /** Underlying GCS file system object. */
  private Supplier<GoogleCloudStorageFileSystem> gcsFsSupplier;

  private Supplier<VectoredIOImpl> vectoredIOSupplier;

  private boolean gcsFsInitialized = false;
  private boolean vectoredIOInitialized = false;
  /**
   * Current working directory; overridden in initialize() if {@link
   * GoogleHadoopFileSystemConfiguration#GCS_WORKING_DIRECTORY} is set.
   */
  private Path workingDirectory;

  /** The fixed reported permission of all files. */
  private FsPermission reportedPermissions;

  private ITraceFactory traceFactory = TraceFactory.get(/* isEnabled */ false);

  private LoggingInterceptor loggingInterceptor;

  /** Instrumentation to track Statistics */
  ITraceFactory getTraceFactory() {
    return this.traceFactory;
  }

  /**
   * Constructs an instance of GoogleHadoopFileSystem; the internal GoogleCloudStorageFileSystem
   * will be set up with config settings when initialize() is called.
   */
  public GoogleHadoopFileSystem() {
    StorageStatistics globalStats =
        GlobalStorageStatistics.INSTANCE.put(
            GhfsGlobalStorageStatistics.NAME, () -> new GhfsGlobalStorageStatistics());

    GlobalStorageStatistics.INSTANCE.put(
        GhfsThreadLocalStatistics.NAME,
        () -> ((GhfsGlobalStorageStatistics) globalStats).getThreadLocalStatistics());

    if (GhfsGlobalStorageStatistics.class.isAssignableFrom(globalStats.getClass())) {
      globalStorageStatistics = (GhfsGlobalStorageStatistics) globalStats;
    } else {
      logger.atWarning().log(
          "Encountered an error while registering to GlobalStorageStatistics. Some of the GCS connector metrics will not be reported to metrics sinks. globalStatsClassLoader=<%s>; classLoader=<%s>",
          globalStats.getClass().getClassLoader(),
          GhfsGlobalStorageStatistics.class.getClassLoader());
      globalStorageStatistics = GhfsGlobalStorageStatistics.DUMMY_INSTANCE;
    }

    GoogleCloudStorageEventBus.register(
        GoogleCloudStorageEventSubscriber.getInstance(globalStorageStatistics));

    globalStorageStatistics.incrementCounter(GoogleCloudStorageStatistics.GS_FILESYSTEM_CREATE, 1);
  }

  /**
   * Constructs an instance of GoogleHadoopFileSystem using the provided
   * GoogleCloudStorageFileSystem; initialize() will not re-initialize it.
   */
  @VisibleForTesting
  GoogleHadoopFileSystem(GoogleCloudStorageFileSystem gcsfs) {
    this();
    checkNotNull(gcsfs, "gcsFs must not be null");
    initializeGcsFs(gcsfs);
  }

  @Override
  public void initialize(URI path, Configuration config) throws IOException {
    logger.atFiner().log("initialize(path: %s, config: %s)", path, config);

    checkArgument(path != null, "path must not be null");
    checkArgument(config != null, "config must not be null");
    checkArgument(path.getScheme() != null, "scheme of path must not be null");
    checkArgument(path.getScheme().equals(getScheme()), "URI scheme not supported: %s", path);

    config =
        ProviderUtils.excludeIncompatibleCredentialProviders(config, GoogleHadoopFileSystem.class);
    super.initialize(path, config);

    initUri = path;

    // Set this configuration as the default config for this instance; configure()
    // will perform some file-system-specific adjustments, but the original should
    // be sufficient (and is required) for the delegation token binding initialization.
    setConf(config);

    if (GCS_CLOUD_LOGGING_ENABLE.get(getConf(), getConf()::getBoolean)) {
      initializeCloudLogger(config);
    }

    globAlgorithm = GCS_GLOB_ALGORITHM.get(config, config::getEnum);
    checksumType = GCS_FILE_CHECKSUM_TYPE.get(config, config::getEnum);
    defaultBlockSize = BLOCK_SIZE.get(config, config::getLong);
    reportedPermissions = new FsPermission(PERMISSIONS_TO_REPORT.get(config, config::get));

    initializeFsRoot();
    initializeWorkingDirectory(config);
    initializeDelegationTokenSupport(config);
    instrumentation = new GhfsInstrumentation(initUri);

    storageStatistics =
        (GhfsStorageStatistics)
            GlobalStorageStatistics.INSTANCE.put(
                GhfsStorageStatistics.NAME,
                () -> new GhfsStorageStatistics(instrumentation.getIOStatistics()));

    initializeVectoredIO(config, globalStorageStatistics, statistics);

    initializeGcsFs(config);

    this.traceFactory =
        TraceFactory.get(GCS_OPERATION_TRACE_LOG_ENABLE.get(config, config::getBoolean));

    globalStorageStatistics.incrementCounter(
        GoogleCloudStorageStatistics.GS_FILESYSTEM_INITIALIZE, 1);
  }

  private void initializeFsRoot() {
    String rootBucket = initUri.getAuthority();
    checkArgument(rootBucket != null, "No bucket specified in GCS URI: %s", initUri);
    // Validate root bucket name
    URI rootUri =
        UriPaths.fromStringPathComponents(
            rootBucket, /* objectName= */ null, /* allowEmptyObjectName= */ true);
    fsRoot = new Path(rootUri);
    logger.atFiner().log("Configured FS root: '%s'", fsRoot);
  }

  private void initializeWorkingDirectory(Configuration config) {
    String configWorkingDirectory = GCS_WORKING_DIRECTORY.get(config, config::get);
    if (isNullOrEmpty(configWorkingDirectory)) {
      logger.atWarning().log(
          "No working directory configured, using default: '%s'", workingDirectory);
    }
    // Use the public method to ensure proper behavior of normalizing and resolving the new
    // working directory relative to the initial filesystem-root directory.
    setWorkingDirectory(
        isNullOrEmpty(configWorkingDirectory) ? fsRoot : new Path(configWorkingDirectory));
    logger.atFiner().log(
        "Configured working directory: %s = %s",
        GCS_WORKING_DIRECTORY.getKey(), getWorkingDirectory());
  }

  private void initializeDelegationTokenSupport(Configuration config) throws IOException {
    logger.atFiner().log("initializeDelegationTokenSupport(config: %s)", config);
    // Load delegation token binding, if support is configured
    if (isNullOrEmpty(DELEGATION_TOKEN_BINDING_CLASS.get(config, config::get))) {
      return;
    }

    GcsDelegationTokens dts = new GcsDelegationTokens();
    Text service = new Text(fsRoot.toString());
    dts.bindToFileSystem(this, service);
    dts.init(config);
    dts.start();
    delegationTokens = dts;
    if (delegationTokens.isBoundToDT()) {
      logger.atFine().log(
          "initializeDelegationTokenSupport(config: %s): using existing delegation token", config);
    }
  }

  private synchronized void initializeGcsFs(Configuration config) throws IOException {
    if (gcsFsSupplier == null) {
      if (GCS_LAZY_INITIALIZATION_ENABLE.get(config, config::getBoolean)) {
        gcsFsSupplier =
            Suppliers.memoize(
                () -> {
                  try {
                    GoogleCloudStorageFileSystem gcsFs = createGcsFs(config);
                    gcsFsInitialized = true;
                    return gcsFs;
                  } catch (IOException e) {
                    GoogleCloudStorageEventBus.postOnException();
                    throw new RuntimeException("Failed to create GCS FS", e);
                  }
                });
      } else {
        initializeGcsFs(createGcsFs(config));
      }
    }
  }

  private synchronized void initializeVectoredIO(
      Configuration config,
      GhfsGlobalStorageStatistics globalStorageStatistics,
      FileSystem.Statistics statistics)
      throws IOException {
    if (vectoredIOSupplier == null) {
      vectoredIOSupplier =
          Suppliers.memoize(
              () -> {
                try {
                  VectoredIOImpl vectoredIO =
                      new VectoredIOImpl(
                          GoogleHadoopFileSystemConfiguration.getVectoredReadOptionBuilder(config)
                              .build(),
                          globalStorageStatistics);
                  vectoredIOInitialized = true;
                  return vectoredIO;
                } catch (Exception e) {
                  throw new RuntimeException("Failure initializing vectoredIO", e);
                }
              });
    }
  }

  private void initializeGcsFs(GoogleCloudStorageFileSystem gcsFs) {
    gcsFsSupplier = Suppliers.ofInstance(gcsFs);
    gcsFsInitialized = true;
  }

  private void initializeCloudLogger(Configuration config) throws IOException {
    GoogleCredentials credentials = getCredentials(config);
    String suffix = GCS_APPLICATION_NAME_SUFFIX.get(getConf(), getConf()::get);
    loggingInterceptor = createLoggingInterceptor(credentials, suffix);
    // Add the LoggingInterceptor to the root logger
    Logger rootLogger = Logger.getLogger("");
    rootLogger.addHandler(loggingInterceptor);
  }

  @VisibleForTesting
  LoggingInterceptor createLoggingInterceptor(GoogleCredentials credentials, String suffix) {
    return new LoggingInterceptor(credentials, suffix);
  }

  private GoogleCloudStorageFileSystem createGcsFs(Configuration config) throws IOException {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config).build();

    GoogleCredentials credentials = getCredentials(config);

    AccessTokenProvider accessTokenProvider =
        credentials instanceof AccessTokenProviderCredentials
            ? ((AccessTokenProviderCredentials) credentials).getAccessTokenProvider()
            : null;
    return accessTokenProvider != null
            && accessTokenProvider.getAccessTokenType() == AccessTokenType.DOWNSCOPED
        ? new GoogleCloudStorageFileSystemImpl(
            /* credentials= */ null,
            accessBoundaries -> accessTokenProvider.getAccessToken(accessBoundaries).getToken(),
            gcsFsOptions)
        : new GoogleCloudStorageFileSystemImpl(
            credentials, /* downscopedAccessTokenFn= */ null, gcsFsOptions);
  }

  private GoogleCredentials getCredentials(Configuration config) throws IOException {
    AccessTokenProvider delegationAccessTokenProvider = getDelegationAccessTokenProvider(config);
    GoogleCredentials credentials =
        delegationAccessTokenProvider == null
            ? HadoopCredentialsConfiguration.getCredentials(config, GCS_CONFIG_PREFIX)
            // If impersonation service account exists, then use current credentials
            // to request access token for the impersonating service account.
            : new AccessTokenProviderCredentials(delegationAccessTokenProvider)
                .createScoped(CLOUD_PLATFORM_SCOPE);
    return Optional.ofNullable(
            HadoopCredentialsConfiguration.getImpersonatedCredentials(
                config, credentials, GCS_CONFIG_PREFIX))
        .orElse(credentials);
  }

  private static boolean isImplicitDirectory(FileStatus curr) {
    // Modification time of 0 indicates implicit directory.
    return curr.isDirectory() && curr.getModificationTime() == 0;
  }

  /** Helper method to get the UGI short username */
  private static String getUgiUserName() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return ugi.getShortUserName();
  }

  /**
   * Converts the given FileStatus to its string representation.
   *
   * @param fileStatus FileStatus to convert.
   * @return String representation of the given FileStatus.
   */
  private static String fileStatusToString(FileStatus fileStatus) {
    checkNotNull(fileStatus, "fileStatus should not be null");
    return String.format(
        "path: %s, isDir: %s, len: %d, owner: %s",
        fileStatus.getPath(), fileStatus.isDirectory(), fileStatus.getLen(), fileStatus.getOwner());
  }

  private static FileChecksum getFileChecksum(GcsFileChecksumType type, FileInfo fileInfo)
      throws IOException {
    switch (type) {
      case NONE:
        return null;
      case CRC32C:
        return new GcsFileChecksum(type, fileInfo.getCrc32cChecksum());
      case MD5:
        return new GcsFileChecksum(type, fileInfo.getMd5Checksum());
    }
    GoogleCloudStorageEventBus.postOnException();
    throw new IOException("Unrecognized GcsFileChecksumType: " + type);
  }

  @Override
  protected void checkPath(Path path) {
    logger.atFiner().log("checkPath(path: %s)", path);
    // Validate scheme
    URI uri = path.toUri();

    String scheme = uri.getScheme();
    if (scheme != null && !scheme.equalsIgnoreCase(getScheme())) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IllegalArgumentException(
          String.format(
              "Wrong scheme: %s, in path: %s, expected scheme: %s", scheme, path, getScheme()));
    }

    String bucket = uri.getAuthority();
    String rootBucket = fsRoot.toUri().getAuthority();

    // Bucket-less URIs will be qualified later
    if (bucket == null || bucket.equals(rootBucket)) {
      return;
    }

    GoogleCloudStorageEventBus.postOnException();
    throw new IllegalArgumentException(
        String.format(
            "Wrong bucket: %s, in path: %s, expected bucket: %s", bucket, path, rootBucket));
  }

  /**
   * Validates that GCS path belongs to this file system. The bucket must match the root bucket
   * provided at initialization time.
   */
  Path getHadoopPath(URI gcsPath) {
    logger.atFiner().log("getHadoopPath(gcsPath: %s)", gcsPath);

    // Handle root. Delegate to getGcsPath on "gs:/" to resolve the appropriate gs://<bucket> URI.
    if (gcsPath.equals(getGcsPath(fsRoot))) {
      return fsRoot;
    }

    StorageResourceId resourceId = StorageResourceId.fromUriPath(gcsPath, true);

    checkArgument(!resourceId.isRoot(), "Missing authority in gcsPath '%s'", gcsPath);
    String rootBucket = fsRoot.toUri().getAuthority();
    checkArgument(
        resourceId.getBucketName().equals(rootBucket),
        "Authority of URI '%s' doesn't match root bucket '%s'",
        resourceId.getBucketName(),
        rootBucket);

    Path hadoopPath =
        new Path(
            fsRoot,
            new Path(/* schema= */ null, /* authority= */ null, resourceId.getObjectName()));
    logger.atFiner().log("getHadoopPath(gcsPath: %s): %s", gcsPath, hadoopPath);
    return hadoopPath;
  }

  /**
   * Translates a "gs:/" style hadoopPath (or relative path which is not fully-qualified) into the
   * appropriate GCS path which is compatible with the underlying GcsFs.
   */
  URI getGcsPath(Path hadoopPath) {
    logger.atFiner().log("getGcsPath(hadoopPath: %s)", hadoopPath);

    // Convert to fully qualified absolute path; the Path object will call back to get our current
    // workingDirectory as part of fully resolving the path.
    Path resolvedPath = makeQualified(hadoopPath);

    String objectName = resolvedPath.toUri().getPath();
    if (objectName != null && resolvedPath.isAbsolute()) {
      // Strip off leading '/' because GoogleCloudStorageFileSystem.getPath appends it explicitly
      // between bucket and objectName.
      objectName = objectName.substring(1);
    }

    // Construct GCS path URI
    String rootBucket = fsRoot.toUri().getAuthority();
    URI gcsPath =
        UriPaths.fromStringPathComponents(rootBucket, objectName, /* allowEmptyObjectName= */ true);
    logger.atFiner().log("getGcsPath(hadoopPath: %s): %s", hadoopPath, gcsPath);
    return gcsPath;
  }

  @Override
  public String getScheme() {
    return GoogleCloudStorageFileSystem.SCHEME;
  }

  @Override
  public FSDataInputStream open(Path hadoopPath, int bufferSize) throws IOException {
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_OPEN,
        hadoopPath,
        this.traceFactory,
        () -> {
          checkArgument(hadoopPath != null, "hadoopPath must not be null");
          checkOpen();
          logger.atFiner().log(
              "open(hadoopPath: %s, bufferSize: %d [ignored])", hadoopPath, bufferSize);
          URI gcsPath = getGcsPath(hadoopPath);
          return new FSDataInputStream(GoogleHadoopFSInputStream.create(this, gcsPath, statistics));
        });
  }

  /** This is an experimental API and can change without notice. */
  public FSDataInputStream open(FileStatus status) throws IOException {
    logger.atFine().log("openWithStatus(%s)", status);

    if (!GoogleHadoopFileStatus.class.isAssignableFrom(status.getClass())) {
      throw new IllegalArgumentException(
          String.format(
              "Expected status to be of type GoogleHadoopFileStatus, but found %s",
              status.getClass()));
    }

    GoogleHadoopFileStatus fileStatus = (GoogleHadoopFileStatus) status;

    checkPath(status.getPath());

    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_OPEN,
        status.getPath(),
        this.traceFactory,
        () -> {
          checkOpen();
          return new FSDataInputStream(
              GoogleHadoopFSInputStream.create(this, fileStatus.getFileInfo(), statistics));
        });
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
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_CREATE,
        hadoopPath,
        traceFactory,
        () -> {
          checkArgument(hadoopPath != null, "hadoopPath must not be null");
          checkArgument(replication > 0, "replication must be a positive integer: %s", replication);
          checkArgument(blockSize > 0, "blockSize must be a positive integer: %s", blockSize);

          checkOpen();

          logger.atFiner().log(
              "create(hadoopPath: %s, overwrite: %b, bufferSize: %d [ignored])",
              hadoopPath, overwrite, bufferSize);

          FSDataOutputStream response =
              new FSDataOutputStream(
                  new GoogleHadoopOutputStream(
                      this,
                      getGcsPath(hadoopPath),
                      CreateFileOptions.builder()
                          .setWriteMode(
                              overwrite
                                  ? CreateFileOptions.WriteMode.OVERWRITE
                                  : CreateFileOptions.WriteMode.CREATE_NEW)
                          .setMinSyncInterval(
                              GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL.getTimeDuration(getConf()))
                          .build(),
                      statistics),
                  statistics);

          incrementStatistic(GhfsStatistic.FILES_CREATED);
          return response;
        });
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
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_CREATE_NON_RECURSIVE,
        hadoopPath,
        traceFactory,
        () -> {

          // incrementStatistic(GhfsStatistic.INVOCATION_CREATE_NON_RECURSIVE);

          URI gcsPath = getGcsPath(checkNotNull(hadoopPath, "hadoopPath must not be null"));
          URI parentGcsPath = UriPaths.getParentPath(gcsPath);
          if (!getGcsFs().getFileInfo(parentGcsPath).exists()) {
            GoogleCloudStorageEventBus.postOnException();
            throw new FileNotFoundException(
                String.format(
                    "Can not create '%s' file, because parent folder does not exist: %s",
                    gcsPath, parentGcsPath));
          }
          return create(
              hadoopPath,
              permission,
              flags.contains(CreateFlag.OVERWRITE),
              bufferSize,
              replication,
              blockSize,
              progress);
        });
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_RENAME,
        String.format("rename(%s -> %s)", src, dst),
        this.traceFactory,
        () -> {
          checkArgument(src != null, "src must not be null");
          checkArgument(dst != null, "dst must not be null");

          // Even though the underlying GCSFS will also throw an IAE if src is root, since our
          // filesystem
          // root happens to equal the global root, we want to explicitly check it here since
          // derived
          // classes may not have filesystem roots equal to the global root.
          if (this.makeQualified(src).equals(fsRoot)) {
            logger.atFiner().log("rename(src: %s, dst: %s): false [src is a root]", src, dst);
            return false;
          }
          try {
            renameInternal(src, dst);
          } catch (IOException e) {
            GoogleCloudStorageEventBus.postOnException();
            if (ApiErrorExtractor.INSTANCE.requestFailure(e)) {
              throw e;
            }
            logger.atFiner().withCause(e).log("rename(src: %s, dst: %s): false [failed]", src, dst);
            return false;
          }
          return true;
        },
        // This is a unique hadoop operation where tracking can help determine if rename is executed
        // as a atomic GCS MOVE or emulated via non-atomic COPY+DELETE sequence.
        TrackedFeatures.RENAME_API);
  }

  /**
   * Tracks the duration of the operation {@code operation}. Also setup operation tracking using
   * {@code ThreadTrace}.
   */
  private <B> B trackDurationWithTracing(
      DurationTrackerFactory factory,
      @Nonnull GhfsGlobalStorageStatistics stats,
      GhfsStatistic statistic,
      Object context,
      ITraceFactory traceFactory,
      CallableRaisingIOE<B> operation)
      throws IOException {
    InvocationIdContext.setInvocationId();
    return GhfsGlobalStorageStatistics.trackDuration(
        factory, stats, statistic, context, traceFactory, operation);
  }

  /**
   * Tracks the duration of the operation {@code operation} and a specific feature usage. Also setup
   * operation tracking using {@code ThreadTrace}.
   */
  private <B> B trackDurationWithTracing(
      DurationTrackerFactory factory,
      @Nonnull GhfsGlobalStorageStatistics stats,
      GhfsStatistic statistic,
      Object context,
      ITraceFactory traceFactory,
      CallableRaisingIOE<B> operation,
      TrackedFeatures feature)
      throws IOException {
    return trackDurationWithTracing(
        factory,
        stats,
        statistic,
        context,
        traceFactory,
        () -> FeatureHeaderGenerator.track(feature, operation::apply));
  }

  @Override
  public boolean delete(Path hadoopPath, boolean recursive) throws IOException {
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_DELETE,
        hadoopPath,
        traceFactory,
        () -> {
          boolean response;
          try {
            boolean result = true;
            checkArgument(hadoopPath != null, "hadoopPath must not be null");

            checkOpen();

            URI gcsPath = getGcsPath(hadoopPath);
            try {
              getGcsFs().delete(gcsPath, recursive);
            } catch (DirectoryNotEmptyException e) {
              throw e;
            } catch (IOException e) {
              GoogleCloudStorageEventBus.postOnException();
              if (ApiErrorExtractor.INSTANCE.requestFailure(e)) {
                throw e;
              }
              logger.atFiner().withCause(e).log(
                  "delete(hadoopPath: %s, recursive: %b): false [failed]", hadoopPath, recursive);
              result = false;
            }
            if (result) {
              logger.atFiner().log(
                  "delete(hadoopPath: %s, recursive: %b): true", hadoopPath, recursive);
            }
            response = result;

            incrementStatistic(GhfsStatistic.FILES_DELETED);
          } catch (IOException e) {
            GoogleCloudStorageEventBus.postOnException();
            incrementStatistic(GhfsStatistic.FILES_DELETE_REJECTED);
            throw e;
          }
          return response;
        });
  }

  @Override
  public FileStatus[] listStatus(Path hadoopPath) throws IOException {
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_LIST_STATUS,
        hadoopPath,
        traceFactory,
        () -> {
          checkArgument(hadoopPath != null, "hadoopPath must not be null");

          checkOpen();

          logger.atFiner().log("listStatus(hadoopPath: %s)", hadoopPath);

          URI gcsPath = getGcsPath(hadoopPath);
          List<FileStatus> status;

          try {
            List<FileInfo> fileInfos = getGcsFs().listFileInfo(gcsPath, LIST_OPTIONS);
            status = new ArrayList<>(fileInfos.size());
            String userName = getUgiUserName();
            for (FileInfo fileInfo : fileInfos) {
              status.add(getGoogleHadoopFileStatus(fileInfo, userName));
            }
          } catch (FileNotFoundException fnfe) {
            GoogleCloudStorageEventBus.postOnException();
            throw (FileNotFoundException)
                new FileNotFoundException(
                        String.format(
                            "listStatus(hadoopPath: %s): '%s' does not exist.",
                            hadoopPath, gcsPath))
                    .initCause(fnfe);
          }

          incrementStatistic(GhfsStatistic.INVOCATION_LIST_STATUS_RESULT_SIZE, status.size());
          return status.toArray(new FileStatus[0]);
        });
  }

  /**
   * Gets FileStatus of all files which are lexicographically greater than and equal the provided
   * path. It filters out any directory objects present in underneath storage.
   *
   * <p>This is an experimental API can change without notice.
   */
  public FileStatus[] listStatusStartingFrom(Path startFrom) throws IOException {
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_LIST_STATUS,
        startFrom,
        traceFactory,
        () -> {
          checkArgument(startFrom != null, "start offset path must not be null");

          checkOpen();

          logger.atFiner().log("listStatusStartingFrom(hadoopPath: %s)", startFrom);

          URI gcsPath = getGcsPath(startFrom);
          List<FileStatus> status;

          List<FileInfo> fileInfos = getGcsFs().listFileInfoStartingFrom(gcsPath, LIST_OPTIONS);
          status = new ArrayList<>(fileInfos.size());
          String userName = getUgiUserName();
          for (FileInfo fileInfo : fileInfos) {
            status.add(getGoogleHadoopFileStatus(fileInfo, userName));
          }
          incrementStatistic(GhfsStatistic.INVOCATION_LIST_STATUS_RESULT_SIZE, status.size());
          return status.toArray(new FileStatus[0]);
        });
  }

  @Override
  public boolean mkdirs(Path hadoopPath, FsPermission permission) throws IOException {
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_MKDIRS,
        hadoopPath,
        traceFactory,
        () -> {
          checkArgument(hadoopPath != null, "hadoopPath must not be null");

          checkOpen();

          URI gcsPath = getGcsPath(hadoopPath);
          try {
            getGcsFs().mkdirs(gcsPath);
          } catch (java.nio.file.FileAlreadyExistsException faee) {
            GoogleCloudStorageEventBus.postOnException();
            // Need to convert to the Hadoop flavor of FileAlreadyExistsException.
            throw (FileAlreadyExistsException)
                new FileAlreadyExistsException(
                        String.format(
                            "mkdirs(hadoopPath: %s, permission: %s): failed",
                            hadoopPath, permission))
                    .initCause(faee);
          }
          logger.atFiner().log(
              "mkdirs(hadoopPath: %s, permission: %s): true", hadoopPath, permission);
          boolean response = true;

          incrementStatistic(GhfsStatistic.DIRECTORIES_CREATED);
          return response;
        });
  }

  @Override
  public FileStatus getFileStatus(Path hadoopPath) throws IOException {
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_GET_FILE_STATUS,
        hadoopPath,
        traceFactory,
        () -> {
          checkArgument(hadoopPath != null, "hadoopPath must not be null");

          checkOpen();

          URI gcsPath = getGcsPath(hadoopPath);
          FileInfo fileInfo = getGcsFs().getFileInfo(gcsPath);
          if (!fileInfo.exists()) {
            throw new FileNotFoundException(
                String.format(
                    "%s not found: %s", fileInfo.isDirectory() ? "Directory" : "File", hadoopPath));
          }
          String userName = getUgiUserName();
          return getGoogleHadoopFileStatus(fileInfo, userName);
        });
  }

  /**
   * Gets FileStatus with Hint. Can be used if the caller want to pass the path type (file vs
   * directory) hint. This hint can be used to prioritize GCS API calls inorder to improve
   * performance and reduce redundant API calls without compromising performance and API behaviour.
   * Currently, only "file" type hint is supported.
   *
   * <p>This is an experimental API can change without notice.
   */
  public FileStatus getFileStatusWithHint(Path hadoopPath, Configuration hint) throws IOException {
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_GET_FILE_STATUS,
        hadoopPath,
        traceFactory,
        () -> {
          checkArgument(hadoopPath != null, "hadoopPath must not be null");
          checkArgument(hint != null, "hint must not be null");

          checkOpen();

          GoogleCloudStorageFileSystemImpl.PathTypeHint pathTypeHint = getHint(hint, hadoopPath);
          if (pathTypeHint == GoogleCloudStorageFileSystemImpl.PathTypeHint.NONE) {
            logger.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
                "No file type hint was provided for path %s", hadoopPath);
          }

          URI gcsPath = getGcsPath(hadoopPath);
          FileInfo fileInfo = getGcsFs().getFileInfoWithHint(gcsPath, pathTypeHint);
          if (!fileInfo.exists()) {
            throw new FileNotFoundException(
                String.format(
                    "%s not found: %s", fileInfo.isDirectory() ? "Directory" : "File", hadoopPath));
          }
          String userName = getUgiUserName();
          return getGoogleHadoopFileStatus(fileInfo, userName);
        });
  }

  private GoogleCloudStorageFileSystemImpl.PathTypeHint getHint(Configuration hint, Path path) {
    String hintString = hint.get(GETFILESTATUS_FILETYPE_HINT);
    if (hintString != null && hintString.toLowerCase().equals("file")) {
      return GoogleCloudStorageFileSystemImpl.PathTypeHint.FILE;
    }

    logger.atWarning().atMostEvery(10, TimeUnit.SECONDS).log(
        "Unexpected hint '%s' received. Ignoring. path=%s", hintString, path);

    return GoogleCloudStorageFileSystemImpl.PathTypeHint.NONE;
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    InvocationIdContext.setInvocationId();
    incrementStatistic(GhfsStatistic.INVOCATION_GLOB_STATUS);
    checkOpen();

    logger.atFiner().log("globStatus(pathPattern: %s, filter: %s)", pathPattern, filter);
    // URI does not handle glob expressions nicely, for the purpose of
    // fully-qualifying a path we can URI-encode them.
    // Using toString() to avoid Path(URI) constructor.
    Path encodedPath = new Path(pathPattern.toUri().toString());
    // We convert pathPattern to GCS path and then to Hadoop path to ensure that it ends up in
    // the correct format. See note in getHadoopPath for more information.
    Path encodedFixedPath = getHadoopPath(getGcsPath(encodedPath));
    // Decode URI-encoded path back into a glob path.
    Path fixedPath = new Path(URI.create(encodedFixedPath.toString()));
    logger.atFiner().log("fixed path pattern: %s => %s", pathPattern, fixedPath);

    if (globAlgorithm == GlobAlgorithm.CONCURRENT && couldUseFlatGlob(fixedPath)) {
      return concurrentGlobInternal(fixedPath, filter);
    }

    if (globAlgorithm == GlobAlgorithm.FLAT && couldUseFlatGlob(fixedPath)) {
      return flatGlobInternal(fixedPath, filter);
    }

    return super.globStatus(fixedPath, filter);
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    InvocationIdContext.setInvocationId();
    incrementStatistic(GhfsStatistic.INVOCATION_GET_DELEGATION_TOKEN);
    Token<?> result = null;
    if (delegationTokens != null) {
      result = delegationTokens.getBoundOrNewDT(renewer);
    }

    logger.atFiner().log("getDelegationToken(renewer: %s): %s", renewer, result);
    return result;
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst)
      throws IOException {
    InvocationIdContext.setInvocationId();
    incrementStatistic(GhfsStatistic.INVOCATION_COPY_FROM_LOCAL_FILE);
    logger.atFiner().log(
        "copyFromLocalFile(delSrc: %b, overwrite: %b, %d srcs, dst: %s)",
        delSrc, overwrite, srcs.length, dst);
    super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
      throws IOException {
    InvocationIdContext.setInvocationId();
    incrementStatistic(GhfsStatistic.INVOCATION_COPY_FROM_LOCAL_FILE);

    logger.atFiner().log(
        "copyFromLocalFile(delSrc: %b, overwrite: %b, src: %s, dst: %s)",
        delSrc, overwrite, src, dst);
    super.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  @Override
  public FileChecksum getFileChecksum(Path hadoopPath) throws IOException {
    InvocationIdContext.setInvocationId();
    incrementStatistic(GhfsStatistic.INVOCATION_GET_FILE_CHECKSUM);
    checkArgument(hadoopPath != null, "hadoopPath must not be null");

    this.checkOpen();

    URI gcsPath = getGcsPath(hadoopPath);
    FileInfo fileInfo = getGcsFs().getFileInfo(gcsPath);
    if (!fileInfo.exists()) {
      throw new FileNotFoundException(
          String.format(
              "%s not found: %s", fileInfo.isDirectory() ? "Directory" : "File", hadoopPath));
    }
    FileChecksum checksum = getFileChecksum(checksumType, fileInfo);
    logger.atFiner().log(
        "getFileChecksum(hadoopPath: %s [gcsPath: %s]): %s", hadoopPath, gcsPath, checksum);
    return checksum;
  }

  @Override
  public boolean exists(Path f) throws IOException {
    incrementStatistic(GhfsStatistic.INVOCATION_EXISTS);
    return super.exists(f);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
    InvocationIdContext.setInvocationId();
    incrementStatistic(GhfsStatistic.INVOCATION_LIST_LOCATED_STATUS);
    return super.listLocatedStatus(f);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_XATTR_GET_NAMED,
        path,
        traceFactory,
        () -> {
          checkNotNull(path, "path should not be null");
          checkNotNull(name, "name should not be null");

          // track the duration and update the statistics of getXAttr()
          Map<String, byte[]> attributes = getGcsFs().getFileInfo(getGcsPath(path)).getAttributes();
          String xAttrKey = getXAttrKey(name);
          byte[] xAttr =
              attributes.containsKey(xAttrKey) ? getXAttrValue(attributes.get(xAttrKey)) : null;

          logger.atFiner().log(
              "getXAttr(path: %s, name: %s): %s",
              path, name, lazy(() -> xAttr == null ? "<null>" : new String(xAttr, UTF_8)));
          return xAttr;
        });
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_XATTR_GET_MAP,
        path,
        traceFactory,
        () -> {
          checkNotNull(path, "path should not be null");

          FileInfo fileInfo = getGcsFs().getFileInfo(getGcsPath(path));

          Map<String, byte[]> xAttrs =
              fileInfo.getAttributes().entrySet().stream()
                  .filter(a -> isXAttr(a.getKey()))
                  .collect(
                      HashMap::new,
                      (m, a) -> m.put(getXAttrName(a.getKey()), getXAttrValue(a.getValue())),
                      Map::putAll);
          logger.atFiner().log("getXAttrs(path: %s): %s", path, xAttrs);
          return xAttrs;
        });
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_XATTR_GET_NAMED_MAP,
        path,
        traceFactory,
        () -> {
          checkNotNull(path, "path should not be null");
          checkNotNull(names, "names should not be null");

          Map<String, byte[]> xAttrs;
          if (names.isEmpty()) {
            xAttrs = new HashMap<>();
          } else {
            Set<String> namesSet = new HashSet<>(names);

            xAttrs =
                getXAttrs(path).entrySet().stream()
                    .filter(a -> namesSet.contains(a.getKey()))
                    .collect(HashMap::new, (m, a) -> m.put(a.getKey(), a.getValue()), Map::putAll);
          }

          logger.atFiner().log("getXAttrs(path: %s, names: %s): %s", path, names, xAttrs);
          return xAttrs;
        });
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return trackDurationWithTracing(
        instrumentation,
        globalStorageStatistics,
        GhfsStatistic.INVOCATION_OP_XATTR_LIST,
        path,
        traceFactory,
        () -> {
          checkNotNull(path, "path should not be null");

          List<String> xAttrs =
              getGcsFs().getFileInfo(getGcsPath(path)).getAttributes().keySet().stream()
                  .filter(this::isXAttr)
                  .map(this::getXAttrName)
                  .collect(Collectors.toCollection(ArrayList::new));
          logger.atFiner().log("listXAttrs(path: %s): %s", path, xAttrs);
          return xAttrs;
        });
  }

  /**
   * Increment a statistic by 1.
   *
   * @param statistic The operation statistic to increment
   */
  private void incrementStatistic(GhfsStatistic statistic) {
    incrementStatistic(statistic, 1);
  }

  /**
   * Increment a statistic by a specific value.
   *
   * @param statistic The operation statistic to increment
   * @param count the count to increment
   */
  private void incrementStatistic(GhfsStatistic statistic, long count) {
    if (isClosed()) {
      return;
    }
    instrumentation.incrementCounter(statistic, count);
    globalStorageStatistics.incrementCounter(statistic, count);
  }

  /**
   * Get the storage statistics of this filesystem.
   *
   * @return the storage statistics
   */
  public GhfsGlobalStorageStatistics getGlobalGcsStorageStatistics() {
    return globalStorageStatistics;
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

  /**
   * Overridden to make root its own parent. This is POSIX compliant, but more importantly guards
   * against poor directory accounting in the PathData class of Hadoop 2's FsShell.
   */
  @Override
  public Path makeQualified(Path path) {
    Path qualifiedPath = super.makeQualified(path);

    URI uri = qualifiedPath.toUri();

    checkState(
        "".equals(uri.getPath()) || qualifiedPath.isAbsolute(),
        "Path '%s' must be fully qualified.",
        qualifiedPath);

    Path result;
    String upath = uri.getPath();

    // Strip initial '..'s to make root is its own parent.
    int i = 0;
    while (upath.startsWith("/../", i)) {
      // Leave a preceding slash, so path is still absolute.
      i += 3;
    }
    if (i == upath.length() || upath.substring(i).equals("/..")) {
      // Allow a Path of gs://someBucket to map to gs://someBucket/
      result = new Path(uri.getScheme(), uri.getAuthority(), "/");
    } else if (i == 0) {
      result = qualifiedPath;
    } else {
      result = new Path(uri.getScheme(), uri.getAuthority(), upath.substring(i));
    }

    logger.atFiner().log("makeQualified(path: %s): %s", path, result);
    return result;
  }

  /** Returns a URI of the root of this FileSystem. */
  @Override
  public URI getUri() {
    return fsRoot.toUri();
  }

  /** The default port is listed as -1 as an indication that ports are not used. */
  @Override
  protected int getDefaultPort() {
    int result = -1;
    logger.atFiner().log("getDefaultPort(): %d", result);
    return result;
  }

  @Override
  public boolean hasPathCapability(Path path, String capability) {
    checkNotNull(path, "path must not be null");
    checkArgument(
        !isNullOrEmpty(capability), "capability must not be null or empty string for %s", path);
    switch (Ascii.toLowerCase(capability)) {
      case CommonPathCapabilities.FS_APPEND:
      case CommonPathCapabilities.FS_CONCAT:
      case GcsConnectorCapabilities.OPEN_WITH_STATUS:
      case GcsConnectorCapabilities.GET_FILE_STATUS_WITH_HINT:
        return true;
      default:
        return false;
    }
  }

  /**
   * Initiate the open operation. This is invoked from both the FileSystem and FileContext APIs
   *
   * @param hadoopPath path to the file
   * @param parameters open file parameters from the builder.
   * @return a future which will evaluate to the opened file.
   * @throws IOException failure to resolve the link.
   * @throws IllegalArgumentException unknown mandatory key
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  public CompletableFuture<FSDataInputStream> openFileWithOptions(
      Path hadoopPath, OpenFileParameters parameters) throws IOException {
    checkNotNull(hadoopPath, "hadoopPath should not be null");
    checkOpen();
    logger.atFiner().log("Path to be opened: %s, parameters: %s ", hadoopPath, parameters);

    URI gcsPath = getGcsPath(hadoopPath);
    AbstractFSBuilderImpl.rejectUnknownMandatoryKeys(
        parameters.getMandatoryKeys(),
        Options.OpenFileOptions.FS_OPTION_OPENFILE_STANDARD_OPTIONS,
        "for " + gcsPath);

    FileStatus fileStatus = parameters.getStatus();
    FileInfo fileInfo =
        fileStatus instanceof GoogleHadoopFileStatus
            ? ((GoogleHadoopFileStatus) fileStatus).getFileInfo()
            : null;
    if (fileInfo == null) {
      return super.openFileWithOptions(hadoopPath, parameters);
    }

    CompletableFuture<FSDataInputStream> result = new CompletableFuture<>();
    backgroundTasksThreadPool.submit(
        () ->
            LambdaUtils.eval(
                result,
                () ->
                    new FSDataInputStream(
                        GoogleHadoopFSInputStream.create(this, fileInfo, statistics))));
    return result;
  }

  /**
   * Appends to an existing file (optional operation). Not supported.
   *
   * @param hadoopPath The existing file to be appended.
   * @param bufferSize The size of the buffer to be used.
   * @param progress For reporting progress if it is not null.
   * @return A writable stream.
   * @throws IOException if an error occurs.
   */
  @Override
  public FSDataOutputStream append(Path hadoopPath, int bufferSize, Progressable progress)
      throws IOException {
    checkArgument(hadoopPath != null, "hadoopPath must not be null");
    logger.atFiner().log(
        "append(hadoopPath: %s, bufferSize: %d [ignored])", hadoopPath, bufferSize);
    URI filePath = getGcsPath(hadoopPath);
    return new FSDataOutputStream(
        new GoogleHadoopOutputStream(
            this,
            filePath,
            CreateFileOptions.builder()
                .setWriteMode(CreateFileOptions.WriteMode.APPEND)
                .setMinSyncInterval(GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL.getTimeDuration(getConf()))
                .build(),
            statistics),
        statistics);
  }

  /**
   * Concat existing files into one file.
   *
   * @param tgt the path to the target destination.
   * @param srcs the paths to the sources to use for the concatenation.
   * @throws IOException IO failure
   */
  @Override
  public void concat(Path tgt, Path[] srcs) throws IOException {
    logger.atFiner().log("concat(tgt: %s, srcs: %s)", tgt, lazy(() -> Arrays.toString(srcs)));

    checkArgument(srcs.length > 0, "srcs must have at least one source");

    URI tgtPath = getGcsPath(tgt);
    List<URI> srcPaths = Arrays.stream(srcs).map(this::getGcsPath).collect(toImmutableList());

    checkArgument(!srcPaths.contains(tgtPath), "target must not be contained in sources");

    List<List<URI>> partitions =
        Lists.partition(srcPaths, GoogleCloudStorage.MAX_COMPOSE_OBJECTS - 1);
    logger.atFiner().log("concat(tgt: %s, %d partitions: %s)", tgt, partitions.size(), partitions);
    for (List<URI> partition : partitions) {
      // We need to include the target in the list of sources to compose since
      // the GCS FS compose operation will overwrite the target, whereas the Hadoop
      // concat operation appends to the target.
      List<URI> sources = Lists.newArrayList(tgtPath);
      sources.addAll(partition);
      getGcsFs().compose(sources, tgtPath, CreateFileOptions.DEFAULT.getContentType());
    }
  }

  /**
   * Renames src to dst.
   *
   * @param src Source path.
   * @param dst Destination path.
   * @throws IOException if an error occurs.
   */
  void renameInternal(Path src, Path dst) throws IOException {
    checkArgument(src != null, "src must not be null");
    checkArgument(dst != null, "dst must not be null");

    checkOpen();

    URI srcPath = getGcsPath(src);
    URI dstPath = getGcsPath(dst);
    getGcsFs().rename(srcPath, dstPath);

    logger.atFiner().log("rename(src: %s, dst: %s): true", src, dst);
  }

  /**
   * Gets the current working directory.
   *
   * @return The current working directory.
   */
  @Override
  public Path getWorkingDirectory() {
    logger.atFiner().log("getWorkingDirectory(): %s", workingDirectory);
    return workingDirectory;
  }

  /** Gets the default replication factor. */
  @Override
  public short getDefaultReplication() {
    return REPLICATION_FACTOR_DEFAULT;
  }

  /** Returns FileStatus corresponding to the given FileInfo value. */
  private GoogleHadoopFileStatus getGoogleHadoopFileStatus(FileInfo fileInfo, String userName) {
    checkNotNull(fileInfo, "fileInfo should not be null");
    // GCS does not provide modification time. It only provides creation time.
    // It works for objects because they are immutable once created.
    GoogleHadoopFileStatus status =
        new GoogleHadoopFileStatus(
            fileInfo,
            getHadoopPath(fileInfo.getPath()),
            REPLICATION_FACTOR_DEFAULT,
            defaultBlockSize,
            reportedPermissions,
            userName);
    logger.atFiner().log(
        "getGoogleHadoopFileStatus(path: %s, userName: %s): %s",
        fileInfo.getPath(), userName, lazy(() -> fileStatusToString(status)));
    return status;
  }

  /**
   * Determines based on suitability of {@code fixedPath} whether to use flat globbing logic where
   * we use a single large listing during globStatus to then perform the core globbing logic
   * in-memory.
   */
  @VisibleForTesting
  boolean couldUseFlatGlob(Path fixedPath) {
    // Only works for filesystems where the base Hadoop Path scheme matches the underlying URI
    // scheme for GCS.
    if (!getUri().getScheme().equals(SCHEME)) {
      logger.atFine().log(
          "Flat glob is on, but doesn't work for scheme '%s', using default behavior.",
          getUri().getScheme());
      return false;
    }

    // The full pattern should have a wildcard, otherwise there's no point doing the flat glob.
    GlobPattern fullPattern = new GlobPattern(fixedPath.toString());
    if (!fullPattern.hasWildcard()) {
      logger.atFine().log(
          "Flat glob is on, but Path '%s' has no wildcard, using default behavior.", fixedPath);
      return false;
    }

    // To use a flat glob, there must be an authority defined.
    if (isNullOrEmpty(fixedPath.toUri().getAuthority())) {
      logger.atFine().log(
          "Flat glob is on, but Path '%s' has a empty authority, using default behavior.",
          fixedPath);
      return false;
    }

    // And the authority must not contain a wildcard.
    GlobPattern authorityPattern = new GlobPattern(fixedPath.toUri().getAuthority());
    if (authorityPattern.hasWildcard()) {
      logger.atFine().log(
          "Flat glob is on, but Path '%s' has a wildcard authority, using default behavior.",
          fixedPath);
      return false;
    }

    return true;
  }

  @VisibleForTesting
  String trimToPrefixWithoutGlob(String path) {
    char[] wildcardChars = "*?{[".toCharArray();
    int trimIndex = path.length();

    // Find the first occurrence of any one of the wildcard characters, or just path.length()
    // if none are found.
    for (char wildcard : wildcardChars) {
      int wildcardIndex = path.indexOf(wildcard);
      if (wildcardIndex >= 0 && wildcardIndex < trimIndex) {
        trimIndex = wildcardIndex;
      }
    }
    return path.substring(0, trimIndex);
  }

  /**
   * Returns an array of FileStatus objects whose path names match pathPattern.
   *
   * <p>Return null if pathPattern has no glob and the path does not exist. Return an empty array if
   * pathPattern has a glob and no path matches it.
   *
   * @param pathPattern A regular expression specifying the path pattern.
   * @return An array of FileStatus objects.
   * @throws IOException if an error occurs.
   */
  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return globStatus(pathPattern, DEFAULT_FILTER);
  }

  /**
   * Use 2 glob algorithms that return the same result but one of them could be significantly faster
   * than another one depending on directory layout.
   */
  private FileStatus[] concurrentGlobInternal(Path fixedPath, PathFilter filter)
      throws IOException {
    try {
      return backgroundTasksThreadPool.invokeAny(
          ImmutableList.of(
              () -> flatGlobInternal(fixedPath, filter),
              () -> super.globStatus(fixedPath, filter)));
    } catch (InterruptedException e) {
      GoogleCloudStorageEventBus.postOnException();
      Thread.currentThread().interrupt();
      throw new IOException(String.format("Concurrent glob execution failed: %s", e), e);
    } catch (ExecutionException e) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(String.format("Concurrent glob execution failed: %s", e.getCause()), e);
    }
  }

  private FileStatus[] flatGlobInternal(Path fixedPath, PathFilter filter) throws IOException {
    String pathString = fixedPath.toString();
    String prefixString = trimToPrefixWithoutGlob(pathString);
    Path prefixPath = new Path(prefixString);
    URI prefixUri = getGcsPath(prefixPath);

    if (prefixString.endsWith("/") && !prefixPath.toString().endsWith("/")) {
      // Path strips a trailing slash unless it's the 'root' path. We want to keep the trailing
      // slash so that we don't wastefully list sibling files which may match the directory-name
      // as a strict prefix but would've been omitted due to not containing the '/' at the end.
      prefixUri = UriPaths.toDirectory(prefixUri);
    }

    // Get everything matching the non-glob prefix.
    logger.atFiner().log("Listing everything with '%s' prefix", prefixUri);
    List<FileStatus> matchedStatuses = null;
    String pageToken = null;
    do {
      GoogleCloudStorage.ListPage<FileInfo> infoPage =
          getGcsFs().listFileInfoForPrefixPage(prefixUri, LIST_OPTIONS, pageToken);

      Collection<FileStatus> statusPage =
          toFileStatusesWithImplicitDirectories(infoPage.getItems());

      // TODO: refactor to use GlobPattern and PathFilter directly without helper FS
      FileSystem helperFileSystem =
          InMemoryGlobberFileSystem.createInstance(getConf(), getWorkingDirectory(), statusPage);
      FileStatus[] matchedStatusPage = helperFileSystem.globStatus(fixedPath, filter);
      if (matchedStatusPage != null) {
        Collections.addAll(
            (matchedStatuses == null ? matchedStatuses = new ArrayList<>() : matchedStatuses),
            matchedStatusPage);
      }

      pageToken = infoPage.getNextPageToken();
    } while (pageToken != null);

    if (matchedStatuses == null || matchedStatuses.isEmpty()) {
      return matchedStatuses == null ? null : new FileStatus[0];
    }

    matchedStatuses.sort(
        Comparator.<FileStatus>naturalOrder()
            // Place duplicate implicit directories after real directory
            .thenComparingInt((FileStatus f) -> isImplicitDirectory(f) ? 1 : 0));

    // Remove duplicate file statuses that could be in the matchedStatuses
    // because of pagination and implicit directories
    List<FileStatus> filteredStatuses = new ArrayList<>(matchedStatuses.size());
    FileStatus lastAdded = null;
    for (FileStatus fileStatus : matchedStatuses) {
      if (lastAdded == null || lastAdded.compareTo(fileStatus) != 0) {
        filteredStatuses.add(fileStatus);
        lastAdded = fileStatus;
      }
    }

    return filteredStatuses.toArray(new FileStatus[0]);
  }

  /** Helper method that converts {@link FileInfo} collection to {@link FileStatus} collection. */
  private Collection<FileStatus> toFileStatusesWithImplicitDirectories(
      Collection<FileInfo> fileInfos) throws IOException {
    List<FileStatus> fileStatuses = new ArrayList<>(fileInfos.size());
    Set<URI> filePaths = Sets.newHashSetWithExpectedSize(fileInfos.size());
    String userName = getUgiUserName();
    for (FileInfo fileInfo : fileInfos) {
      filePaths.add(fileInfo.getPath());
      fileStatuses.add(getGoogleHadoopFileStatus(fileInfo, userName));
    }

    // The flow for populating this doesn't bother to populate metadata entries for parent
    // directories, but we know the parent directories are expected to exist, so we'll just
    // populate the missing entries explicitly here. Necessary for getFileStatus(parentOfInfo)
    // to work when using an instance of this class.
    for (FileInfo fileInfo : fileInfos) {
      URI parentPath = UriPaths.getParentPath(fileInfo.getPath());
      while (parentPath != null && !parentPath.equals(GoogleCloudStorageFileSystem.GCS_ROOT)) {
        if (!filePaths.contains(parentPath)) {
          logger.atFiner().log("Adding fake entry for missing parent path '%s'", parentPath);
          StorageResourceId id = StorageResourceId.fromUriPath(parentPath, true);

          GoogleCloudStorageItemInfo fakeItemInfo =
              GoogleCloudStorageItemInfo.createInferredDirectory(id);
          FileInfo fakeFileInfo = FileInfo.fromItemInfo(fakeItemInfo);

          filePaths.add(parentPath);
          fileStatuses.add(getGoogleHadoopFileStatus(fakeFileInfo, userName));
        }
        parentPath = UriPaths.getParentPath(parentPath);
      }
    }

    return fileStatuses;
  }

  /**
   * Returns home directory of the current user.
   *
   * <p>Note: This directory is only used for Hadoop purposes. It is not the same as a user's OS
   * home directory.
   */
  @Override
  public Path getHomeDirectory() {
    Path result = new Path(fsRoot, "user/" + System.getProperty("user.name"));
    logger.atFiner().log("getHomeDirectory(): %s", result);
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns the service if delegation tokens are configured, otherwise, null.
   */
  @Override
  public String getCanonicalServiceName() {
    String result = delegationTokens == null ? null : delegationTokens.getService().toString();
    logger.atFiner().log("getCanonicalServiceName(): %s", result);
    return result;
  }

  /** Gets GCS FS instance. */
  public GoogleCloudStorageFileSystem getGcsFs() {
    return gcsFsSupplier.get();
  }

  public Supplier<VectoredIOImpl> getVectoredIOSupplier() {
    return vectoredIOSupplier;
  }

  /**
   * Loads an {@link AccessTokenProvider} implementation retrieved from the provided {@code
   * AbstractDelegationTokenBinding} if configured, otherwise it's null.
   */
  private AccessTokenProvider getDelegationAccessTokenProvider(Configuration config) {
    AccessTokenProvider accessTokenProvider =
        delegationTokens == null ? null : delegationTokens.getAccessTokenProvider();
    if (accessTokenProvider != null) {
      accessTokenProvider.setConf(config);
    }
    return accessTokenProvider;
  }

  /** Assert that the FileSystem has been initialized and not close()d. */
  private void checkOpen() throws IOException {
    if (isClosed()) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException("GoogleHadoopFileSystem has been closed or not initialized.");
    }
  }

  boolean isClosed() {
    return gcsFsSupplier == null || gcsFsSupplier.get() == null;
  }

  @Override
  public boolean deleteOnExit(Path f) throws IOException {
    checkOpen();
    boolean result = super.deleteOnExit(f);
    logger.atFiner().log("deleteOnExit(path: %s): %b", f, result);
    return result;
  }

  @Override
  protected void processDeleteOnExit() {
    logger.atFiner().log("processDeleteOnExit()");
    super.processDeleteOnExit();
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    ContentSummary result = super.getContentSummary(f);
    logger.atFiner().log("getContentSummary(path: %s): %b", f, result);
    return result;
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    logger.atFiner().log("copyToLocalFile(delSrc: %b, src: %s, dst: %s)", delSrc, src, dst);
    super.copyToLocalFile(delSrc, src, dst);
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    Path result = super.startLocalOutput(fsOutputFile, tmpLocalFile);
    logger.atFiner().log(
        "startLocalOutput(fsOutputFile: %s, tmpLocalFile: %s): %s",
        fsOutputFile, tmpLocalFile, result);
    return result;
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    logger.atFiner().log(
        "startLocalOutput(fsOutputFile: %s, tmpLocalFile: %s)", fsOutputFile, tmpLocalFile);
    super.completeLocalOutput(fsOutputFile, tmpLocalFile);
  }

  @Override
  public void close() throws IOException {
    logger.atFiner().log("close()");
    super.close();

    // NB: We must *first* have the superclass close() before we close the underlying gcsFsSupplier
    // since the superclass may decide to perform various heavyweight cleanup operations (such as
    // deleteOnExit).
    if (gcsFsSupplier != null) {
      if (gcsFsInitialized) {
        getGcsFs().close();
      }
      gcsFsSupplier = null;
    }

    if (delegationTokens != null) {
      try {
        delegationTokens.close();
      } catch (IOException e) {
        logger.atSevere().withCause(e).log("Failed to stop delegation tokens support");
      }
    }

    if (vectoredIOSupplier != null) {
      if (vectoredIOInitialized) {
        try {
          vectoredIOSupplier.get().close();
        } catch (Exception e) {
          logger.atWarning().withCause(e).log(
              "Failed to close the underneath vectoredIO implementation");
        } finally {
          vectoredIOSupplier = null;
          vectoredIOInitialized = false;
        }
      }
    }

    if (loggingInterceptor != null) {
      try {
        Logger.getLogger("").removeHandler(loggingInterceptor);
        loggingInterceptor.close();
      } catch (RuntimeException e) {
        logger.atSevere().withCause(e).log("Failed to stop cloud logging service");
      } finally {
        loggingInterceptor = null;
      }
    }

    if (instrumentation != null) {
      instrumentation.close();
      instrumentation = null;
    }

    if (backgroundTasksThreadPool != null) {
      backgroundTasksThreadPool.shutdown();
      backgroundTasksThreadPool = null;
    }
  }

  @Override
  public long getUsed() throws IOException {
    long result = super.getUsed();
    logger.atFiner().log("getUsed(): %s", result);
    return result;
  }

  @Override
  public long getDefaultBlockSize() {
    logger.atFiner().log("getDefaultBlockSize(): %d", defaultBlockSize);
    return defaultBlockSize;
  }

  @Override
  public void setWorkingDirectory(Path hadoopPath) {
    checkArgument(hadoopPath != null, "hadoopPath must not be null");
    URI gcsPath = UriPaths.toDirectory(getGcsPath(hadoopPath));
    workingDirectory = getHadoopPath(gcsPath);
    logger.atFiner().log("setWorkingDirectory(hadoopPath: %s): %s", hadoopPath, workingDirectory);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    logger.atFiner().log("setVerifyChecksum(verifyChecksum: %s)", verifyChecksum);
    super.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    logger.atFiner().log("setPermission(path: %s, permission: %s)", p, permission);
    super.setPermission(p, permission);
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    logger.atFiner().log("setOwner(path: %s, username: %s, groupname: %s)", p, username, groupname);
    super.setOwner(p, username, groupname);
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    logger.atFiner().log("setTimes(path: %s, mtime: %d, atime: %d)", p, mtime, atime);
    super.setTimes(p, mtime, atime);
  }

  /** {@inheritDoc} */
  @Override
  public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flags)
      throws IOException {
    logger.atFiner().log(
        "setXAttr(path: %s, name: %s, value %s, flags %s",
        path, name, lazy(() -> new String(value, UTF_8)), flags);
    checkNotNull(path, "path should not be null");
    checkNotNull(name, "name should not be null");
    checkArgument(flags != null && !flags.isEmpty(), "flags should not be null or empty");

    FileInfo fileInfo = getGcsFs().getFileInfo(getGcsPath(path));
    String xAttrKey = getXAttrKey(name);
    Map<String, byte[]> attributes = fileInfo.getAttributes();

    if (attributes.containsKey(xAttrKey) && !flags.contains(XAttrSetFlag.REPLACE)) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(
          String.format(
              "REPLACE flag must be set to update XAttr (name='%s', value='%s') for '%s'",
              name, new String(value, UTF_8), path));
    }
    if (!attributes.containsKey(xAttrKey) && !flags.contains(XAttrSetFlag.CREATE)) {
      GoogleCloudStorageEventBus.postOnException();
      throw new IOException(
          String.format(
              "CREATE flag must be set to create XAttr (name='%s', value='%s') for '%s'",
              name, new String(value, UTF_8), path));
    }

    UpdatableItemInfo updateInfo =
        new UpdatableItemInfo(
            StorageResourceId.fromUriPath(fileInfo.getPath(), /* allowEmptyObjectName= */ false),
            ImmutableMap.of(xAttrKey, getXAttrValue(value)));
    getGcsFs().getGcs().updateItems(ImmutableList.of(updateInfo));
  }

  /** {@inheritDoc} */
  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    logger.atFiner().log("removeXAttr(path: %s, name: %s)", path, name);
    checkNotNull(path, "path should not be null");
    checkNotNull(name, "name should not be null");

    FileInfo fileInfo = getGcsFs().getFileInfo(getGcsPath(path));
    Map<String, byte[]> xAttrToRemove = new HashMap<>();
    xAttrToRemove.put(getXAttrKey(name), null);
    UpdatableItemInfo updateInfo =
        new UpdatableItemInfo(
            StorageResourceId.fromUriPath(fileInfo.getPath(), /* allowEmptyObjectName= */ false),
            xAttrToRemove);
    getGcsFs().getGcs().updateItems(ImmutableList.of(updateInfo));
  }

  private boolean isXAttr(String key) {
    return key != null && key.startsWith(XATTR_KEY_PREFIX);
  }

  private String getXAttrKey(String name) {
    return XATTR_KEY_PREFIX + name;
  }

  private String getXAttrName(String key) {
    return key.substring(XATTR_KEY_PREFIX.length());
  }

  private byte[] getXAttrValue(byte[] value) {
    return value == null ? XATTR_NULL_VALUE : value;
  }

  /**
   * Available GCS checksum types for use with {@link
   * GoogleHadoopFileSystemConfiguration#GCS_FILE_CHECKSUM_TYPE}.
   */
  public enum GcsFileChecksumType {
    NONE(null, 0),
    CRC32C("COMPOSITE-CRC32C", 4),
    MD5("MD5", 16);

    private final String algorithmName;
    private final int byteLength;

    GcsFileChecksumType(String algorithmName, int byteLength) {
      this.algorithmName = algorithmName;
      this.byteLength = byteLength;
    }

    public String getAlgorithmName() {
      return algorithmName;
    }

    public int getByteLength() {
      return byteLength;
    }
  }

  /**
   * Available GCS glob algorithms for use with {@link
   * GoogleHadoopFileSystemConfiguration#GCS_GLOB_ALGORITHM}.
   */
  public enum GlobAlgorithm {
    CONCURRENT,
    DEFAULT,
    FLAT
  }

  /**
   * GCS {@link FileChecksum} which takes constructor parameters to define the return values of the
   * various abstract methods of {@link FileChecksum}.
   */
  private static class GcsFileChecksum extends FileChecksum {

    private final GcsFileChecksumType checksumType;
    private final byte[] bytes;

    public GcsFileChecksum(GcsFileChecksumType checksumType, byte[] bytes) {
      this.checksumType = checksumType;
      this.bytes = bytes;
      checkState(
          bytes == null || bytes.length == checksumType.getByteLength(),
          "Checksum value length (%s) should be equal to the algorithm byte length (%s)",
          checksumType.getByteLength(),
          bytes == null ? null : bytes.length);
    }

    @Override
    public String getAlgorithmName() {
      return checksumType.getAlgorithmName();
    }

    @Override
    public int getLength() {
      return checksumType.getByteLength();
    }

    @Override
    public byte[] getBytes() {
      return bytes;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      in.readFully(bytes);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.write(bytes);
    }

    @Override
    public String toString() {
      return String.format(
          "%s: %s", getAlgorithmName(), bytes == null ? null : BaseEncoding.base16().encode(bytes));
    }
  }

  private class GcsConnectorCapabilities {
    public static final String OPEN_WITH_STATUS = "fs.gs.capability.open.with.status";
    public static final String GET_FILE_STATUS_WITH_HINT =
        "fs.gs.capability.getfilestatus.with.hint";
  }
}
