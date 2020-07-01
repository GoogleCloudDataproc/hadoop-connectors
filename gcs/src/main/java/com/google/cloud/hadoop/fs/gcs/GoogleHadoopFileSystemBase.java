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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.OutputStreamType.FLUSHABLE_COMPOSITE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.BLOCK_SIZE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.CONFIG_KEY_PREFIXES;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONCURRENT_GLOB_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_OVERRIDE_FILE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_FILE_CHECKSUM_TYPE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_FLAT_GLOB_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL_MS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_TYPE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_WORKING_DIRECTORY;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.PERMISSIONS_TO_REPORT;
import static com.google.cloud.hadoop.gcsio.CreateFileOptions.DEFAULT_NO_OVERWRITE;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.flogger.LazyArgs.lazy;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.cloud.hadoop.fs.gcs.auth.GcsDelegationTokens;
import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage.ListPage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.UpdatableItemInfo;
import com.google.cloud.hadoop.gcsio.UriPaths;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.util.CredentialFactory.CredentialHttpRetryInitializer;
import com.google.cloud.hadoop.util.CredentialFromAccessTokenProviderClassFactory;
import com.google.cloud.hadoop.util.GoogleCredentialWithIamAccessToken;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.util.PropertyUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.flogger.GoogleLogger;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * This class provides a Hadoop compatible File System on top of Google Cloud Storage (GCS).
 *
 * <p>It is implemented as a thin abstraction layer on top of GCS. The layer hides any specific
 * characteristics of the underlying store and exposes FileSystem interface understood by the Hadoop
 * engine.
 *
 * <p>Users interact with the files in the storage using fully qualified URIs. The file system
 * exposed by this class is identified using the 'gs' scheme. For example, {@code
 * gs://dir1/dir2/file1.txt}.
 *
 * <p>This implementation translates paths between hadoop Path and GCS URI with the convention that
 * the Hadoop root directly corresponds to the GCS "root", e.g. gs:/. This is convenient for many
 * reasons, such as data portability and close equivalence to gsutil paths, but imposes certain
 * inherited constraints, such as files not being allowed in root (only 'directories' can be placed
 * in root), and directory names inside root have a more limited set of allowed characters.
 *
 * <p>One of the main goals of this implementation is to maintain compatibility with behavior of
 * HDFS implementation when accessed through FileSystem interface. HDFS implementation is not very
 * consistent about the cases when it throws versus the cases when methods return false. We run GHFS
 * tests and HDFS tests against the same test data and use that as a guide to decide whether to
 * throw or to return false.
 */
public abstract class GoogleHadoopFileSystemBase extends FileSystem
    implements FileSystemDescriptor {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * Available types for use with {@link
   * GoogleHadoopFileSystemConfiguration#GCS_OUTPUT_STREAM_TYPE}.
   */
  public enum OutputStreamType {
    BASIC,
    FLUSHABLE_COMPOSITE,
    SYNCABLE_COMPOSITE
  }

  /**
   * Available GCS checksum types for use with {@link
   * GoogleHadoopFileSystemConfiguration#GCS_FILE_CHECKSUM_TYPE}.
   */
  public static enum GcsFileChecksumType {
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

  /** Default value of replication factor. */
  public static final short REPLICATION_FACTOR_DEFAULT = 3;

  /** Default PathFilter that accepts all paths. */
  public static final PathFilter DEFAULT_FILTER = path -> true;

  /** A resource file containing GCS related build properties. */
  public static final String PROPERTIES_FILE = "gcs.properties";

  /** The key in the PROPERTIES_FILE that contains the version built. */
  public static final String VERSION_PROPERTY = "gcs.connector.version";

  /** The version returned when one cannot be found in properties. */
  public static final String UNKNOWN_VERSION = "0.0.0";

  /** Current version. */
  public static final String VERSION;

  /** Identifies this version of the GoogleHadoopFileSystemBase library. */
  public static final String GHFS_ID;

  static {
    VERSION =
        PropertyUtil.getPropertyOrDefault(
            GoogleHadoopFileSystemBase.class, PROPERTIES_FILE, VERSION_PROPERTY, UNKNOWN_VERSION);
    logger.atFine().log("GHFS version: %s", VERSION);
    GHFS_ID = String.format("GHFS/%s", VERSION);
  }

  private static final String XATTR_KEY_PREFIX = "GHFS_XATTR_";

  // Use empty array as null value because GCS API already uses null value to remove metadata key
  private static final byte[] XATTR_NULL_VALUE = new byte[0];

  private static final ThreadFactory DAEMON_THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("ghfs-thread-%d").setDaemon(true).build();

  @VisibleForTesting
  boolean enableFlatGlob = GCS_FLAT_GLOB_ENABLE.getDefault();

  @VisibleForTesting
  boolean enableConcurrentGlob = GCS_CONCURRENT_GLOB_ENABLE.getDefault();

  private GcsFileChecksumType checksumType = GCS_FILE_CHECKSUM_TYPE.getDefault();

  /** The URI the File System is passed in initialize. */
  protected URI initUri;

  /** Delegation token support */
  protected GcsDelegationTokens delegationTokens = null;

  /** Underlying GCS file system object. */
  private Supplier<GoogleCloudStorageFileSystem> gcsFsSupplier;

  private boolean gcsFsInitialized = false;

  /**
   * Current working directory; overridden in initialize() if {@link
   * GoogleHadoopFileSystemConfiguration#GCS_WORKING_DIRECTORY} is set.
   */
  private Path workingDirectory;

  /**
   * Default block size. Note that this is the size that is reported to Hadoop FS clients. It does
   * not modify the actual block size of an underlying GCS object, because GCS JSON API does not
   * allow modifying or querying the value. Modifying this value allows one to control how many
   * mappers are used to process a given file.
   */
  protected long defaultBlockSize = BLOCK_SIZE.getDefault();

  /** The fixed reported permission of all files. */
  private FsPermission reportedPermissions;

  /** Map of counter values */
  protected final ImmutableMap<Counter, AtomicLong> counters = createCounterMap();

  protected ImmutableMap<Counter, AtomicLong> createCounterMap() {
    EnumMap<Counter, AtomicLong> countersMap = new EnumMap<>(Counter.class);
    for (Counter counter : ALL_COUNTERS) {
      countersMap.put(counter, new AtomicLong());
    }
    return Maps.immutableEnumMap(countersMap);
  }

  /**
   * Defines names of counters we track for each operation.
   *
   * There are two types of counters:
   * -- METHOD_NAME      : Number of successful invocations of method METHOD.
   * -- METHOD_NAME_TIME : Total inclusive time spent in method METHOD.
   */
  public enum Counter {
    APPEND,
    APPEND_TIME,
    CREATE,
    CREATE_TIME,
    DELETE,
    DELETE_TIME,
    GET_FILE_CHECKSUM,
    GET_FILE_CHECKSUM_TIME,
    GET_FILE_STATUS,
    GET_FILE_STATUS_TIME,
    INIT,
    INIT_TIME,
    INPUT_STREAM,
    INPUT_STREAM_TIME,
    LIST_STATUS,
    LIST_STATUS_TIME,
    MKDIRS,
    MKDIRS_TIME,
    OPEN,
    OPEN_TIME,
    OUTPUT_STREAM,
    OUTPUT_STREAM_TIME,
    READ1,
    READ1_TIME,
    READ,
    READ_TIME,
    READ_FROM_CHANNEL,
    READ_FROM_CHANNEL_TIME,
    READ_CLOSE,
    READ_CLOSE_TIME,
    READ_POS,
    READ_POS_TIME,
    RENAME,
    RENAME_TIME,
    SEEK,
    SEEK_TIME,
    SET_WD,
    SET_WD_TIME,
    WRITE1,
    WRITE1_TIME,
    WRITE,
    WRITE_TIME,
    WRITE_CLOSE,
    WRITE_CLOSE_TIME,
  }

  /**
   * Set of all counters.
   *
   * <p>It is used for performance optimization instead of `Counter.values`, because
   * `Counter.values` returns new array on each invocation.
   */
  private static final ImmutableSet<Counter> ALL_COUNTERS =
      Sets.immutableEnumSet(EnumSet.allOf(Counter.class));

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
          checksumType.getByteLength(), bytes.length);
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

  /**
   * Constructs an instance of GoogleHadoopFileSystemBase; the internal {@link
   * GoogleCloudStorageFileSystem} will be set up with config settings when initialize() is called.
   */
  public GoogleHadoopFileSystemBase() {}

  /**
   * Constructs an instance of {@link GoogleHadoopFileSystemBase} using the provided
   * GoogleCloudStorageFileSystem; initialize() will not re-initialize it.
   */
  // TODO(b/120887495): This @VisibleForTesting annotation was being ignored by prod code.
  // Please check that removing it is correct, and remove this comment along with it.
  // @VisibleForTesting
  GoogleHadoopFileSystemBase(GoogleCloudStorageFileSystem gcsFs) {
    checkNotNull(gcsFs, "gcsFs must not be null");
    setGcsFs(gcsFs);
  }

  private void setGcsFs(GoogleCloudStorageFileSystem gcsFs) {
    this.gcsFsSupplier = Suppliers.ofInstance(gcsFs);
    this.gcsFsInitialized = true;
  }

  /**
   * Returns an unqualified path without any leading slash, relative to the filesystem root,
   * which serves as the home directory of the current user; see {@code getHomeDirectory} for
   * a description of what the home directory means.
   */
  protected abstract String getHomeDirectorySubpath();

  /**
   * Gets Hadoop path corresponding to the given GCS path.
   *
   * @param gcsPath Fully-qualified GCS path, of the form gs://bucket/object-path.
   */
  public abstract Path getHadoopPath(URI gcsPath);

  /**
   * Gets GCS path corresponding to the given Hadoop path, which can be relative or absolute, and
   * can have either {@code gs://<path>} or {@code gs:/<path>} forms.
   *
   * @param hadoopPath Hadoop path.
   */
  public abstract URI getGcsPath(Path hadoopPath);

  /**
   * Gets the default value of working directory.
   */
  public abstract Path getDefaultWorkingDirectory();

  // =================================================================
  // Methods implementing FileSystemDescriptor interface; these define the way
  // paths are translated between Hadoop and GCS.
  // =================================================================

  @Override
  public abstract Path getFileSystemRoot();

  @Override
  public abstract String getScheme();

  /**
   * Overridden to make root its own parent. This is POSIX compliant, but more importantly guards
   * against poor directory accounting in the PathData class of Hadoop 2's FsShell.
   */
  @Override
  public Path makeQualified(final Path path) {
    Path qualifiedPath = super.makeQualified(path);

    URI uri = qualifiedPath.toUri();

    checkState(
        "".equals(uri.getPath()) || qualifiedPath.isAbsolute(),
        "Path '%s' must be fully qualified.",
        qualifiedPath);

    // Strip initial '..'s to make root is its own parent.
    StringBuilder sb = new StringBuilder(uri.getPath());
    while (sb.indexOf("/../") == 0) {
      // Leave a preceding slash, so path is still absolute.
      sb.delete(0, 3);
    }

    String strippedPath = sb.toString();

    // Allow a Path of gs://someBucket to map to gs://someBucket/
    if (strippedPath.equals("/..") || strippedPath.equals("")) {
      strippedPath = "/";
    }

    Path result = new Path(uri.getScheme(), uri.getAuthority(), strippedPath);
    logger.atFinest().log("makeQualified(path: %s): %s", path, result);
    return result;
  }

  @Override
  protected void checkPath(Path path) {
    URI uri = path.toUri();
    String scheme = uri.getScheme();
    // Only check that the scheme matches. The authority and path will be
    // validated later.
    if (scheme == null || scheme.equalsIgnoreCase(getScheme())) {
      return;
    }
    String msg = String.format(
        "Wrong FS scheme: %s, in path: %s, expected scheme: %s",
        scheme, path, getScheme());
    throw new IllegalArgumentException(msg);
  }

  /**
   * See {@link #initialize(URI, Configuration, boolean)} for details; calls with third arg
   * defaulting to 'true' for initializing the superclass.
   *
   * @param path URI of a file/directory within this file system.
   * @param config Hadoop configuration.
   */
  @Override
  public void initialize(URI path, Configuration config) throws IOException {
    initialize(path, config, /* initSuperclass= */ true);
  }

  /**
   * Initializes this file system instance.
   *
   * Note:
   * The path passed to this method could be path of any file/directory.
   * It does not matter because the only thing we check is whether
   * it uses 'gs' scheme. The rest is ignored.
   *
   * @param path URI of a file/directory within this file system.
   * @param config Hadoop configuration.
   * @param initSuperclass if false, doesn't call super.initialize(path, config); avoids
   *     registering a global Statistics object for this instance.
   */
  public void initialize(URI path, Configuration config, boolean initSuperclass)
      throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkArgument(path != null, "path must not be null");
    Preconditions.checkArgument(config != null, "config must not be null");
    Preconditions.checkArgument(path.getScheme() != null, "scheme of path must not be null");
    if (!path.getScheme().equals(getScheme())) {
      throw new IllegalArgumentException("URI scheme not supported: " + path);
    }
    initUri = path;
    logger.atFine().log(
        "initialize(path: %s, config: %s, initSuperclass: %b)", path, config, initSuperclass);

    if (initSuperclass) {
      super.initialize(path, config);
    } else {
      logger.atFiner().log(
          "Initializing 'statistics' as an instance not attached to the static FileSystem map");
      // Provide an ephemeral Statistics object to avoid NPE, but still avoid registering a global
      // statistics object.
      statistics = new Statistics(getScheme());
    }

    // Set this configuration as the default config for this instance; configure()
    // will perform some file-system-specific adjustments, but the original should
    // be sufficient (and is required) for the delegation token binding initialization.
    setConf(config);

    // Initialize the delegation token support, if it is configured
    initializeDelegationTokenSupport(config, path);

    configure(config);

    long duration = System.nanoTime() - startTime;
    increment(Counter.INIT);
    increment(Counter.INIT_TIME, duration);
  }

  /**
   * Initialize the delegation token support for this filesystem.
   *
   * @param config The filesystem configuration
   * @param path The filesystem path
   * @throws IOException
   */
  private void initializeDelegationTokenSupport(Configuration config, URI path) throws IOException {
    logger.atFine().log("initializeDelegationTokenSupport(config: %s, path: %s)", config, path);
    // Load delegation token binding, if support is configured
    GcsDelegationTokens dts = new GcsDelegationTokens();
    Text service = new Text(getScheme() + "://" + path.getAuthority());
    dts.bindToFileSystem(this, service);
    try {
      dts.init(config);
      delegationTokens = dts;
      if (delegationTokens.isBoundToDT()) {
        logger.atFine().log(
            "initializeDelegationTokenSupport(config: %s, path: %s):"
                + " using existing delegation token",
            config, path);
      }
    } catch (IllegalStateException e) {
      logger.atFiner().withCause(e).log("Failed to initialize delegation token support");
    }
  }

  /**
   * Returns a URI of the root of this FileSystem.
   */
  @Override
  public URI getUri() {
    return getFileSystemRoot().toUri();
  }

  /**
   * The default port is listed as -1 as an indication that ports are not used.
   */
  @Override
  protected int getDefaultPort() {
    int result = -1;
    logger.atFinest().log("getDefaultPort(): %d", result);
    return result;
  }

  /**
   * Opens the given file for reading.
   *
   * <p>Note: This function overrides the given bufferSize value with a higher number unless further
   * overridden using configuration parameter {@code fs.gs.inputstream.buffer.size}.
   *
   * @param hadoopPath File to open.
   * @param bufferSize Size of buffer to use for IO.
   * @return A readable stream.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if an error occurs.
   */
  @Override
  public FSDataInputStream open(Path hadoopPath, int bufferSize) throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    checkOpen();

    logger.atFiner().log("open(hadoopPath: %s, bufferSize: %d [ignored])", hadoopPath, bufferSize);
    URI gcsPath = getGcsPath(hadoopPath);
    GoogleCloudStorageReadOptions readChannelOptions =
        getGcsFs().getOptions().getCloudStorageOptions().getReadChannelOptions();
    GoogleHadoopFSInputStream in =
        new GoogleHadoopFSInputStream(this, gcsPath, readChannelOptions, statistics);

    long duration = System.nanoTime() - startTime;
    increment(Counter.OPEN);
    increment(Counter.OPEN_TIME, duration);
    return new FSDataInputStream(in);
  }

  /**
   * Opens the given file for writing.
   *
   * <p>Note: This function overrides the given bufferSize value with a higher number unless further
   * overridden using configuration parameter {@code fs.gs.outputstream.buffer.size}.
   *
   * @param hadoopPath The file to open.
   * @param permission Permissions to set on the new file. Ignored.
   * @param overwrite If a file with this name already exists, then if true, the file will be
   *     overwritten, and if false an error will be thrown.
   * @param bufferSize The size of the buffer to use.
   * @param replication Required block replication for the file. Ignored.
   * @param blockSize The block-size to be used for the new file. Ignored.
   * @param progress Progress is reported through this. Ignored.
   * @return A writable stream.
   * @throws IOException if an error occurs.
   * @see #setPermission(Path, FsPermission)
   */
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

    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");
    Preconditions.checkArgument(
        replication > 0, "replication must be a positive integer: %s", replication);
    Preconditions.checkArgument(
        blockSize > 0, "blockSize must be a positive integer: %s", blockSize);

    checkOpen();

    logger.atFiner().log(
        "create(hadoopPath: %s, overwrite: %b, bufferSize: %d [ignored])",
        hadoopPath, overwrite, bufferSize);

    URI gcsPath = getGcsPath(hadoopPath);

    OutputStreamType type = GCS_OUTPUT_STREAM_TYPE.get(getConf(), getConf()::getEnum);
    OutputStream out;
    switch (type) {
      case BASIC:
        out =
            new GoogleHadoopOutputStream(
                this, gcsPath, statistics, new CreateFileOptions(overwrite));
        break;
      case FLUSHABLE_COMPOSITE:
        SyncableOutputStreamOptions flushableOutputStreamOptions =
            SyncableOutputStreamOptions.builder()
                .setMinSyncInterval(
                    Duration.ofMillis(
                        GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL_MS.get(getConf(), getConf()::getInt)))
                .setSyncOnFlushEnabled(true)
                .build();
        out =
            new GoogleHadoopSyncableOutputStream(
                this,
                gcsPath,
                statistics,
                new CreateFileOptions(overwrite),
                flushableOutputStreamOptions);
        break;
      case SYNCABLE_COMPOSITE:
        SyncableOutputStreamOptions syncableOutputStreamOptions =
            SyncableOutputStreamOptions.builder()
                .setMinSyncInterval(
                    Duration.ofMillis(
                        GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL_MS.get(getConf(), getConf()::getInt)))
                .build();
        out =
            new GoogleHadoopSyncableOutputStream(
                this,
                gcsPath,
                statistics,
                new CreateFileOptions(overwrite),
                syncableOutputStreamOptions);
        break;
      default:
        throw new IOException(
            String.format(
                "Unsupported output stream type given for key '%s': '%s'",
                GCS_OUTPUT_STREAM_TYPE.getKey(), type));
    }

    long duration = System.nanoTime() - startTime;
    increment(Counter.CREATE);
    increment(Counter.CREATE_TIME, duration);
    return new FSDataOutputStream(out, null);
  }

  /** {@inheritDoc} */
  @Override
  public FSDataOutputStream createNonRecursive(
      Path hadoopPath,
      FsPermission permission,
      EnumSet<org.apache.hadoop.fs.CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    URI gcsPath = getGcsPath(checkNotNull(hadoopPath, "hadoopPath must not be null"));
    URI parentGcsPath = UriPaths.getParentPath(gcsPath);
    GoogleCloudStorageItemInfo parentInfo = getGcsFs().getFileInfo(parentGcsPath).getItemInfo();
    if (!parentInfo.isRoot() && !parentInfo.isBucket() && !parentInfo.exists()) {
      throw new FileNotFoundException(
          String.format(
              "Can not create '%s' file, because parent folder does not exist: %s",
              gcsPath, parentGcsPath));
    }
    return create(
        hadoopPath,
        permission,
        flags.contains(org.apache.hadoop.fs.CreateFlag.OVERWRITE),
        bufferSize,
        replication,
        blockSize,
        progress);
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
    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    logger.atFiner().log(
        "append(hadoopPath: %s, bufferSize: %d [ignored])", hadoopPath, bufferSize);

    URI filePath = getGcsPath(hadoopPath);
    SyncableOutputStreamOptions syncableOutputStreamOptions =
        SyncableOutputStreamOptions.builder()
            .setAppendEnabled(true)
            .setMinSyncInterval(
                Duration.ofMillis(
                    GCS_OUTPUT_STREAM_SYNC_MIN_INTERVAL_MS.get(getConf(), getConf()::getInt)))
            .setSyncOnFlushEnabled(
                GCS_OUTPUT_STREAM_TYPE.get(getConf(), getConf()::getEnum) == FLUSHABLE_COMPOSITE)
            .build();
    FSDataOutputStream appendStream =
        new FSDataOutputStream(
            new GoogleHadoopSyncableOutputStream(
                this, filePath, statistics, DEFAULT_NO_OVERWRITE, syncableOutputStreamOptions),
            statistics);

    long duration = System.nanoTime() - startTime;
    increment(Counter.APPEND);
    increment(Counter.APPEND_TIME, duration);
    return appendStream;
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
    logger.atFinest().log("concat(tgt: %s, %d partitions: %s)", tgt, partitions.size(), partitions);
    for (List<URI> partition : partitions) {
      // We need to include the target in the list of sources to compose since
      // the GCS FS compose operation will overwrite the target, whereas the Hadoop
      // concat operation appends to the target.
      List<URI> sources = Lists.newArrayList(tgtPath);
      sources.addAll(partition);
      getGcsFs().compose(sources, tgtPath, CreateFileOptions.DEFAULT_CONTENT_TYPE);
    }
  }

  /**
   * Renames src to dst. Src must not be equal to the filesystem root.
   *
   * @param src Source path.
   * @param dst Destination path.
   * @return true if successful, or false if the old name does not exist or if the new name already
   *     belongs to the namespace.
   * @throws IOException if an error occurs.
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Preconditions.checkArgument(src != null, "src must not be null");
    Preconditions.checkArgument(dst != null, "dst must not be null");

    // Even though the underlying GCSFS will also throw an IAE if src is root, since our filesystem
    // root happens to equal the global root, we want to explicitly check it here since derived
    // classes may not have filesystem roots equal to the global root.
    if (src.makeQualified(this).equals(getFileSystemRoot())) {
      logger.atFiner().log("rename(src: %s, dst: %s): false [src is a root]", src, dst);
      return false;
    }
    try {
      renameInternal(src, dst);
    } catch (IOException e) {
      if (ApiErrorExtractor.INSTANCE.requestFailure(e)) {
        throw e;
      }
      logger.atFiner().withCause(e).log("rename(src: %s, dst: %s): false [failed]", src, dst);
      return false;
    }
    return true;
  }

  /**
   * Renames src to dst.
   *
   * @param src Source path.
   * @param dst Destination path.
   * @throws IOException if an error occurs.
   */
  void renameInternal(Path src, Path dst) throws IOException {
    Preconditions.checkArgument(src != null, "src must not be null");
    Preconditions.checkArgument(dst != null, "dst must not be null");

    long startTime = System.nanoTime();

    checkOpen();

    URI srcPath = getGcsPath(src);
    URI dstPath = getGcsPath(dst);

    getGcsFs().rename(srcPath, dstPath);

    long duration = System.nanoTime() - startTime;
    increment(Counter.RENAME);
    increment(Counter.RENAME_TIME, duration);
    logger.atFiner().log("rename(src: %s, dst: %s): true", src, dst);
  }

  /**
   * Deletes the given file or directory.
   *
   * @param hadoopPath The path to delete.
   * @param recursive If path is a directory and set to
   * true, the directory is deleted, else throws an exception.
   * In case of a file, the recursive parameter is ignored.
   * @return  true if delete is successful else false.
   * @throws IOException if an error occurs.
   */
  @Override
  public boolean delete(Path hadoopPath, boolean recursive) throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    checkOpen();

    URI gcsPath = getGcsPath(hadoopPath);
    try {
      getGcsFs().delete(gcsPath, recursive);
    } catch (DirectoryNotEmptyException e) {
      throw e;
    } catch (IOException e) {
      if (ApiErrorExtractor.INSTANCE.requestFailure(e)) {
        throw e;
      }
      logger.atFiner().withCause(e).log(
          "delete(hadoopPath: %s, recursive: %b): false [failed]", hadoopPath, recursive);
      return false;
    }

    long duration = System.nanoTime() - startTime;
    increment(Counter.DELETE);
    increment(Counter.DELETE_TIME, duration);
    logger.atFiner().log("delete(hadoopPath: %s, recursive: %b): true", hadoopPath, recursive);
    return true;
  }

  /**
   * Lists file status. If the given path points to a directory then the status
   * of children is returned, otherwise the status of the given file is returned.
   *
   * @param hadoopPath Given path.
   * @return File status list or null if path does not exist.
   * @throws IOException if an error occurs.
   */
  @Override
  public FileStatus[] listStatus(Path hadoopPath)
      throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    checkOpen();

    logger.atFinest().log("listStatus(hadoopPath: %s)", hadoopPath);

    URI gcsPath = getGcsPath(hadoopPath);
    List<FileStatus> status;

    try {
      List<FileInfo> fileInfos = getGcsFs().listFileInfo(gcsPath);
      status = new ArrayList<>(fileInfos.size());
      String userName = getUgiUserName();
      for (FileInfo fileInfo : fileInfos) {
        status.add(getFileStatus(fileInfo, userName));
      }
    } catch (FileNotFoundException fnfe) {
      throw (FileNotFoundException)
          new FileNotFoundException(
                  String.format(
                      "listStatus(hadoopPath: %s): '%s' does not exist.", hadoopPath, gcsPath))
              .initCause(fnfe);
    }

    long duration = System.nanoTime() - startTime;
    increment(Counter.LIST_STATUS);
    increment(Counter.LIST_STATUS_TIME, duration);
    return status.toArray(new FileStatus[0]);
  }

  /**
   * Sets the current working directory to the given path.
   *
   * @param hadoopPath New working directory.
   */
  @Override
  public void setWorkingDirectory(Path hadoopPath) {
    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    URI gcsPath = UriPaths.toDirectory(getGcsPath(hadoopPath));
    Path newPath = getHadoopPath(gcsPath);

    // Ideally we should check (as we did earlier) if the given path really points to an existing
    // directory. However, it takes considerable amount of time for that check which hurts perf.
    // Given that HDFS code does not do such checks either, we choose to not do them in favor of
    // better performance.

    workingDirectory = newPath;
    logger.atFinest().log("setWorkingDirectory(hadoopPath: %s): %s", hadoopPath, workingDirectory);

    long duration = System.nanoTime() - startTime;
    increment(Counter.SET_WD);
    increment(Counter.SET_WD_TIME, duration);
  }

  /**
   * Gets the current working directory.
   *
   * @return The current working directory.
   */
  @Override
  public Path getWorkingDirectory() {
    logger.atFinest().log("getWorkingDirectory(): %s", workingDirectory);
    return workingDirectory;
  }

  /**
   * Makes the given path and all non-existent parents directories.
   * Has the semantics of Unix 'mkdir -p'.
   *
   * @param hadoopPath Given path.
   * @param permission Permissions to set on the given directory.
   * @return true on success, false otherwise.
   * @throws IOException if an error occurs.
   */
  @Override
  public boolean mkdirs(Path hadoopPath, FsPermission permission)
      throws IOException {

    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    checkOpen();

    URI gcsPath = getGcsPath(hadoopPath);
    try {
      getGcsFs().mkdirs(gcsPath);
    } catch (java.nio.file.FileAlreadyExistsException faee) {
      // Need to convert to the Hadoop flavor of FileAlreadyExistsException.
      throw (FileAlreadyExistsException)
          new FileAlreadyExistsException(
                  String.format(
                      "mkdirs(hadoopPath: %s, permission: %s): failed", hadoopPath, permission))
              .initCause(faee);
    }

    long duration = System.nanoTime() - startTime;
    increment(Counter.MKDIRS);
    increment(Counter.MKDIRS_TIME, duration);
    logger.atFiner().log("mkdirs(hadoopPath: %s, permission: %s): true", hadoopPath, permission);
    return true;
  }

  /**
   * Gets the default replication factor.
   */
  @Override
  public short getDefaultReplication() {
    return REPLICATION_FACTOR_DEFAULT;
  }

  /**
   * Gets status of the given path item.
   *
   * @param hadoopPath The path we want information about.
   * @return A FileStatus object for the given path.
   * @throws FileNotFoundException when the path does not exist;
   * @throws IOException on other errors.
   */
  @Override
  public FileStatus getFileStatus(Path hadoopPath)
      throws IOException {

    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    checkOpen();

    URI gcsPath = getGcsPath(hadoopPath);
    FileInfo fileInfo = getGcsFs().getFileInfo(gcsPath);
    if (!fileInfo.exists()) {
      throw new FileNotFoundException(
          String.format(
              "%s not found: %s", fileInfo.isDirectory() ? "Directory" : "File", hadoopPath));
    }
    String userName = getUgiUserName();
    FileStatus status = getFileStatus(fileInfo, userName);

    long duration = System.nanoTime() - startTime;
    increment(Counter.GET_FILE_STATUS);
    increment(Counter.GET_FILE_STATUS_TIME, duration);
    return status;
  }

  /** Gets FileStatus corresponding to the given FileInfo value. */
  private FileStatus getFileStatus(FileInfo fileInfo, String userName) throws IOException {
    // GCS does not provide modification time. It only provides creation time.
    // It works for objects because they are immutable once created.
    FileStatus status =
        new FileStatus(
            fileInfo.getSize(),
            fileInfo.isDirectory(),
            REPLICATION_FACTOR_DEFAULT,
            defaultBlockSize,
            /* modificationTime= */ fileInfo.getModificationTime(),
            /* accessTime= */ fileInfo.getModificationTime(),
            reportedPermissions,
            /* owner= */ userName,
            /* group= */ userName,
            getHadoopPath(fileInfo.getPath()));
    logger.atFinest().log(
        "getFileStatus(path: %s, userName: %s): %s",
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
    if (!getUri().getScheme().equals(GoogleCloudStorageFileSystem.SCHEME)) {
      logger.atFinest().log(
          "Flat glob is on, but doesn't work for scheme '%s', using default behavior.",
          getUri().getScheme());
      return false;
    }

    // The full pattern should have a wildcard, otherwise there's no point doing the flat glob.
    GlobPattern fullPattern = new GlobPattern(fixedPath.toString());
    if (!fullPattern.hasWildcard()) {
      logger.atFinest().log(
          "Flat glob is on, but Path '%s' has no wildcard, using default behavior.", fixedPath);
      return false;
    }

    // To use a flat glob, there must be an authority defined.
    if (isNullOrEmpty(fixedPath.toUri().getAuthority())) {
      logger.atFinest().log(
          "Flat glob is on, but Path '%s' has a empty authority, using default behavior.",
          fixedPath);
      return false;
    }

    // And the authority must not contain a wildcard.
    GlobPattern authorityPattern = new GlobPattern(fixedPath.toUri().getAuthority());
    if (authorityPattern.hasWildcard()) {
      logger.atFinest().log(
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
   * Return null if pathPattern has no glob and the path does not exist.
   * Return an empty array if pathPattern has a glob and no path matches it.
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
   * Returns an array of FileStatus objects whose path names match pathPattern and is accepted by
   * the user-supplied path filter. Results are sorted by their path names.
   *
   * <p>Return null if pathPattern has no glob and the path does not exist. Return an empty array if
   * pathPattern has a glob and no path matches it.
   *
   * @param pathPattern A regular expression specifying the path pattern.
   * @param filter A user-supplied path filter.
   * @return An array of FileStatus objects.
   * @throws IOException if an error occurs.
   */
  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
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
    logger.atFinest().log("fixed path pattern: %s => %s", pathPattern, fixedPath);

    if (enableConcurrentGlob && couldUseFlatGlob(fixedPath)) {
      return concurrentGlobInternal(fixedPath, filter);
    }

    if (enableFlatGlob && couldUseFlatGlob(fixedPath)) {
      return flatGlobInternal(fixedPath, filter);
    }

    return super.globStatus(fixedPath, filter);
  }

  /**
   * Use 2 glob algorithms that return the same result but one of them could be significantly faster
   * than another one depending on directory layout.
   */
  private FileStatus[] concurrentGlobInternal(Path fixedPath, PathFilter filter)
      throws IOException {
    ExecutorService executorService = Executors.newFixedThreadPool(2, DAEMON_THREAD_FACTORY);
    Callable<FileStatus[]> flatGlobTask = () -> flatGlobInternal(fixedPath, filter);
    Callable<FileStatus[]> nonFlatGlobTask = () -> super.globStatus(fixedPath, filter);

    try {
      return executorService.invokeAny(Arrays.asList(flatGlobTask, nonFlatGlobTask));
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IOException("Concurrent glob execution failed", e);
    } finally {
      executorService.shutdownNow();
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
    logger.atFinest().log("Listing everything with '%s' prefix", prefixUri);
    List<FileStatus> matchedStatuses = null;
    String pageToken = null;
    do {
      ListPage<FileInfo> infoPage = getGcsFs().listAllFileInfoForPrefixPage(prefixUri, pageToken);

      // TODO: Are implicit directories really always needed for globbing?
      //  Probably they should be inferred only when fs.gs.implicit.dir.infer.enable is true.
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
        ((Comparator<FileStatus>) Comparator.<FileStatus>naturalOrder())
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

  private static boolean isImplicitDirectory(FileStatus curr) {
    // Modification time of 0 indicates implicit directory.
    return curr.isDir() && curr.getModificationTime() == 0;
  }

  /** Helper method that converts {@link FileInfo} collection to {@link FileStatus} collection. */
  private Collection<FileStatus> toFileStatusesWithImplicitDirectories(
      Collection<FileInfo> fileInfos) throws IOException {
    List<FileStatus> fileStatuses = new ArrayList<>(fileInfos.size());
    Set<URI> filePaths = Sets.newHashSetWithExpectedSize(fileInfos.size());
    String userName = getUgiUserName();
    for (FileInfo fileInfo : fileInfos) {
      filePaths.add(fileInfo.getPath());
      fileStatuses.add(getFileStatus(fileInfo, userName));
    }

    // The flow for populating this doesn't bother to populate metadata entries for parent
    // directories but we know the parent directories are expected to exist, so we'll just
    // populate the missing entries explicitly here. Necessary for getFileStatus(parentOfInfo)
    // to work when using an instance of this class.
    for (FileInfo fileInfo : fileInfos) {
      URI parentPath = UriPaths.getParentPath(fileInfo.getPath());
      while (parentPath != null && !parentPath.equals(GoogleCloudStorageFileSystem.GCS_ROOT)) {
        if (!filePaths.contains(parentPath)) {
          logger.atFinest().log("Adding fake entry for missing parent path '%s'", parentPath);
          StorageResourceId id = StorageResourceId.fromUriPath(parentPath, true);

          GoogleCloudStorageItemInfo fakeItemInfo =
              GoogleCloudStorageItemInfo.createInferredDirectory(id);
          FileInfo fakeFileInfo = FileInfo.fromItemInfo(fakeItemInfo);

          filePaths.add(parentPath);
          fileStatuses.add(getFileStatus(fakeFileInfo, userName));
        }
        parentPath = UriPaths.getParentPath(parentPath);
      }
    }

    return fileStatuses;
  }

  /** Helper method to get the UGI short user name */
  private static String getUgiUserName() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return ugi.getShortUserName();
  }

  /**
   * Returns home directory of the current user.
   *
   * Note: This directory is only used for Hadoop purposes.
   *       It is not the same as a user's OS home directory.
   */
  @Override
  public Path getHomeDirectory() {
    Path result = new Path(getFileSystemRoot(), getHomeDirectorySubpath());
    logger.atFinest().log("getHomeDirectory(): %s", result);
    return result;
  }

  /**
   * Converts the given FileStatus to its string representation.
   *
   * @param stat FileStatus to convert.
   * @return String representation of the given FileStatus.
   */
  private static String fileStatusToString(FileStatus stat) {
    assert stat != null;

    return String.format(
        "path: %s, isDir: %s, len: %d, owner: %s",
        stat.getPath().toString(),
        stat.isDir(),
        stat.getLen(),
        stat.getOwner());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns the service if delegation tokens are configured, otherwise, null.
   */
  @Override
  public String getCanonicalServiceName() {
    String service = null;
    if (delegationTokens != null) {
      service = delegationTokens.getService().toString();
    }
    logger.atFinest().log("getCanonicalServiceName(): %s", service);
    return service;
  }

  /** Gets GCS FS instance. */
  public GoogleCloudStorageFileSystem getGcsFs() {
    return gcsFsSupplier.get();
  }

  /**
   * Increments by 1 the counter indicated by key.
   */
  void increment(Counter key) {
    increment(key, 1);
  }

  /**
   * Adds value to the counter indicated by key.
   */
  void increment(Counter key, long value) {
    counters.get(key).addAndGet(value);
  }

  /**
   * Gets value of all counters as a formatted string.
   */
  @VisibleForTesting
  String countersToString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    double numNanoSecPerSec = TimeUnit.SECONDS.toNanos(1);
    String timeSuffix = "_TIME";
    for (Counter c : Counter.values()) {
      String name = c.toString();
      if (!name.endsWith(timeSuffix)) {
        // Log invocation counter.
        long count = counters.get(c).get();
        sb.append(String.format("%20s = %d calls\n", name, count));

        // Log duration counter.
        String timeCounterName = name + timeSuffix;
        double totalTime =
            counters.get(Enum.valueOf(Counter.class, timeCounterName)).get()
                / numNanoSecPerSec;
        sb.append(String.format("%20s = %.2f sec\n", timeCounterName, totalTime));

        // Compute and log average duration per call (== total duration / num invocations).
        String avgName = name + " avg.";
        double avg = totalTime / count;
        sb.append(String.format("%20s = %.2f sec / call\n\n", avgName, avg));
      }
    }
    return sb.toString();
  }

  /**
   * Logs values of all counters.
   */
  private void logCounters() {
    logger.atFine().log("%s", lazy(this::countersToString));
  }

  /**
   * Retrieve user's Credential. If user implemented {@link AccessTokenProvider} and provided the
   * class name (See {@link HadoopCredentialConfiguration#ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX} then
   * build a credential with access token provided by this provider; Otherwise obtain credential
   * through {@link HadoopCredentialConfiguration#getCredentialFactory(Configuration, String...)}.
   */
  private Credential getCredential(
      Configuration config, GoogleCloudStorageFileSystemOptions gcsFsOptions)
      throws IOException, GeneralSecurityException {
    Credential credential = null;

    // Check if delegation token support is configured
    if (delegationTokens != null) {
      // If so, use the delegation token to acquire the Google credentials
      AccessTokenProvider atp = delegationTokens.getAccessTokenProvider();
      if (atp != null) {
        atp.setConf(config);
        credential =
            CredentialFromAccessTokenProviderClassFactory.credential(
                atp, CredentialFactory.GCS_SCOPES);
      }
    } else {
      // If delegation token support is not configured, check if a
      // custom AccessTokenProvider implementation is configured, and attempt
      // to acquire the Google credentials using it
      credential =
          CredentialFromAccessTokenProviderClassFactory.credential(
              config, ImmutableList.of(GCS_CONFIG_PREFIX), CredentialFactory.GCS_SCOPES);

      if (credential == null) {
        // Finally, if no credentials have been acquired at this point, employ
        // the default mechanism.
        credential =
            HadoopCredentialConfiguration.getCredentialFactory(config, GCS_CONFIG_PREFIX)
                .getCredential(CredentialFactory.GCS_SCOPES);
      }
    }

    // If impersonation service account exists, then use current credential to request access token
    // for the impersonating service account.
    return getImpersonatedCredential(config, gcsFsOptions, credential).orElse(credential);
  }

  /**
   * Generate a {@link Credential} from the internal access token provider based on the service
   * account to impersonate.
   */
  private static Optional<Credential> getImpersonatedCredential(
      Configuration config, GoogleCloudStorageFileSystemOptions gcsFsOptions, Credential credential)
      throws IOException {
    GoogleCloudStorageOptions options = gcsFsOptions.getCloudStorageOptions();
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    Optional<String> serviceAccountToImpersonate =
        Stream.of(
                () ->
                    getServiceAccountToImpersonateForUserGroup(
                        USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX
                            .withPrefixes(CONFIG_KEY_PREFIXES)
                            .getPropsWithPrefix(config),
                        ImmutableList.of(currentUser.getShortUserName())),
                () ->
                    getServiceAccountToImpersonateForUserGroup(
                        GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX
                            .withPrefixes(CONFIG_KEY_PREFIXES)
                            .getPropsWithPrefix(config),
                        ImmutableList.copyOf(currentUser.getGroupNames())),
                (Supplier<Optional<String>>)
                    () ->
                        Optional.ofNullable(
                            IMPERSONATION_SERVICE_ACCOUNT_SUFFIX
                                .withPrefixes(CONFIG_KEY_PREFIXES)
                                .get(config, config::get)))
            .map(Supplier::get)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(sa -> !isNullOrEmpty(sa))
            .findFirst();

    if (serviceAccountToImpersonate.isPresent()) {
      HttpTransport httpTransport =
          HttpTransportFactory.createHttpTransport(
              options.getTransportType(),
              options.getProxyAddress(),
              options.getProxyUsername(),
              options.getProxyPassword());
      GoogleCredential impersonatedCredential =
          new GoogleCredentialWithIamAccessToken(
              httpTransport,
              new CredentialHttpRetryInitializer(credential),
              serviceAccountToImpersonate.get(),
              CredentialFactory.GCS_SCOPES);
      logger.atFine().log(
          "Impersonating '%s' service account for '%s' user",
          serviceAccountToImpersonate.get(), currentUser);
      return Optional.of(impersonatedCredential.createScoped(CredentialFactory.GCS_SCOPES));
    }

    return Optional.empty();
  }

  private static Optional<String> getServiceAccountToImpersonateForUserGroup(
      Map<String, String> serviceAccountMapping, List<String> userGroups) {
    return serviceAccountMapping.entrySet().stream()
        .filter(e -> userGroups.contains(e.getKey()))
        .map(Map.Entry::getValue)
        .findFirst();
  }

  /**
   * Configures GHFS using the supplied configuration.
   *
   * @param config Hadoop configuration object.
   */
  private synchronized void configure(Configuration config) throws IOException {
    logger.atFine().log("GHFS_ID=%s: configure(config: %s)", GHFS_ID, config);

    overrideConfigFromFile(config);
    // Set this configuration as the default config for this instance.
    setConf(config);

    enableFlatGlob = GCS_FLAT_GLOB_ENABLE.get(config, config::getBoolean);
    enableConcurrentGlob = GCS_CONCURRENT_GLOB_ENABLE.get(config, config::getBoolean);
    checksumType = GCS_FILE_CHECKSUM_TYPE.get(config, config::getEnum);
    defaultBlockSize = BLOCK_SIZE.get(config, config::getLong);
    reportedPermissions = new FsPermission(PERMISSIONS_TO_REPORT.get(config, config::get));

    if (gcsFsSupplier == null) {
      if (GCS_LAZY_INITIALIZATION_ENABLE.get(config, config::getBoolean)) {
        gcsFsSupplier =
            Suppliers.memoize(
                () -> {
                  try {
                    GoogleCloudStorageFileSystem gcsFs = createGcsFs(config);

                    configureBuckets(gcsFs);
                    configureWorkingDirectory(config);
                    gcsFsInitialized = true;

                    return gcsFs;
                  } catch (IOException e) {
                    throw new RuntimeException("Failed to create GCS FS", e);
                  }
                });
      } else {
        setGcsFs(createGcsFs(config));
        configureBuckets(getGcsFs());
        configureWorkingDirectory(config);
      }
    } else {
      configureBuckets(getGcsFs());
      configureWorkingDirectory(config);
    }
  }

  /**
   * If overrides file configured, update properties from override file into {@link Configuration}
   * object
   */
  private void overrideConfigFromFile(Configuration config) throws IOException {
    String configFile = GCS_CONFIG_OVERRIDE_FILE.get(config, config::get);
    if (configFile != null) {
      config.addResource(new FileInputStream(configFile));
    }
  }

  private GoogleCloudStorageFileSystem createGcsFs(Configuration config) throws IOException {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config).build();

    Credential credential;
    try {
      credential = getCredential(config, gcsFsOptions);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }

    return new GoogleCloudStorageFileSystem(credential, gcsFsOptions);
  }

  /**
   * Validates and possibly creates buckets needed by subclass.
   *
   * @param gcsFs {@link GoogleCloudStorageFileSystem} to configure buckets
   * @throws IOException if bucket name is invalid or cannot be found.
   */
  @VisibleForTesting
  protected abstract void configureBuckets(GoogleCloudStorageFileSystem gcsFs) throws IOException;

  private void configureWorkingDirectory(Configuration config) {
    // Set initial working directory to root so that any configured value gets resolved
    // against file system root.
    workingDirectory = getFileSystemRoot();

    Path newWorkingDirectory;
    String configWorkingDirectory = GCS_WORKING_DIRECTORY.get(config, config::get);
    if (isNullOrEmpty(configWorkingDirectory)) {
      newWorkingDirectory = getDefaultWorkingDirectory();
      logger.atWarning().log(
          "No working directory configured, using default: '%s'", newWorkingDirectory);
    } else {
      newWorkingDirectory = new Path(configWorkingDirectory);
    }

    // Use the public method to ensure proper behavior of normalizing and resolving the new
    // working directory relative to the initial filesystem-root directory.
    setWorkingDirectory(newWorkingDirectory);
    logger.atFinest().log(
        "Configured working directory: %s = %s",
        GCS_WORKING_DIRECTORY.getKey(), getWorkingDirectory());
  }

  /**
   * Assert that the FileSystem has been initialized and not close()d.
   */
  private void checkOpen() throws IOException {
    if (isClosed()) {
      throw new IOException("GoogleHadoopFileSystem has been closed or not initialized.");
    }
  }

  private boolean isClosed() {
    return gcsFsSupplier == null || gcsFsSupplier.get() == null;
  }

  // =================================================================
  // Overridden functions for debug tracing. The following functions
  // do not change functionality. They just log parameters and call base
  // class' function.
  // =================================================================

  @Override
  public boolean deleteOnExit(Path f) throws IOException {
    checkOpen();
    boolean result = super.deleteOnExit(f);
    logger.atFiner().log("deleteOnExit(path: %s): %b", f, result);
    return result;
  }

  @Override
  protected void processDeleteOnExit() {
    logger.atFinest().log("processDeleteOnExit()");
    super.processDeleteOnExit();
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    ContentSummary result = super.getContentSummary(f);
    logger.atFinest().log("getContentSummary(path: %s): %b", f, result);
    return result;
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
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
    logger.atFiner().log(
        "copyFromLocalFile(delSrc: %b, overwrite: %b, %d srcs, dst: %s)",
        delSrc, overwrite, srcs.length, dst);
    super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
      throws IOException {
    logger.atFiner().log(
        "copyFromLocalFile(delSrc: %b, overwrite: %b, src: %s, dst: %s)",
        delSrc, overwrite, src, dst);
    super.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
      throws IOException {
    logger.atFiner().log("copyToLocalFile(delSrc: %b, src: %s, dst: %s)", delSrc, src, dst);
    super.copyToLocalFile(delSrc, src, dst);
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
    Path result = super.startLocalOutput(fsOutputFile, tmpLocalFile);
    logger.atFiner().log(
        "startLocalOutput(fsOutputFile: %s, tmpLocalFile: %s): %s",
        fsOutputFile, tmpLocalFile, result);
    return result;
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
    logger.atFiner().log(
        "startLocalOutput(fsOutputFile: %s, tmpLocalFile: %s)", fsOutputFile, tmpLocalFile);
    super.completeLocalOutput(fsOutputFile, tmpLocalFile);
  }

  @Override
  public void close() throws IOException {
    logger.atFinest().log("close()");
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
    logCounters();
  }

  @Override
  public long getUsed() throws IOException {
    long result = super.getUsed();
    logger.atFinest().log("getUsed(): %s", result);
    return result;
  }

  @Override
  public long getDefaultBlockSize() {
    long result = defaultBlockSize;
    logger.atFinest().log("getDefaultBlockSize(): %d", result);
    return result;
  }

  @Override
  public FileChecksum getFileChecksum(Path hadoopPath) throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    checkOpen();

    URI gcsPath = getGcsPath(hadoopPath);
    final FileInfo fileInfo = getGcsFs().getFileInfo(gcsPath);
    if (!fileInfo.exists()) {
      throw new FileNotFoundException(
          String.format(
              "%s not found: %s", fileInfo.isDirectory() ? "Directory" : "File", hadoopPath));
    }
    FileChecksum checksum = getFileChecksum(checksumType, fileInfo);
    logger.atFinest().log(
        "getFileChecksum(hadoopPath: %s [gcsPath: %s]): %s", hadoopPath, gcsPath, checksum);

    long duration = System.nanoTime() - startTime;
    increment(Counter.GET_FILE_CHECKSUM);
    increment(Counter.GET_FILE_CHECKSUM_TIME, duration);
    return checksum;
  }

  private static FileChecksum getFileChecksum(GcsFileChecksumType type, FileInfo fileInfo)
      throws IOException {
    switch (type) {
      case NONE:
        return null;
      case CRC32C:
        return new GcsFileChecksum(
            type, fileInfo.getItemInfo().getVerificationAttributes().getCrc32c());
      case MD5:
        return new GcsFileChecksum(
            type, fileInfo.getItemInfo().getVerificationAttributes().getMd5hash());
    }
    throw new IOException("Unrecognized GcsFileChecksumType: " + type);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    logger.atFinest().log("setVerifyChecksum(verifyChecksum: %s)", verifyChecksum);
    super.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    logger.atFinest().log("setPermission(path: %s, permission: %s)", p, permission);
    super.setPermission(p, permission);
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    logger.atFinest().log(
        "setOwner(path: %s, username: %s, groupname: %s)", p, username, groupname);
    super.setOwner(p, username, groupname);
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    logger.atFinest().log("setTimes(path: %s, mtime: %d, atime: %d)", p, mtime, atime);
    super.setTimes(p, mtime, atime);
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    checkNotNull(path, "path should not be null");
    checkNotNull(name, "name should not be null");

    Map<String, byte[]> attributes = getGcsFs().getFileInfo(getGcsPath(path)).getAttributes();
    String xAttrKey = getXAttrKey(name);
    byte[] xAttr =
        attributes.containsKey(xAttrKey) ? getXAttrValue(attributes.get(xAttrKey)) : null;

    logger.atFiner().log(
        "getXAttr(path: %s, name: %s): %s", path, name, lazy(() -> new String(xAttr, UTF_8)));
    return xAttr;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
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
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
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
  }

  /** {@inheritDoc} */
  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    checkNotNull(path, "path should not be null");

    FileInfo fileInfo = getGcsFs().getFileInfo(getGcsPath(path));

    List<String> xAttrs =
        fileInfo.getAttributes().keySet().stream()
            .filter(this::isXAttr)
            .map(this::getXAttrName)
            .collect(Collectors.toCollection(ArrayList::new));

    logger.atFiner().log("listXAttrs(path: %s): %s", path, xAttrs);
    return xAttrs;
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
      throw new IOException(
          String.format(
              "REPLACE flag must be set to update XAttr (name='%s', value='%s') for '%s'",
              name, new String(value, UTF_8), path));
    }
    if (!attributes.containsKey(xAttrKey) && !flags.contains(XAttrSetFlag.CREATE)) {
      throw new IOException(
          String.format(
              "CREATE flag must be set to create XAttr (name='%s', value='%s') for '%s'",
              name, new String(value, UTF_8), path));
    }

    UpdatableItemInfo updateInfo =
        new UpdatableItemInfo(
            fileInfo.getItemInfo().getResourceId(),
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
        new UpdatableItemInfo(fileInfo.getItemInfo().getResourceId(), xAttrToRemove);
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
}
