package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_COOPERATIVE_LOCKING_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_COOPERATIVE_LOCKING_EXPIRATION_TIMEOUT_MS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.time.temporal.ChronoUnit.MILLIS;

import com.google.cloud.hadoop.gcsio.GcsAtomicOperations;
import com.google.cloud.hadoop.gcsio.GcsAtomicOperations.Operation;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem.DeleteOperation;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem.RenameOperation;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.Gson;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * FSCK tool to recover failed directory mutations guarded by GCS Connector Cooperative Locking
 * feature.
 *
 * <p>Usage: <code>
 *   hadoop jar /usr/lib/hadoop/lib/gcs-connector.jar
 *       com.google.cloud.hadoop.fs.gcs.AtomicGcsFsck gs://my-bucket
 * </code>
 */
public class AtomicGcsFsck {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final Gson GSON = new Gson();

  private final String bucket;
  private final Configuration conf;

  public static void main(String[] args) throws Exception {
    new AtomicGcsFsck(args[0]).repair();
  }

  AtomicGcsFsck(String bucket) {
    this(bucket, new Configuration());
  }

  AtomicGcsFsck(String bucket, Configuration conf) {
    this.bucket = bucket;
    checkArgument(bucket.startsWith("gs://"), "bucket parameter should have 'gs://' scheme");
    this.conf = conf;
  }

  void repair() throws Exception {
    // Disable cooperative locking to prevent blocking
    conf.set(GCS_COOPERATIVE_LOCKING_ENABLE.getKey(), "false");
    conf.set(GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.getKey(), "false");

    URI bucketUri = URI.create(bucket);
    String bucketName = bucketUri.getAuthority();
    GoogleHadoopFileSystem ghFs = (GoogleHadoopFileSystem) FileSystem.get(bucketUri, conf);
    GoogleCloudStorageFileSystem gcsFs = ghFs.getGcsFs();
    GcsAtomicOperations gcsAtomic = gcsFs.getGcsAtomic();

    Instant operationExpirationTime = Instant.now();

    Set<Operation> lockedOperations = gcsAtomic.getLockedOperations(bucketUri.getAuthority());
    if (lockedOperations.isEmpty()) {
      logger.atInfo().log("No expired operation locks");
      return;
    }

    Map<FileStatus, Operation> expiredOperations = new HashMap<>();
    for (Operation lockedOperation : lockedOperations) {
      String operationId = lockedOperation.getOperationId();
      URI operationPattern =
          bucketUri.resolve(
              "/" + GcsAtomicOperations.LOCK_DIRECTORY + "*" + operationId + "*.lock");
      FileStatus[] operationStatuses = ghFs.globStatus(new Path(operationPattern));
      checkState(
          operationStatuses.length < 2,
          "operation %s should not have more than one lock file",
          operationId);

      // Lock file not created - nothing to repair
      if (operationStatuses.length == 0) {
        logger.atInfo().log(
            "Operation %s for %s resources doesn't have lock file, unlocking",
            lockedOperation.getOperationId(), lockedOperation.getResources());
        StorageResourceId[] lockedResources =
            lockedOperation.getResources().stream()
                .map(r -> StorageResourceId.fromObjectName(bucketUri.resolve("/" + r).toString()))
                .toArray(StorageResourceId[]::new);
        gcsAtomic.unlockPaths(lockedOperation.getOperationId(), lockedResources);
        continue;
      }

      FileStatus operation = operationStatuses[0];

      Instant lockInstant = Instant.ofEpochSecond(lockedOperation.getLockEpochSeconds());
      Instant renewedInstant = getLockRenewedInstant(ghFs, operation);
      if (isLockExpired(renewedInstant, operationExpirationTime)
          && isLockExpired(lockInstant, operationExpirationTime)) {
        expiredOperations.put(operation, lockedOperation);
        logger.atInfo().log("Operation %s expired.", operation.getPath());
      } else {
        logger.atInfo().log("Operation %s not expired.", operation.getPath());
      }
    }

    Function<Map.Entry<FileStatus, Operation>, Boolean> operationRecovery =
        expiredOperation -> {
          FileStatus operation = expiredOperation.getKey();
          Operation lockedOperation = expiredOperation.getValue();

          String operationId = getOperationId(operation);
          try {
            if (operation.getPath().toString().contains("_delete_")) {
              logger.atInfo().log("Repairing FS after %s delete operation.", operation.getPath());
              DeleteOperation operationObject =
                  getOperationObject(ghFs, operation, DeleteOperation.class);
              gcsAtomic.lockOperation(
                  bucketName, operationId, lockedOperation.getLockEpochSeconds());
              Future<?> lockUpdateFuture =
                  gcsFs.scheduleLockUpdate(
                      operationId,
                      new URI(operation.getPath().toString()),
                      DeleteOperation.class,
                      (o, i) -> o.setLockEpochSeconds(i.getEpochSecond()));
              try {
                ghFs.delete(new Path(operationObject.getResource()), /* recursive= */ true);
                gcsAtomic.unlockPaths(
                    operationId, StorageResourceId.fromObjectName(operationObject.getResource()));
              } finally {
                lockUpdateFuture.cancel(/* mayInterruptIfRunning= */ false);
              }
            } else if (operation.getPath().toString().contains("_rename_")) {
              RenameOperation operationObject =
                  getOperationObject(ghFs, operation, RenameOperation.class);
              gcsAtomic.lockOperation(
                  bucketName, operationId, lockedOperation.getLockEpochSeconds());
              Future<?> lockUpdateFuture =
                  gcsFs.scheduleLockUpdate(
                      operationId,
                      new URI(operation.getPath().toString()),
                      RenameOperation.class,
                      (o, i) -> o.setLockEpochSeconds(i.getEpochSecond()));
              try {
                if (operationObject.getCopySucceeded()) {
                  logger.atInfo().log(
                      "Repairing FS after %s rename operation (deleting source (%s)).",
                      operation.getPath(), operationObject.getSrcResource());
                  ghFs.delete(new Path(operationObject.getSrcResource()), /* recursive= */ true);
                } else {
                  logger.atInfo().log(
                      "Repairing FS after %s rename operation"
                          + " (deleting destination (%s) and renaming (%s -> %s)).",
                      operation.getPath(),
                      operationObject.getDstResource(),
                      operationObject.getSrcResource(),
                      operationObject.getDstResource());
                  ghFs.delete(new Path(operationObject.getDstResource()), /* recursive= */ true);
                  ghFs.rename(
                      new Path(operationObject.getSrcResource()),
                      new Path(operationObject.getDstResource()));
                }
                gcsAtomic.unlockPaths(
                    operationId,
                    StorageResourceId.fromObjectName(operationObject.getSrcResource()),
                    StorageResourceId.fromObjectName(operationObject.getDstResource()));
              } finally {
                lockUpdateFuture.cancel(/* mayInterruptIfRunning= */ false);
              }
            } else {
              throw new IllegalStateException("Unknown operation type: " + operation.getPath());
            }
          } catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Failed to recover operation: ", e);
          }
          return true;
        };

    for (Map.Entry<FileStatus, Operation> expiredOperation : expiredOperations.entrySet()) {
      long start = System.currentTimeMillis();
      try {
        boolean succeeded = operationRecovery.apply(expiredOperation);
        long finish = System.currentTimeMillis();
        if (succeeded) {
          logger.atInfo().log(
              "Operation %s successfully rolled forward in %dms", expiredOperation, finish - start);
        } else {
          logger.atSevere().log(
              "Operation %s failed to roll forward in %dms", expiredOperation, finish - start);
        }
      } catch (Exception e) {
        long finish = System.currentTimeMillis();
        logger.atSevere().withCause(e).log(
            "Operation %s failed to roll forward in %dms", expiredOperation, finish - start);
      }
    }
  }

  private boolean isLockExpired(Instant lockInstant, Instant expirationInstant) {
    return lockInstant
        .plus(GCS_COOPERATIVE_LOCKING_EXPIRATION_TIMEOUT_MS.get(conf, conf::getLong), MILLIS)
        .isBefore(expirationInstant);
  }

  private static Instant getLockRenewedInstant(GoogleHadoopFileSystem ghfs, FileStatus operation)
      throws IOException {
    if (operation.getPath().toString().contains("_delete_")) {
      return Instant.ofEpochSecond(
          getOperationObject(ghfs, operation, DeleteOperation.class).getLockEpochSeconds());
    }
    if (operation.getPath().toString().contains("_rename_")) {
      return Instant.ofEpochSecond(
          getOperationObject(ghfs, operation, RenameOperation.class).getLockEpochSeconds());
    }
    throw new IllegalStateException("Unknown operation type: " + operation.getPath());
  }

  private static <T> T getOperationObject(
      GoogleHadoopFileSystem ghfs, FileStatus operation, Class<T> clazz) throws IOException {
    String operationContent;
    try (FSDataInputStream in = ghfs.open(operation.getPath())) {
      operationContent = IOUtils.toString(in);
    }
    return GSON.fromJson(operationContent, clazz);
  }

  private static String getOperationId(FileStatus operation) {
    String[] fileParts = operation.getPath().toString().split("_");
    return fileParts[fileParts.length - 1].split("\\.")[0];
  }
}
