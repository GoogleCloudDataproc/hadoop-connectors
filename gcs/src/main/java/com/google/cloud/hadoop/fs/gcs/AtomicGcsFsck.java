package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_COOPERATIVE_LOCKING_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.google.cloud.hadoop.gcsio.GcsAtomicOperations;
import com.google.cloud.hadoop.gcsio.GcsAtomicOperations.Operation;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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

  private static final int LOCK_EXPIRATION_SECONDS = 120;

  public static void main(String[] args) throws Exception {
    String bucket = args[0];
    checkArgument(bucket.startsWith("gs://"), "bucket parameter should have 'gs://' scheme");

    Configuration conf = new Configuration();
    // Disable cooperative locking to prevent blocking
    conf.set(GCS_COOPERATIVE_LOCKING_ENABLE.getKey(), "false");
    conf.set(GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.getKey(), "false");

    URI bucketUri = URI.create(bucket);
    GoogleHadoopFileSystem ghFs = (GoogleHadoopFileSystem) FileSystem.get(bucketUri, conf);
    GoogleCloudStorageFileSystem gcsFs = ghFs.getGcsFs();
    GoogleCloudStorage gcs = gcsFs.getGcs();
    GcsAtomicOperations gcsAtomic = gcsFs.getGcsAtomic();

    Instant operationExpirationTime = Instant.now();

    Set<Operation> lockedOperations = gcsAtomic.getLockedOperations(bucketUri.getAuthority());
    if (lockedOperations.isEmpty()) {
      logger.atInfo().log("No expired operation locks");
      return;
    }

    Map<FileStatus, String[]> expiredOperations = new HashMap<>();
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
        do {
          try {
            if (gcsAtomic.unlockPaths(lockedOperation.getOperationId(), lockedResources)) {
              break;
            }
          } catch (Exception e) {
            logger.atWarning().withCause(e).log(
                "Failed to unlock (operation=%s, resources=%s), retrying.",
                operationId, lockedResources);
          }
        } while (true);
        continue;
      }

      FileStatus operation = operationStatuses[0];
      String[] content;
      try (FSDataInputStream in = ghFs.open(operation.getPath())) {
        content = IOUtils.toString(in).split("\\n");
      }

      Instant operationLockEpoch = Instant.ofEpochSecond(Long.parseLong(content[0]));
      if (operationLockEpoch
          .plus(LOCK_EXPIRATION_SECONDS, SECONDS)
          .isBefore(operationExpirationTime)) {
        expiredOperations.put(operation, content);
        logger.atInfo().log("Operation %s expired.", operation.getPath());
      } else {
        logger.atInfo().log("Operation %s not expired.", operation.getPath());
      }
    }

    Function<Map.Entry<FileStatus, String[]>, Boolean> operationRecovery =
        expiredOperation -> {
          FileStatus operation = expiredOperation.getKey();
          String[] operationContent = expiredOperation.getValue();

          String operationId = getOperationId(operation);
          try {
            if (operation.getPath().toString().contains("_delete_")) {
              logger.atInfo().log("Repairing FS after %s delete operation.", operation.getPath());
              ghFs.delete(new Path(operationContent[1]), /* recursive= */ true);
              do {
                try {
                  if (gcsAtomic.unlockPaths(
                      operationId, StorageResourceId.fromObjectName(operationContent[1]))) {
                    break;
                  }
                } catch (Exception e) {
                  logger.atWarning().withCause(e).log(
                      "Failed to unlock (client=%s, res=%s), retrying.",
                      operationId, operationContent[1]);
                }
              } while (true);
            } else if (operation.getPath().toString().contains("_rename_")) {
              boolean copySucceeded = Boolean.valueOf(operationContent[3]);
              if (copySucceeded) {
                logger.atInfo().log(
                    "Repairing FS after %s rename operation (deleting source (%s)).",
                    operation.getPath(), operationContent[1]);
                ghFs.delete(new Path(operationContent[1]), /* recursive= */ true);
              } else {
                logger.atInfo().log(
                    "Repairing FS after %s rename operation"
                        + " (deleting destination (%s) and renaming (%s -> %s)).",
                    operation.getPath(),
                    operationContent[2],
                    operationContent[1],
                    operationContent[2]);
                ghFs.delete(new Path(operationContent[2]), /* recursive= */ true);
                ghFs.rename(new Path(operationContent[1]), new Path(operationContent[2]));
              }
              do {
                try {
                  if (gcsAtomic.unlockPaths(
                      operationId,
                      StorageResourceId.fromObjectName(operationContent[1]),
                      StorageResourceId.fromObjectName(operationContent[2]))) {
                    break;
                  }
                } catch (Exception e) {
                  logger.atWarning().withCause(e).log(
                      "Failed to unlock (client=%s, src=%s, dst=%s), retrying.",
                      operationId, operationContent[1], operationContent[2]);
                }
              } while (true);
            } else {
              throw new IllegalStateException("Unknown operation type: " + operation.getPath());
            }
          } catch (IOException e) {
            throw new RuntimeException("Failed to recover operation: ", e);
          }
          return true;
        };

    for (Map.Entry<FileStatus, String[]> expiredOperation : expiredOperations.entrySet()) {
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

  private static String getOperationId(FileStatus operation) {
    String[] fileParts = operation.getPath().toString().split("_");
    return fileParts[fileParts.length - 1].split("\\.")[0];
  }
}
