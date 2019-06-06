package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_COOPERATIVE_LOCKING_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_COOPERATIVE_LOCKING_EXPIRATION_TIMEOUT_MS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.google.cloud.hadoop.gcsio.GcsAtomicOperations;
import com.google.cloud.hadoop.gcsio.GcsAtomicOperations.Operation;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem.DeleteOperation;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem.RenameOperation;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * FSCK tool to recover failed directory mutations guarded by GCS Connector Cooperative Locking
 * feature.
 *
 * <p>Usage: <code>
 *   hadoop jar /usr/lib/hadoop/lib/gcs-connector.jar
 *       com.google.cloud.hadoop.fs.gcs.AtomicGcsFsck --rollForward gs://my-bucket
 * </code>
 */
public class AtomicGcsFsck extends Configured implements Tool {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final String COMMAND_CHECK = "--check";
  private static final String COMMAND_ROLL_FORWARD = "--rollForward";

  private static final Set<String> FSCK_COMMANDS =
      ImmutableSet.of(COMMAND_CHECK, COMMAND_ROLL_FORWARD);

  private static final Gson GSON = new Gson();
  private static final Splitter RENAME_LOG_RECORD_SPLITTER = Splitter.on(" -> ");

  public static void main(String[] args) throws Exception {
    if (args.length == 1 && "--help".equals(args[0])) {
      System.out.println(
          "FSCK tool to recover failed directory mutations guarded by"
              + " GCS Connector Cooperative Locking feature."
              + "\n\nUsage:"
              + String.format(
                  "\n\thadoop jar /usr/lib/hadoop/lib/gcs-connector.jar %s <COMMAND> gs://<BUCKET>",
                  AtomicGcsFsck.class.getCanonicalName())
              + "\n\nSupported commands:"
              + String.format(
                  "\n\t%s - prints out failed operation for the bucket", COMMAND_CHECK)
              + String.format(
                  "\n\t%s - recover directory operations in the bucket by rolling them forward",
                  COMMAND_ROLL_FORWARD));
      return;
    }

    // Let ToolRunner handle generic command-line options
    int res = ToolRunner.run(new Configuration(), new AtomicGcsFsck(), args);

    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    checkArgument(
        args.length == 2, "2 arguments should be specified, but were: %s", Arrays.asList(args));

    String command = args[0];
    checkArgument(FSCK_COMMANDS.contains(command), "Unknown %s command, should be %s", command);

    String bucket = args[1];
    checkArgument(bucket.startsWith("gs://"), "bucket parameter should have 'gs://' scheme");

    Configuration conf = getConf();

    // Disable cooperative locking to prevent blocking
    conf.set(GCS_COOPERATIVE_LOCKING_ENABLE.getKey(), "false");
    conf.set(GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.getKey(), "false");

    URI bucketUri = URI.create(bucket);
    String bucketName = bucketUri.getAuthority();
    GoogleHadoopFileSystem ghfs = (GoogleHadoopFileSystem) FileSystem.get(bucketUri, conf);
    GoogleCloudStorageFileSystem gcsFs = ghfs.getGcsFs();
    GcsAtomicOperations gcsAtomic = gcsFs.getGcsAtomic();

    Instant operationExpirationTime = Instant.now();

    Set<Operation> lockedOperations = gcsAtomic.getLockedOperations(bucketUri.getAuthority());
    if (lockedOperations.isEmpty()) {
      logger.atInfo().log("No expired operation locks");
      return 0;
    }

    Map<FileStatus, Operation> expiredOperations = new HashMap<>();
    for (Operation lockedOperation : lockedOperations) {
      String operationId = lockedOperation.getOperationId();
      URI operationPattern =
          bucketUri.resolve(
              "/" + GcsAtomicOperations.LOCK_DIRECTORY + "*" + operationId + "*.lock");
      FileStatus[] operationStatuses = ghfs.globStatus(new Path(operationPattern));
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
      Instant renewedInstant = getLockRenewedInstant(ghfs, operation);
      if (isLockExpired(conf, renewedInstant, operationExpirationTime)
          && isLockExpired(conf, lockInstant, operationExpirationTime)) {
        expiredOperations.put(operation, lockedOperation);
        logger.atInfo().log("Operation %s expired.", operation.getPath());
      } else {
        logger.atInfo().log("Operation %s not expired.", operation.getPath());
      }
    }

    if (COMMAND_CHECK.equals(command)) {
      return 0;
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
                  getOperationObject(ghfs, operation, DeleteOperation.class);
              gcsAtomic.lockOperation(
                  bucketName, operationId, lockedOperation.getLockEpochSeconds());
              Future<?> lockUpdateFuture =
                  gcsFs.scheduleLockUpdate(
                      operationId,
                      new URI(operation.getPath().toString()),
                      DeleteOperation.class,
                      (o, i) -> o.setLockEpochSeconds(i.getEpochSecond()));
              try {
                List<String> loggedResources = getOperationLog(ghfs, operation, l -> l);
                deleteResource(ghfs, operationObject.getResource(), loggedResources);
                gcsAtomic.unlockPaths(
                    operationId, StorageResourceId.fromObjectName(operationObject.getResource()));
              } finally {
                lockUpdateFuture.cancel(/* mayInterruptIfRunning= */ false);
              }
            } else if (operation.getPath().toString().contains("_rename_")) {
              RenameOperation operationObject =
                  getOperationObject(ghfs, operation, RenameOperation.class);
              gcsAtomic.lockOperation(
                  bucketName, operationId, lockedOperation.getLockEpochSeconds());
              Future<?> lockUpdateFuture =
                  gcsFs.scheduleLockUpdate(
                      operationId,
                      new URI(operation.getPath().toString()),
                      RenameOperation.class,
                      (o, i) -> o.setLockEpochSeconds(i.getEpochSecond()));
              try {
                List<Pair<String, String>> loggedResources =
                    getOperationLog(
                        ghfs,
                        operation,
                        l -> {
                          List<String> srcToDst = RENAME_LOG_RECORD_SPLITTER.splitToList(l);
                          checkState(srcToDst.size() == 2);
                          return Pair.of(srcToDst.get(0), srcToDst.get(1));
                        });
                if (operationObject.getCopySucceeded()) {
                  logger.atInfo().log(
                      "Repairing FS after %s rename operation (deleting source (%s)).",
                      operation.getPath(), operationObject.getSrcResource());
                  deleteResource(
                      ghfs,
                      operationObject.getSrcResource(),
                      loggedResources.stream().map(Pair::getLeft).collect(toList()));
                } else {
                  logger.atInfo().log(
                      "Repairing FS after %s rename operation"
                          + " (deleting destination (%s) and renaming (%s -> %s)).",
                      operation.getPath(),
                      operationObject.getDstResource(),
                      operationObject.getSrcResource(),
                      operationObject.getDstResource());
                  deleteResource(
                      ghfs,
                      operationObject.getDstResource(),
                      loggedResources.stream().map(Pair::getRight).collect(toList()));
                  gcsFs
                      .getGcs()
                      .copy(
                          bucketName,
                          loggedResources.stream()
                              .map(
                                  p ->
                                      StorageResourceId.fromObjectName(p.getLeft()).getObjectName())
                              .collect(toList()),
                          bucketName,
                          loggedResources.stream()
                              .map(
                                  p ->
                                      StorageResourceId.fromObjectName(p.getRight())
                                          .getObjectName())
                              .collect(toList()));
                  deleteResource(
                      ghfs,
                      operationObject.getSrcResource(),
                      loggedResources.stream().map(Pair::getLeft).collect(toList()));
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
    return 0;
  }

  private void deleteResource(
      GoogleHadoopFileSystem ghfs, String resource, List<String> loggedResources)
      throws IOException {
    Path lockedResource = new Path(resource);
    Set<String> allObjects =
        Arrays.stream(ghfs.listStatus(lockedResource))
            .map(s -> s.getPath().toString())
            .collect(toSet());
    List<StorageResourceId> objectsToDelete = new ArrayList<>(loggedResources.size());
    for (String loggedObject : loggedResources) {
      if (allObjects.contains(loggedObject)) {
        objectsToDelete.add(StorageResourceId.fromObjectName(loggedObject));
      }
    }
    GoogleCloudStorage gcs = ghfs.getGcsFs().getGcs();
    gcs.deleteObjects(objectsToDelete);

    // delete directory if empty
    allObjects.removeAll(loggedResources);
    if (allObjects.isEmpty() && ghfs.exists(lockedResource)) {
      ghfs.delete(lockedResource, /* recursive= */ false);
    }
  }

  private boolean isLockExpired(
      Configuration conf, Instant lockInstant, Instant expirationInstant) {
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

  private static <T> List<T> getOperationLog(
      GoogleHadoopFileSystem ghfs, FileStatus operation, Function<String, T> logRecordFn)
      throws IOException {
    List<T> log = new ArrayList<>();
    Path operationLog = new Path(operation.getPath().toString().replace(".lock", ".log"));
    try (BufferedReader in = new BufferedReader(new InputStreamReader(ghfs.open(operationLog)))) {
      String line;
      while ((line = in.readLine()) != null) {
        log.add(logRecordFn.apply(line));
      }
    }
    return log;
  }

  private static String getOperationId(FileStatus operation) {
    String[] fileParts = operation.getPath().toString().split("_");
    return fileParts[fileParts.length - 1].split("\\.")[0];
  }
}
