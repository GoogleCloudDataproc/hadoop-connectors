/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.flogger.LazyArgs.lazy;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.api.client.util.ExponentialBackOff;
import com.google.common.base.MoreObjects;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.Gson;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class GcsAtomicOperations {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final String LOCK_DIRECTORY = "_lock/";

  private static final Gson GSON = new Gson();

  public static final String LOCK_FILE = "all.lock";
  public static final String LOCK_PATH = LOCK_DIRECTORY + LOCK_FILE;

  private static final String LOCK_METADATA_KEY = "lock";
  private static final int MAX_LOCKS_COUNT = 20;

  private final GoogleCloudStorageImpl gcs;

  public GcsAtomicOperations(GoogleCloudStorage gcs) {
    this.gcs = gcs instanceof GoogleCloudStorageImpl ? (GoogleCloudStorageImpl) gcs : null;
  }

  public Set<Operation> getLockedOperations(String bucketName) throws IOException {
    long startMs = System.currentTimeMillis();
    logger.atFine().log("getLockedOperations(%s)", bucketName);
    StorageResourceId lockId = getLockId(bucketName);
    GoogleCloudStorageItemInfo lockInfo = gcs.getItemInfo(lockId);
    Set<Operation> operations =
        !lockInfo.exists()
                || lockInfo.getMetaGeneration() == 0
                || lockInfo.getMetadata().get(LOCK_METADATA_KEY) == null
            ? new HashSet<>()
            : getLockRecords(lockInfo).getOperations();
    logger.atFine().log(
        "[%dms] lockPaths(%s): %s", System.currentTimeMillis() - startMs, bucketName, operations);
    return operations;
  }

  public void lockOperation(String bucketName, String operationId, long lockEpochSeconds)
      throws IOException {
    long startMs = System.currentTimeMillis();
    logger.atFine().log("lockOperation(%s, %d)", operationId, lockEpochSeconds);
    modifyLock(this::updateLockEpochSeconds, bucketName, operationId, lockEpochSeconds);
    logger.atFine().log(
        "[%dms] lockOperation(%s, %s)",
        System.currentTimeMillis() - startMs, operationId, lockEpochSeconds);
  }

  public void lockPaths(String operationId, StorageResourceId... resources) throws IOException {
    long startMs = System.currentTimeMillis();
    logger.atFine().log("lockPaths(%s, %s)", operationId, lazy(() -> Arrays.toString(resources)));
    Set<String> objects = validateResources(resources);
    String bucketName = resources[0].getBucketName();
    modifyLock(this::addLockRecords, bucketName, operationId, objects);
    logger.atFine().log(
        "[%dms] lockPaths(%s, %s)",
        System.currentTimeMillis() - startMs, operationId, lazy(() -> Arrays.toString(resources)));
  }

  private Set<String> validateResources(StorageResourceId[] resources) {
    checkNotNull(resources, "resources should not be null");
    checkArgument(resources.length > 0, "resources should not be empty");
    String bucketName = resources[0].getBucketName();
    checkState(
        Arrays.stream(resources).allMatch(r -> r.getBucketName().equals(bucketName)),
        "All resources should be in the same bucket");

    return Arrays.stream(resources).map(StorageResourceId::getObjectName).collect(toImmutableSet());
  }

  public void unlockPaths(String operationId, StorageResourceId... resources) throws IOException {
    long startMs = System.currentTimeMillis();
    logger.atFine().log("unlockPaths(%s, %s)", operationId, lazy(() -> Arrays.toString(resources)));
    Set<String> objects = validateResources(resources);
    String bucketName = resources[0].getBucketName();
    modifyLock(this::removeLockRecords, bucketName, operationId, objects);
    logger.atFine().log(
        "[%dms] unlockPaths(%s, %s)",
        System.currentTimeMillis() - startMs, operationId, lazy(() -> Arrays.toString(resources)));
  }

  private <T> void modifyLock(
      LockRecordsModificationFunction<Boolean, OperationLocks, String, T> modificationFn,
      String bucketName,
      String operationId,
      T modificationFnParam)
      throws IOException {
    long startMs = System.currentTimeMillis();
    StorageResourceId lockId = getLockId(bucketName);

    ExponentialBackOff backOff =
        new ExponentialBackOff.Builder()
            .setInitialIntervalMillis(100)
            .setMultiplier(1.2)
            .setMaxIntervalMillis(30_000)
            .setMaxElapsedTimeMillis(Integer.MAX_VALUE)
            .build();

    do {
      try {
        GoogleCloudStorageItemInfo lockInfo = gcs.getItemInfo(lockId);
        if (!lockInfo.exists()) {
          gcs.createEmptyObject(lockId, new CreateObjectOptions(false));
          lockInfo = gcs.getItemInfo(lockId);
        }
        OperationLocks lockRecords =
            lockInfo.getMetaGeneration() == 0
                    || lockInfo.getMetadata().get(LOCK_METADATA_KEY) == null
                ? new OperationLocks().setFormatVersion(OperationLocks.FORMAT_VERSION)
                : getLockRecords(lockInfo);

        if (!modificationFn.apply(lockRecords, operationId, modificationFnParam)) {
          logger.atInfo().atMostEvery(5, SECONDS).log(
              "Failed to update %s entries in %s file: resources could be locked. Re-trying.",
              modificationFnParam, lockRecords.getOperations().size(), lockId);
          sleepUninterruptibly(backOff.nextBackOffMillis(), MILLISECONDS);
          continue;
        }

        // Unlocked all objects - delete lock object
        if (lockRecords.getOperations().isEmpty()) {
          gcs.deleteObject(lockInfo.getResourceId(), lockInfo.getMetaGeneration());
          break;
        }

        if (lockRecords.getOperations().size() > MAX_LOCKS_COUNT) {
          logger.atInfo().atMostEvery(5, SECONDS).log(
              "Skipping lock entries update in %s file: too many (%d) locked resources. Re-trying.",
              lockRecords.getOperations().size(), lockId);
          sleepUninterruptibly(backOff.nextBackOffMillis(), MILLISECONDS);
          continue;
        }

        String lockContent = GSON.toJson(lockRecords, OperationLocks.class);
        Map<String, byte[]> metadata = new HashMap<>(lockInfo.getMetadata());
        metadata.put(LOCK_METADATA_KEY, lockContent.getBytes(UTF_8));

        gcs.updateMetadata(lockInfo, metadata);

        logger.atFine().log(
            "Updated lock file in %dms for %s operation with %s parameter",
            System.currentTimeMillis() - startMs, operationId, lazy(modificationFnParam::toString));
        break;
      } catch (IOException e) {
        // continue after sleep if update failed due to file generation mismatch or other
        // IOException
        if (e.getMessage().contains("conditionNotMet")) {
          logger.atInfo().atMostEvery(5, SECONDS).log(
              "Failed to update entries (conditionNotMet) in %s file for operation %s. Re-trying.",
              lockId, operationId);
        } else {
          logger.atWarning().withCause(e).log(
              "Failed to modify lock for %s operation with %s parameter, retrying.",
              operationId, modificationFnParam);
        }
        sleepUninterruptibly(backOff.nextBackOffMillis(), MILLISECONDS);
      }
    } while (true);
  }

  private StorageResourceId getLockId(String bucketName) {
    String lockObject = "gs://" + bucketName + "/" + LOCK_PATH;
    return StorageResourceId.fromObjectName(lockObject);
  }

  private OperationLocks getLockRecords(GoogleCloudStorageItemInfo lockInfo) throws IOException {
    String lockContent = new String(lockInfo.getMetadata().get(LOCK_METADATA_KEY), UTF_8);
    OperationLocks lockRecords = GSON.fromJson(lockContent, OperationLocks.class);
    checkState(
        lockRecords.getFormatVersion() == OperationLocks.FORMAT_VERSION,
        "Unsupported metadata format: expected %d, but was %d",
        lockRecords.getFormatVersion(),
        OperationLocks.FORMAT_VERSION);
    return lockRecords;
  }

  private boolean updateLockEpochSeconds(
      OperationLocks lockRecords, String operationId, long lockEpochSeconds) {
    Optional<Operation> operationOptional =
        lockRecords.getOperations().stream()
            .filter(o -> o.getOperationId().equals(operationId))
            .findAny();
    checkState(operationOptional.isPresent(), "operation %s not found", operationId);
    Operation operation = operationOptional.get();
    checkState(
        lockEpochSeconds == operation.getLockEpochSeconds(),
        "operation %s should have %s lock epoch, but was %s",
        operationId,
        lockEpochSeconds,
        operation.getLockEpochSeconds());
    operation.setLockEpochSeconds(Instant.now().getEpochSecond());
    return true;
  }

  private boolean addLockRecords(
      OperationLocks lockRecords, String operationId, Set<String> resourcesToAdd) {
    // TODO: optimize to match more efficiently
    if (lockRecords.getOperations().stream()
        .flatMap(operation -> operation.getResources().stream())
        .anyMatch(
            resource -> {
              for (String resourceToAdd : resourcesToAdd) {
                if (resourceToAdd.equals(resource)
                    || isChildObject(resource, resourceToAdd)
                    || isChildObject(resourceToAdd, resource)) {
                  return true;
                }
              }
              return false;
            })) {
      return false;
    }

    long lockEpochSeconds = Instant.now().getEpochSecond();
    lockRecords
        .getOperations()
        .add(
            new Operation()
                .setOperationId(operationId)
                .setResources(resourcesToAdd)
                .setLockEpochSeconds(lockEpochSeconds));

    return true;
  }

  private boolean isChildObject(String parent, String child) {
    return parent.startsWith(child.endsWith(PATH_DELIMITER) ? child : child + PATH_DELIMITER);
  }

  private boolean removeLockRecords(
      OperationLocks lockRecords, String operationId, Set<String> resourcesToRemove) {
    List<Operation> operationLocksToRemove =
        lockRecords.getOperations().stream()
            .filter(o -> o.getResources().stream().anyMatch(resourcesToRemove::contains))
            .collect(Collectors.toList());
    checkState(
        operationLocksToRemove.size() == 1
            && operationLocksToRemove.get(0).getOperationId().equals(operationId),
        "All resources %s should belong to %s operation, but was %s",
        resourcesToRemove.size(),
        operationLocksToRemove.size());
    Operation operationToRemove = operationLocksToRemove.get(0);
    checkState(
        operationToRemove.getResources().equals(resourcesToRemove),
        "All of %s resources should be locked by operation, but was locked only %s resources",
        resourcesToRemove,
        operationToRemove.getResources());
    checkState(
        lockRecords.getOperations().remove(operationToRemove),
        "operation %s was not removed",
        operationToRemove);
    return true;
  }

  @FunctionalInterface
  private interface LockRecordsModificationFunction<T, T1, T2, T3> {
    T apply(T1 p1, T2 p2, T3 p3);
  }

  public static class OperationLocks {
    public static final long FORMAT_VERSION = 1;

    private long formatVersion = -1;
    private Set<Operation> operations =
        new TreeSet<>(Comparator.comparing(Operation::getOperationId));

    public long getFormatVersion() {
      return formatVersion;
    }

    public OperationLocks setFormatVersion(long formatVersion) {
      this.formatVersion = formatVersion;
      return this;
    }

    public Set<Operation> getOperations() {
      return operations;
    }

    public OperationLocks setOperations(Set<Operation> operations) {
      this.operations = new TreeSet<>(Comparator.comparing(Operation::getOperationId));
      this.operations.addAll(operations);
      return this;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("formatVersion", formatVersion)
          .add("operations", operations)
          .toString();
    }
  }

  public static class Operation {
    private String operationId;
    private long lockEpochSeconds;
    private Set<String> resources = new TreeSet<>();

    public String getOperationId() {
      return operationId;
    }

    public Operation setOperationId(String operationId) {
      this.operationId = operationId;
      return this;
    }

    public long getLockEpochSeconds() {
      return lockEpochSeconds;
    }

    public Operation setLockEpochSeconds(long lockEpochSeconds) {
      this.lockEpochSeconds = lockEpochSeconds;
      return this;
    }

    public Set<String> getResources() {
      return resources;
    }

    public Operation setResources(Set<String> resources) {
      this.resources = new TreeSet<>(resources);
      return this;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("operationId", operationId)
          .add("lockEpochSeconds", lockEpochSeconds)
          .add("resources", resources)
          .toString();
    }
  }
}
