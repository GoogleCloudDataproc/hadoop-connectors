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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.Gson;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

public class GcsAtomicOperations {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final String LOCK_DIRECTORY = "_lock/";

  private static final Gson GSON = new Gson();
  static final String LOCK_FILE = "all.lock";
  static final String LOCK_PATH = LOCK_DIRECTORY + LOCK_FILE;
  private static final String LOCK_METADATA_KEY = "lock";
  private static final int MAX_LOCKS_COUNT = 20;

  // lock record mapping
  private static final int OPERATION_ID_INDEX = 0;
  private static final int LOCKED_PATH_INDEX = 1;

  private final GoogleCloudStorage gcs;

  public GcsAtomicOperations(GoogleCloudStorage gcs) {
    this.gcs = gcs;
  }

  public Map<String, Collection<String>> getLockedOperations(String bucketName) throws IOException {
    long startMs = System.currentTimeMillis();
    logger.atFine().log("getLockedOperations(%s)", bucketName);
    StorageResourceId lockId = getLockId(bucketName);
    GoogleCloudStorageItemInfo lockInfo = gcs.getItemInfo(lockId);
    Map<String, Collection<String>> operations =
        !lockInfo.exists()
                || lockInfo.getMetaGeneration() == 0
                || lockInfo.getMetadata().get(LOCK_METADATA_KEY) == null
            ? new HashMap<>()
            : getLockRecords(lockInfo).stream()
                .collect(
                    Multimaps.toMultimap(
                        r -> r[OPERATION_ID_INDEX],
                        r -> r[LOCKED_PATH_INDEX],
                        MultimapBuilder.hashKeys().arrayListValues()::build))
                .asMap();
    logger.atFine().log(
        "[%dms] lockPaths(%s): %s", System.currentTimeMillis() - startMs, bucketName, operations);
    return operations;
  }

  public boolean lockPaths(String operationId, StorageResourceId... resources) throws IOException {
    long startMs = System.currentTimeMillis();
    logger.atFine().log("lockPaths(%s, %s)", operationId, lazy(() -> Arrays.toString(resources)));
    boolean result = modifyLock(this::addLockRecords, operationId, resources);
    logger.atFine().log(
        "[%dms] lockPaths(%s, %s): %s",
        System.currentTimeMillis() - startMs,
        operationId,
        lazy(() -> Arrays.toString(resources)),
        result);
    return result;
  }

  public boolean unlockPaths(String operationId, StorageResourceId... resources)
      throws IOException {
    long startMs = System.currentTimeMillis();
    logger.atFine().log("unlockPaths(%s, %s)", operationId, lazy(() -> Arrays.toString(resources)));
    boolean result = modifyLock(this::removeLockRecords, operationId, resources);
    logger.atFine().log(
        "[%dms] unlockPaths(%s, %s): %s",
        System.currentTimeMillis() - startMs,
        operationId,
        lazy(() -> Arrays.toString(resources)),
        result);
    return result;
  }

  private boolean modifyLock(
      LockRecordsModificationFunction<Boolean, List<String[]>, String, Set<String>> modificationFn,
      String operationId,
      StorageResourceId... resources)
      throws IOException {
    long startMs = System.currentTimeMillis();

    checkNotNull(resources, "resourcesArray should not be null");
    checkArgument(resources.length > 0, "resourcesArray should not be empty");
    String bucketName = resources[0].getBucketName();
    checkState(
        Arrays.stream(resources).allMatch(r -> r.getBucketName().equals(bucketName)),
        "All resources should be in the same bucket");

    ImmutableSet<String> objects =
        Arrays.stream(resources).map(StorageResourceId::getObjectName).collect(toImmutableSet());

    StorageResourceId lockId = getLockId(bucketName);

    ExponentialBackOff backOff =
        new ExponentialBackOff.Builder()
            .setInitialIntervalMillis(100)
            .setMultiplier(1.2)
            .setMaxIntervalMillis(30_000)
            .setMaxElapsedTimeMillis(Integer.MAX_VALUE)
            .build();

    do {
      GoogleCloudStorageItemInfo lockInfo = gcs.getItemInfo(lockId);
      if (!lockInfo.exists()) {
        gcs.createEmptyObject(lockId, new CreateObjectOptions(false));
        lockInfo = gcs.getItemInfo(lockId);
      }
      List<String[]> lockRecords =
          lockInfo.getMetaGeneration() == 0 || lockInfo.getMetadata().get(LOCK_METADATA_KEY) == null
              ? new ArrayList<>()
              : getLockRecords(lockInfo);

      if (!modificationFn.apply(lockRecords, operationId, objects)) {
        sleepUninterruptibly(backOff.nextBackOffMillis(), MILLISECONDS);
        continue;
      }

      if (lockRecords.size() > MAX_LOCKS_COUNT) {
        logger.atInfo().atMostEvery(5, SECONDS).log(
            "Skipping lock entries update in %s file: too many (%d) locked resources. Re-trying.",
            lockRecords.size(), lockId);
        sleepUninterruptibly(backOff.nextBackOffMillis(), MILLISECONDS);
        continue;
      }

      String lockContent = GSON.toJson(lockRecords.toArray(new String[0][0]), String[][].class);
      Map<String, byte[]> metadata = new HashMap<>(lockInfo.getMetadata());
      metadata.put(LOCK_METADATA_KEY, lockContent.getBytes(UTF_8));

      try {
        ((GoogleCloudStorageImpl) gcs).updateMetadata(lockInfo, metadata);
      } catch (IOException e) {
        // continue after sleep if update failed due to file generation mismatch
        if (e.getMessage().contains("conditionNotMet")) {
          logger.atInfo().atMostEvery(5, SECONDS).log(
              "Failed to update entries in %s file. Re-trying.", lockId);
          sleepUninterruptibly(backOff.nextBackOffMillis(), MILLISECONDS);
          continue;
        }

        throw e;
      }

      logger.atFine().log(
          "updated lock file in %dms for %s client and %s resources",
          System.currentTimeMillis() - startMs,
          operationId,
          lazy(() -> Arrays.toString(resources)));
      return true;
    } while (true);
  }

  private StorageResourceId getLockId(String bucketName) {
    String lockObject = "gs://" + bucketName + "/" + LOCK_PATH;
    return StorageResourceId.fromObjectName(lockObject);
  }

  private List<String[]> getLockRecords(GoogleCloudStorageItemInfo lockInfo) throws IOException {
    String lockContent = new String(lockInfo.getMetadata().get(LOCK_METADATA_KEY), UTF_8);
    String[][] jsonArray = GSON.fromJson(lockContent, String[][].class);
    return new ArrayList<>(Arrays.asList(jsonArray));
  }

  private boolean addLockRecords(
      List<String[]> lockRecords, String operationId, Set<String> objectsToAdd) {
    // TODO: optimize to match more efficiently
    if (lockRecords.stream()
        .anyMatch(
            r -> {
              String lockedObject = r[LOCKED_PATH_INDEX];
              for (String objectToAdd : objectsToAdd) {
                if (objectToAdd.equals(lockedObject)
                    || isChildObject(lockedObject, objectToAdd)
                    || isChildObject(objectToAdd, lockedObject)) {
                  return true;
                }
              }
              return false;
            })) {
      return false;
    }

    String lockTime = Instant.now().toString();
    for (String object : objectsToAdd) {
      lockRecords.add(new String[] {operationId, object, lockTime});
    }

    lockRecords.sort(Comparator.comparing(r -> r[OPERATION_ID_INDEX]));

    return true;
  }

  private boolean isChildObject(String parent, String child) {
    return parent.startsWith(child.endsWith(PATH_DELIMITER) ? child : child + PATH_DELIMITER);
  }

  private boolean removeLockRecords(
      List<String[]> lockRecords, String operationId, Set<String> objectsToRemove) {
    int[] indexesToRemove =
        IntStream.range(0, lockRecords.size())
            .filter(i -> objectsToRemove.contains(lockRecords.get(i)[LOCKED_PATH_INDEX]))
            .peek(
                i ->
                    checkState(
                        operationId.equals(lockRecords.get(i)[OPERATION_ID_INDEX]),
                        "record %s should be locked by client %s",
                        Arrays.asList(lockRecords.get(i)),
                        operationId))
            .toArray();
    checkState(
        indexesToRemove.length == objectsToRemove.size(),
        "%s objects should be locked, but was %s",
        objectsToRemove.size(),
        indexesToRemove.length);

    for (int i = indexesToRemove.length - 1; i >= 0; i--) {
      lockRecords.remove(indexesToRemove[i]);
    }

    return true;
  }

  @FunctionalInterface
  private interface LockRecordsModificationFunction<T, T1, T2, T3> {
    T apply(T1 p1, T2 p2, T3 p3);
  }
}
