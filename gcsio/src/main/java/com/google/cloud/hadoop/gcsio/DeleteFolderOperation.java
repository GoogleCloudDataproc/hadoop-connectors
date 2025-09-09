/*
 * Copyright 2024 Google Inc. All Rights Reserved.
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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ErrorTypeExtractor;
import com.google.cloud.hadoop.util.ErrorTypeExtractor.ErrorType;
import com.google.cloud.hadoop.util.GoogleCloudStorageEventBus;
import com.google.cloud.hadoop.util.GrpcErrorTypeExtractor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.FutureCallback;
import com.google.storage.control.v2.DeleteFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

@VisibleForTesting
class DeleteFolderOperation {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Maximum number of times to retry deletes in the case of precondition failures.
  private static final int MAXIMUM_PRECONDITION_FAILURES_IN_DELETE = 4;
  private static final ApiErrorExtractor errorExtractor = ApiErrorExtractor.INSTANCE;

  // Error extractor to map APi exception to meaningful ErrorTypes.
  private static final ErrorTypeExtractor errorTypeExtractor = GrpcErrorTypeExtractor.INSTANCE;
  private final GoogleCloudStorageOptions storageOptions;
  private final KeySetView<IOException, Boolean> allExceptions;
  private final List<FolderInfo> folders;
  private final BatchExecutor batchExecutor;
  private final StorageControlClient storageControlClient;
  private final BlockingQueue<FolderInfo> folderDeleteBlockingQueue;
  private final ConcurrentHashMap<String, Long> countOfChildren;

  DeleteFolderOperation(
      List<FolderInfo> folders,
      GoogleCloudStorageOptions storageOptions,
      StorageControlClient storageControlClient) {
    this.folders = folders;
    this.storageOptions = storageOptions;
    this.storageControlClient = storageControlClient;
    this.folderDeleteBlockingQueue = new LinkedBlockingQueue<>(folders.size());

    // threads for parallel delete of folder resources
    this.batchExecutor = new BatchExecutor(this.storageOptions.getBatchThreads());

    // Gather exceptions to wrap in a composite exception at the end.
    this.allExceptions = ConcurrentHashMap.newKeySet();

    // Map to store number of children for each parent object
    this.countOfChildren = new ConcurrentHashMap<>();
  }

  /** Helper function that performs the deletion process for folder resources */
  public void performDeleteOperation() throws IOException {
    int foldersRemaining = folders.size();
    computeChildrenForFolderResource();

    try {
      while (foldersRemaining != 0 && encounteredNoExceptions()) {
        FolderInfo folderToDelete = getElementFromBlockingQueue();
        if (folderToDelete != null) {
          // Queue the deletion request.
          queueSingleFolderDelete(folderToDelete, /* attempt */ 1);
          foldersRemaining--;
        } else if (batchExecutor.isIdle()) {
          // Throwing an IllegalStateException here because some folders remained undeleted
          // even though threads are waiting idle.
          throw new IllegalStateException(
              String.format(
                  "Deletion stalled: No active threads, but %d folders remain.",
                  countOfChildren.size()));
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(
          String.format(
              "Received InterruptedException while deletion of folder resource : %s",
              e.getMessage()),
          e);
    } catch (IllegalStateException e) {
      throw new IOException(
          String.format(
              "Received IllegalStateException while deletion of folder resource : %s",
              e.getMessage()),
          e);
    } finally {
      batchExecutorShutdown();
    }
  }

  /** Shutting down batch executor and flushing any remaining requests */
  private void batchExecutorShutdown() {
    try {
      batchExecutor.shutdown();
    } catch (IOException e) {
      addException(
          new IOException(
              String.format("Error in shutting down batch executor : %s", e.getMessage())));
    }
  }

  public boolean encounteredNoExceptions() {
    return allExceptions.isEmpty();
  }

  public KeySetView<IOException, Boolean> getAllExceptions() {
    return allExceptions;
  }

  /** Gets the head from the blocking queue */
  private FolderInfo getElementFromBlockingQueue() throws InterruptedException {
    try {
      return folderDeleteBlockingQueue.poll(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      logger.atSevere().log(
          "Encountered exception while getting an element from queue in HN enabled bucket : %s", e);
      throw e;
    }
  }

  /** Adding to batch executor's queue */
  public void addToBatchExecutorQueue(Callable callable, FutureCallback callback) {
    batchExecutor.queue(callable, callback);
  }

  /** Computes the number of children for each folder resource */
  private void computeChildrenForFolderResource() {
    for (FolderInfo currentFolder : folders) {
      if (!countOfChildren.containsKey(currentFolder.getFolderName())) {
        countOfChildren.put(currentFolder.getFolderName(), 0L);
      }

      String parentFolder = currentFolder.getParentFolderName();
      if (!Strings.isNullOrEmpty(parentFolder)) {
        countOfChildren.merge(parentFolder, 1L, (oldValue, newValue) -> oldValue + newValue);
      }
    }
    // Add leaf folders to blocking queue
    for (FolderInfo currentFolder : folders) {
      if (countOfChildren.get(currentFolder.getFolderName()) == 0L) {
        addFolderResourceInBlockingQueue(currentFolder);
      }
    }
  }

  /**
   * Helper function to add the parent of successfully deleted folder resource into the blocking
   * queue
   *
   * @param folderResource of the folder that is now deleted
   */
  protected synchronized void successfullDeletionOfFolderResource(FolderInfo folderResource) {
    // remove the folderResource from list of map
    countOfChildren.remove(folderResource.getFolderName());

    String parentFolder = folderResource.getParentFolderName();
    if (countOfChildren.containsKey(parentFolder)) {

      // update the parent's count of children
      countOfChildren.replace(parentFolder, countOfChildren.get(parentFolder) - 1);

      // if the parent folder is now empty, append in the queue
      if (countOfChildren.get(parentFolder) == 0) {
        addFolderResourceInBlockingQueue(
            new FolderInfo(
                FolderInfo.createFolderInfoObject(folderResource.getBucket(), parentFolder)));
      }
    }
  }

  /** Helper function to delete a single folder resource */
  protected void queueSingleFolderDelete(@Nonnull final FolderInfo folder, final int attempt) {
    final String bucketName = folder.getBucket();
    final String folderName = folder.getFolderName();
    checkArgument(
        !Strings.isNullOrEmpty(bucketName),
        String.format("No bucket for folder resource %s", bucketName));
    checkArgument(
        !Strings.isNullOrEmpty(folderName),
        String.format("No folder path for folder resource %s", folderName));

    addToBatchExecutorQueue(
        new DeleteFolderRequestCallable(folder, storageControlClient),
        getDeletionCallback(folder, allExceptions, attempt));
  }

  /**
   * Helper function to add folderResource to blocking queue
   *
   * @param folderResource
   */
  private void addFolderResourceInBlockingQueue(FolderInfo folderResource) {
    folderDeleteBlockingQueue.add(folderResource);
  }

  /** Helper to create a callback for a particular deletion request for folder. */
  private FutureCallback getDeletionCallback(
      final FolderInfo resourceId,
      final KeySetView<IOException, Boolean> allExceptions,
      final int attempt) {
    return new FutureCallback<Void>() {
      @Override
      public void onSuccess(Void result) {
        logger.atFiner().log("Successfully deleted folder %s", resourceId.toString());
        successfullDeletionOfFolderResource(resourceId);
      }

      @Override
      public void onFailure(Throwable throwable) {
        if (isErrorType(throwable, ErrorType.NOT_FOUND)) {
          // Ignore item-not-found errors. We do not have to delete what we cannot find.
          // This
          // error typically shows up when we make a request to delete something and the
          // server
          // receives the request but we get a retry-able error before we get a response.
          // During a retry, we no longer find the item because the server had deleted
          // it already.
          logger.atFiner().log(
              "Delete folder '%s' not found: %s", resourceId, throwable.getMessage());
          successfullDeletionOfFolderResource(resourceId);
        } else if (isErrorType(throwable, ErrorType.FAILED_PRECONDITION)
            && attempt <= MAXIMUM_PRECONDITION_FAILURES_IN_DELETE) {
          logger.atInfo().log(
              "Precondition not met while deleting '%s'. Attempt %s." + " Retrying:%s",
              resourceId, attempt, throwable);
          queueSingleFolderDelete(resourceId, attempt + 1);
        } else {
          GoogleCloudStorageEventBus.postOnException();
          addException(
              new IOException(
                  String.format("Error deleting '%s', stage 2", resourceId), throwable));
        }
      }
    };
  }

  private boolean isErrorType(Throwable throwable, ErrorType errorType) {
    return throwable instanceof Exception
        && (errorTypeExtractor.getErrorType((Exception) throwable) == errorType);
  }

  private synchronized void addException(IOException e) {
    allExceptions.add(e);
  }

  /* Callable class specifically for deletion of folder resource */
  private class DeleteFolderRequestCallable implements Callable<Void> {
    private StorageControlClient storageControlClient;
    private DeleteFolderRequest deleteFolderRequest;

    @Override
    public Void call() {
      storageControlClient.deleteFolder(deleteFolderRequest);
      return null;
    }

    DeleteFolderRequestCallable(FolderInfo folder, StorageControlClient storageControlClient) {
      checkArgument(storageControlClient != null, "StorageControlClient cannot be null");
      this.storageControlClient = storageControlClient;
      this.deleteFolderRequest =
          DeleteFolderRequest.newBuilder().setName(folder.toString()).build();
    }
  }
}
