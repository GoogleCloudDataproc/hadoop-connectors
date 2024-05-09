package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions.createJsonResponseException;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.base.Strings;
import com.google.common.flogger.GoogleLogger;
import com.google.storage.control.v2.DeleteFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.Nonnull;

public class DeleteFolderOperation {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Maximum number of times to retry deletes in the case of precondition failures.
  private static final int MAXIMUM_PRECONDITION_FAILURES_IN_DELETE = 4;
  private ApiErrorExtractor errorExtractor = ApiErrorExtractor.INSTANCE;
  private GoogleCloudStorageOptions storageOptions;
  private final KeySetView<IOException, Boolean> innerExceptions;
  private List<FolderInfo> folders;
  private BatchExecutor batchExecutor;
  private StorageControlClient storageControlClient;
  private BlockingQueue<FolderInfo> folderDeleteBlockingQueue;
  private ConcurrentHashMap<String, Long> countOfChildren;

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
    this.innerExceptions = ConcurrentHashMap.newKeySet();

    // Map to store number of children for each parent object
    this.countOfChildren = new ConcurrentHashMap<>();
  }

  /** Helper function that performs the deletion process for folder resources */
  public void performDeleteOperation() throws IOException {
    int folderSize = this.folders.size();
    computeChildrenForFolderResource();

    // this will avoid infinite loop when all folders are deleted
    while (folderSize != 0 && isInnerExceptionEmpty()) {
      FolderInfo folderToDelete = getElementFromBlockingQueue();
      folderSize--;

      // Queue the deletion request
      queueSingleFolderDelete(folderToDelete, /* attempt */ 1);
    }
    // deleting any remaining resources
    this.batchExecutor.shutdown();
  }

  public boolean isInnerExceptionEmpty() {
    return this.innerExceptions.isEmpty();
  }

  public KeySetView<IOException, Boolean> getInnerExceptions() {
    return innerExceptions;
  }

  /** Gets the head from the blocking queue */
  public FolderInfo getElementFromBlockingQueue() {
    try {
      return this.folderDeleteBlockingQueue.take();
    } catch (InterruptedException e) {
      innerExceptions.add(
          new IOException(
              String.format(
                  "Error while getting folder resource from blocking queue : %s", e.getMessage())));
      return null;
    }
  }

  /** Computes the number of children for each folder resource */
  public void computeChildrenForFolderResource() {
    for (FolderInfo currentFolder : this.folders) {
      if (!this.countOfChildren.containsKey(currentFolder.getFolderName())) {
        this.countOfChildren.put(currentFolder.getFolderName(), 0L);
      }

      String parentFolder = currentFolder.getParentFolderName();
      if (!Strings.isNullOrEmpty(parentFolder)) {
        this.countOfChildren.merge(parentFolder, 1L, (oldValue, newValue) -> oldValue + newValue);
      }
    }
    // Add leaf folders to blocking queue
    for (FolderInfo currentFolder : this.folders) {
      if (this.countOfChildren.get(currentFolder.getFolderName()) == 0L) {
        addFolderResourceInBlockingQueue(currentFolder);
      }
    }
  }

  /**
   * Helper function to add folderResource to blocking queue
   *
   * @param folderResource
   */
  private void addFolderResourceInBlockingQueue(FolderInfo folderResource) {
    this.folderDeleteBlockingQueue.add(folderResource);
  }

  /**
   * Helper function to add the parent of successfully deleted folder resource into the blocking
   * queue
   *
   * @param folderResource of the folder that is now deleted
   */
  private void successfullDeletionOfFolderResource(FolderInfo folderResource) {
    // remove the folderResource from list of map
    this.countOfChildren.remove(folderResource.getFolderName());

    String parentFolder = folderResource.getParentFolderName();
    if (this.countOfChildren.containsKey(parentFolder)) {

      // update the parent's count of children
      this.countOfChildren.replace(parentFolder, this.countOfChildren.get(parentFolder) - 1);

      // if the parent folder is now empty, append in the queue
      if (this.countOfChildren.get(parentFolder) == 0) {
        addFolderResourceInBlockingQueue(
            new FolderInfo(
                FolderInfo.createFolderInfoObject(folderResource.getBucket(), parentFolder)));
      }
    }
  }

  /** Helper function to delete a single folder resource */
  private void queueSingleFolderDelete(@Nonnull final FolderInfo folder, final int attempt) {
    final String bucketName = folder.getBucket();
    final String folderName = folder.getFolderName();
    checkArgument(
        !Strings.isNullOrEmpty(bucketName),
        String.format("No bucket for folder resource %s", bucketName));
    checkArgument(
        !Strings.isNullOrEmpty(folderName),
        String.format("No folder path for folder resource %s", folderName));

    this.batchExecutor.queue(
        new DeleteFolderRequestBuilder(folder, this.storageControlClient),
        getDeletionCallback(folder, this.innerExceptions, attempt));
  }

  /** Helper to create a callback for a particular deletion request for folder. */
  private JsonBatchCallback<Void> getDeletionCallback(
      final FolderInfo resourceId,
      final KeySetView<IOException, Boolean> innerExceptions,
      final int attempt) {
    return new JsonBatchCallback<Void>() {

      @Override
      public void onSuccess(Void obj, HttpHeaders responseHeaders) {
        logger.atFiner().log("Successfully deleted folder %s", resourceId.toString());
        successfullDeletionOfFolderResource(resourceId);
      }

      @Override
      public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders)
          throws IOException {
        GoogleJsonResponseException cause = createJsonResponseException(jsonError, responseHeaders);
        if (errorExtractor.itemNotFound(cause)) {
          // Ignore item-not-found errors. We do not have to delete what we cannot find.
          // This
          // error typically shows up when we make a request to delete something and the
          // server
          // receives the request but we get a retry-able error before we get a response.
          // During a retry, we no longer find the item because the server had deleted
          // it already.
          logger.atFiner().log("Delete folder '%s' not found:%n%s", resourceId, jsonError);
          successfullDeletionOfFolderResource(resourceId);
        } else if (errorExtractor.preconditionNotMet(cause)
            && attempt <= MAXIMUM_PRECONDITION_FAILURES_IN_DELETE) {
          logger.atInfo().log(
              "Precondition not met while deleting '%s'. Attempt %s." + " Retrying:%n%s",
              resourceId, attempt, jsonError);
          queueSingleFolderDelete(resourceId, attempt + 1);
        } else {
          innerExceptions.add(
              new IOException(String.format("Error deleting '%s', stage 2", resourceId), cause));
        }
      }
    };
  }

  /* Callable class specifically for deletion of folder resource */
  private class DeleteFolderRequestBuilder implements Callable<Void> {
    private StorageControlClient storageControlClient;
    private DeleteFolderRequest deleteFolderRequest;

    @Override
    public Void call() {
      // deletion of folder takes place
      this.storageControlClient.deleteFolder(deleteFolderRequest);
      return null;
    }

    DeleteFolderRequestBuilder(FolderInfo folder, StorageControlClient storageControlClient) {
      checkArgument(storageControlClient != null, "StorageControlClient cannot be null");
      this.storageControlClient = storageControlClient;
      this.deleteFolderRequest =
          DeleteFolderRequest.newBuilder().setName(folder.toString()).build();
    }
  }

  /*Only for testing purposes. This bypasses the storageControlClient.deleteFolder call*/
  public void queueSingleFolderDelete(@Nonnull final FolderInfo folder) {
    this.batchExecutor.queue(
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            return null;
          }
        },
        getDeletionCallback(folder, this.innerExceptions, 1));
  }
}
