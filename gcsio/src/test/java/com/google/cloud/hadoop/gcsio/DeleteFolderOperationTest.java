package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.storage.control.v2.StorageControlClient;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import org.junit.Test;

public class DeleteFolderOperationTest {

  private static final String BUCKET_NAME = "foo-bucket";

  @Test
  public void checkDeletionOrderForHnBucketBalancedFolders() throws InterruptedException {
    String folderString = "test-folder-start/";
    List<FolderInfo> foldersToDelete = new LinkedList<>();

    addFolders(foldersToDelete, folderString);
    CustomDeleteFolderOperationTest deleteFolderOperation =
        new CustomDeleteFolderOperationTest(
            foldersToDelete, GoogleCloudStorageOptions.DEFAULT, null);

    List<FolderInfo> orderOfDeletion = deleteFolderOperation.getOrderOfDeletion();
    deleteFolderOperation.performDeleteOperation();
    assertThat(orderOfDeletion.size()).isEqualTo(foldersToDelete.size());

    // Map to store the index at which a folder was deleted
    HashMap<String, Integer> deletionOrder = new HashMap<>();
    for (int i = 0; i < orderOfDeletion.size(); i++) {
      deletionOrder.put(orderOfDeletion.get(i).getFolderName(), i);
    }

    for (int i = 0; i < orderOfDeletion.size(); i++) {
      FolderInfo curFolder = orderOfDeletion.get(i);
      String curFolderName = curFolder.getFolderName();
      String parentFolderName = curFolder.getParentFolderName();

      if (!Strings.isNullOrEmpty(parentFolderName)) {
        assertThat(deletionOrder.get(parentFolderName) > deletionOrder.get(curFolderName)).isTrue();
      }
    }
  }

  @Test
  public void checkDeletionOrderForHnBucketSkewedFolders() throws InterruptedException {
    String folderString = "test-folder-start/";
    List<FolderInfo> foldersToDelete = new LinkedList<>();

    for (int i = 0; i < 10; i++) {
      foldersToDelete.add(
          new FolderInfo(FolderInfo.createFolderInfoObject(BUCKET_NAME, folderString)));
      folderString += ("test-folder-" + i + "/");
    }

    CustomDeleteFolderOperationTest deleteFolderOperation =
        new CustomDeleteFolderOperationTest(
            foldersToDelete, GoogleCloudStorageOptions.DEFAULT, null);

    deleteFolderOperation.performDeleteOperation();
    List<FolderInfo> orderOfDeletion = deleteFolderOperation.getOrderOfDeletion();
    assertThat(orderOfDeletion.size()).isEqualTo(foldersToDelete.size());
    for (int i = 1; i < orderOfDeletion.size(); i++) {
      FolderInfo prev = orderOfDeletion.get(i - 1);
      FolderInfo cur = orderOfDeletion.get(i);
      assertThat(prev.getParentFolderName()).isEqualTo(cur.getFolderName());
    }
  }

  private void addFolders(List<FolderInfo> foldersToDelete, String curFolderName) {
    Random r = new Random();
    Queue<String> q = new ArrayDeque<>();
    q.add(curFolderName);

    while (!q.isEmpty()) {
      String top = q.poll();
      foldersToDelete.add(new FolderInfo(FolderInfo.createFolderInfoObject(BUCKET_NAME, top)));
      if (foldersToDelete.size() > 2000) return;

      for (int i = 0; i < 3; i++) {
        long nextFolderName = r.nextInt(100000);
        q.add(top + nextFolderName + "/");
      }
    }
  }

  /** Custom DeleteFolderOperation class to store order of folder deletion */
  private class CustomDeleteFolderOperationTest extends DeleteFolderOperation {

    /* Stores the order of deletion of folder resources*/
    private List<FolderInfo> orderOfDeletion;

    CustomDeleteFolderOperationTest(
        List<FolderInfo> folders,
        GoogleCloudStorageOptions storageOptions,
        StorageControlClient storageControlClient) {
      super(folders, storageOptions, storageControlClient);
      this.orderOfDeletion = new ArrayList<>(folders.size());
    }

    public List<FolderInfo> getOrderOfDeletion() {
      return orderOfDeletion;
    }

    public void queueSingleFolderDelete(final FolderInfo folder, final int attempt) {
      addToToBatchExecutorQueue(() -> null, getDeletionCallback(folder));
    }

    private synchronized void addToOrderOfDeletion(FolderInfo folderDeleted) {
      orderOfDeletion.add(folderDeleted);
    }

    private FutureCallback getDeletionCallback(final FolderInfo resourceId) {
      return new FutureCallback<Void>() {
        @Override
        public void onSuccess(Void result) {
          successfullDeletionOfFolderResource(resourceId);
          addToOrderOfDeletion(resourceId);
        }

        @Override
        public void onFailure(Throwable t) {
          // do nothing
        }
      };
    }
  }
}
