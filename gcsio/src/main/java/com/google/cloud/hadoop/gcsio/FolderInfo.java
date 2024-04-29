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

package com.google.cloud.hadoop.gcsio;

import com.google.api.services.storage.model.Folder;

/** Contains information about a Folder resource */
public class FolderInfo {

  // Folder bucket
  private final String bucket;

  // Folder name
  private final String folderName;

  /**
   * Constructs an instance of FolderInfo.
   *
   * @param folder Information about the underlying folder.
   */
  public FolderInfo(Folder folder) {
    this.bucket = folder.getBucket();

    // Construct the path once.
    this.folderName = folder.getName();
  }

  public FolderInfo(String bucket, String folderName) {
    this.bucket = bucket;
    this.folderName = folderName;
  }

  /** Gets the path of this file or directory. */
  public String getBucket() {
    return this.bucket;
  }

  /** Returns the folder name, ie path excluding the bucket name */
  public String getFolderName() {
    return this.folderName;
  }

  public boolean isBucket() {
    return this.bucket != null && this.folderName == null;
  }

  /**
   * Returns the parent folder name if exists, else returns null
   *
   * @return parent folderName
   */
  public String getParentFolderName() {
    int lastIndex = this.folderName.lastIndexOf('/', this.folderName.length() - 2);
    return this.folderName.substring(0, lastIndex + 1);
  }

  /** Gets string representation of this instance. */
  public String toString() {
    return getBucket() + "/" + getFolderName();
  }
}
