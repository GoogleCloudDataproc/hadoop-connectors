/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class adds a caching layer around a GoogleCloudStorage instance, caching calls that create,
 * update, remove, and query for GoogleCloudStorageItemInfo. Those cached copies are returned when
 * requesting data through {@link GoogleCloudStorage#getItemInfo(StorageResourceId)} and {@link
 * GoogleCloudStorage#getItemInfo(StorageResourceId)}. This provides faster access to recently
 * queried data in the scope of this instance. Because the data is cached, modifications made
 * outside of this instance may not be immediately reflected.
 */
public class PerformanceCachingGoogleCloudStorage extends ForwardingGoogleCloudStorage {

    /** Cache to hold item info and manage invalidation. */
    private final PrefixMappedItemCache cache;

    /**
     * Creates a wrapper around a GoogleCloudStorage instance, caching calls that create, update,
     * remove, and query for GoogleCloudStorageItemInfo. Those cached copies are returned when
     * requesting data through {@link GoogleCloudStorage#getItemInfo(StorageResourceId)} and {@link
     * GoogleCloudStorage#getItemInfo(StorageResourceId)}. This provides faster access to recently
     * queried data in the scope of this instance. Because the data is cached, modifications made
     * outside of this instance may not be immediately reflected.
     *
     * @param delegate the {@link GoogleCloudStorage} instance to wrap and delegate calls to.
     * @param options the options to configure this cache with.
     */
    public PerformanceCachingGoogleCloudStorage(
            GoogleCloudStorage delegate, PerformanceCachingGoogleCloudStorageOptions options) {
        this(delegate, createCache(options));
    }

    @VisibleForTesting
    PerformanceCachingGoogleCloudStorage(GoogleCloudStorage delegate, PrefixMappedItemCache cache) {
        super(delegate);
        this.cache = cache;
    }

    private static PrefixMappedItemCache createCache(
            PerformanceCachingGoogleCloudStorageOptions options) {
        return new PrefixMappedItemCache(Duration.ofMillis(options.getMaxEntryAgeMillis()));
    }

    @Override
    public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
            throws IOException {
        // If the item exists in cache upon creation, remove it from cache so that later getItemInfo
        // will pull the most updated item info.
        if (cache.getItem(resourceId) != null) {
            cache.removeItem(resourceId);
        }

        return super.create(resourceId, options);
    }

    @Override
    public void deleteBuckets(List<String> bucketNames) throws IOException {
        super.deleteBuckets(bucketNames);

        // Remove objects that reside in deleted buckets.
        for (String bucket : bucketNames) {
            cache.invalidateBucket(bucket);
        }
    }

    @Override
    public void deleteObjects(List<StorageResourceId> resourceIds) throws IOException {
        super.deleteObjects(resourceIds);

        // Remove the deleted objects from cache.
        for (StorageResourceId resourceId : resourceIds) {
            cache.removeItem(resourceId);
        }
    }

    @Override
    public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
        List<GoogleCloudStorageItemInfo> result = super.listBucketInfo();

        // Add the results to the cache.
        for (GoogleCloudStorageItemInfo item : result) {
            cache.putItem(item);
        }

        return result;
    }

    /** This function may return cached copies of {@link GoogleCloudStorageItemInfo}. */
    @Override
    public List<GoogleCloudStorageItemInfo> listObjectInfo(
            String bucketName, String objectNamePrefix, ListObjectOptions listOptions)
            throws IOException {
        listOptions = getListObjectOptionsWithAllFields(listOptions);
        if (listOptions.getMaxResults() == 1 && listOptions.isIncludePrefix()) {
            GoogleCloudStorageItemInfo item =
                    cache.getItem(new StorageResourceId(bucketName, objectNamePrefix));
            if (item != null) {
                return ImmutableList.of(item);
            }
        }
        List<GoogleCloudStorageItemInfo> result =
                super.listObjectInfo(bucketName, objectNamePrefix, listOptions);
        for (GoogleCloudStorageItemInfo item : result) {
            cache.putItem(item);
        }

        return result;
    }

    @Override
    public ListPage<GoogleCloudStorageItemInfo> listObjectInfoPage(
            String bucketName,
            String objectNamePrefix,
            ListObjectOptions listOptions,
            String pageToken)
            throws IOException {
        listOptions = getListObjectOptionsWithAllFields(listOptions);
        ListPage<GoogleCloudStorageItemInfo> result =
                super.listObjectInfoPage(bucketName, objectNamePrefix, listOptions, pageToken);
        for (GoogleCloudStorageItemInfo item : result.getItems()) {
            cache.putItem(item);
        }
        return result;
    }

    /** This function may return cached copies of GoogleCloudStorageItemInfo. */
    @Override
    public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId) throws IOException {
        // Get the item from cache.
        GoogleCloudStorageItemInfo item = cache.getItem(resourceId);
        if (item != null) {
            return item;
        }

        // If item is not in the cache but directory item is, then item does not exist.
        if (resourceId.isStorageObject()
                && !resourceId.isDirectory()
                && cache.getItem(resourceId.toDirectoryId()) != null) {
            return GoogleCloudStorageItemInfo.createNotFound(resourceId);
        }

        // If item is not in the cache but directory item is in it, then item does not exist.
        if (!resourceId.isDirectory() && cache.getItem(resourceId.toDirectoryId()) != null) {
            return GoogleCloudStorageItemInfo.createNotFound(resourceId);
        }

        // If it wasn't in the cache and wasn't cached in directory list request
        // then request and cache it directly.
        item = super.getItemInfo(resourceId);
        cache.putItem(item);
        return item;
    }

    /** This function may return cached copies of GoogleCloudStorageItemInfo. */
    @Override
    public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
            throws IOException {
        List<GoogleCloudStorageItemInfo> result = new ArrayList<>(resourceIds.size());
        List<StorageResourceId> request = new ArrayList<>(resourceIds.size());

        // Populate the result list with items in the cache, and the request list with resources
        // that
        // still need to be resolved. Null items are added to the result list to preserve ordering.
        for (StorageResourceId resourceId : resourceIds) {
            GoogleCloudStorageItemInfo item = cache.getItem(resourceId);
            if (item == null) {
                request.add(resourceId);
            }
            result.add(item);
        }

        // Resolve all the resources which were not cached, cache them, and add them to the result
        // list.
        // Null entries in the result list are replaced by the fresh entries from the underlying
        // GoogleCloudStorage.
        if (!request.isEmpty()) {
            List<GoogleCloudStorageItemInfo> response = super.getItemInfos(request);
            Iterator<GoogleCloudStorageItemInfo> responseIterator = response.iterator();

            // Iterate through the result set, replacing the null entries added previously with
            // entries
            // from the response.
            for (int i = 0; i < result.size() && responseIterator.hasNext(); i++) {
                if (result.get(i) == null) {
                    GoogleCloudStorageItemInfo item = responseIterator.next();
                    cache.putItem(item);
                    result.set(i, item);
                }
            }
        }

        return result;
    }

    @Override
    public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
            throws IOException {
        List<GoogleCloudStorageItemInfo> result = super.updateItems(itemInfoList);

        // Update the cache with the returned items. This overwrites the originals as the
        // StorageResourceIds of the items do not change in an update.
        for (GoogleCloudStorageItemInfo item : result) {
            cache.putItem(item);
        }

        return result;
    }

    @Override
    public GoogleCloudStorageItemInfo composeObjects(
            List<StorageResourceId> sources,
            StorageResourceId destination,
            CreateObjectOptions options)
            throws IOException {
        GoogleCloudStorageItemInfo item = super.composeObjects(sources, destination, options);

        // Cache the composed object.
        cache.putItem(item);

        return item;
    }

    @Override
    public void close() {
        super.close();

        // Respect close and empty the cache.
        cache.invalidateAll();
    }

    @VisibleForTesting
    public void invalidateCache() {
        cache.invalidateAll();
    }

    // Resets requested object fields in list request to return all support object fields because we
    // initialize cache with objects returned in list response and they can be retrieve for non-list
    // request responses that need access to any supported fields
    private static ListObjectOptions getListObjectOptionsWithAllFields(
            ListObjectOptions listOptions) {
        return GoogleCloudStorageImpl.OBJECT_FIELDS.equals(listOptions.getFields())
                ? listOptions
                : listOptions.toBuilder().setFields(GoogleCloudStorageImpl.OBJECT_FIELDS).build();
    }
}
