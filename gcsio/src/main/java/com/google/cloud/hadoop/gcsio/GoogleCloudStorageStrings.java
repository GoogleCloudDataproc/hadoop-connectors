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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

/** Contains helper methods for standardizing String-matching algorithms specific to GCS. */
public class GoogleCloudStorageStrings {
    /**
     * Helper that mimics the GCS API behavior for taking an existing objectName and checking if it
     * matches a user-supplied prefix with an optional directory "delimiter". If it matches, either
     * the full objectName will be returned unmodified, or the return value will be a String that is
     * a prefix of the objectName inclusive of the matching prefix but truncating any suffix past
     * the first appearance of the delimiter after the full prefix. The returned prefix includes the
     * delimiter String at which the objectName was truncated.
     *
     * @param objectNamePrefix The prefix that {@code objectName} must match to be returned in any
     *     form. May be null; then an objectName will always be returned, just possibly truncated.
     * @param objectName The name to attempt to match against the prefix and delimiter.
     * @param listOptions List object options to match list prefix.
     * @return A substring of objectName or the full unaltered objectName after applying GCS
     *     matching logic, or null if the supply objectName does not match the provided prefix.
     */
    public static String matchListPrefix(
            String objectNamePrefix, String objectName, ListObjectOptions listOptions) {
        checkArgument(
                !isNullOrEmpty(objectName),
                "matchListPrefix(%s, %s, %s): objectName must not be null or empty.",
                objectNamePrefix,
                objectName,
                listOptions);

        // The suffix that we'll use to check for the delimiter is just the whole name if no prefix
        // was supplied.
        String suffix = objectName;
        int suffixIndex = 0;
        if (objectNamePrefix != null) {
            // The underlying GCS API does return objectName when it equals the prefix, but our
            // GoogleCloudStorage wrapper filters this case if the objectName also ends with
            // PATH_DELIMITER.
            if (!objectName.startsWith(objectNamePrefix)
                    || (objectName.equals(objectNamePrefix)
                            && objectNamePrefix.endsWith(PATH_DELIMITER)
                            && !listOptions.isIncludePrefix())) {
                return null;
            }
            suffixIndex = objectNamePrefix.length();
            suffix = objectName.substring(suffixIndex);
        }

        String delimiter = listOptions.getDelimiter();
        return !isNullOrEmpty(delimiter) && suffix.contains(delimiter)
                // Return the full prefix and suffix up through first occurrence of delimiter after
                // the prefix, inclusive of the delimiter.
                ? objectName.substring(
                        0, objectName.indexOf(delimiter, suffixIndex) + delimiter.length())
                : objectName;
    }
}
