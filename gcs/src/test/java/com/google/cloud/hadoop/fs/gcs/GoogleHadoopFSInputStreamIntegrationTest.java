/*
 * Copyright 2019 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestBase.loadConfig;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class GoogleHadoopFSInputStreamIntegrationTest
        extends HadoopFileSystemTestBase {

    @ClassRule
    public static NotInheritableExternalResource storageResource =
            new NotInheritableExternalResource(GoogleHadoopFSInputStreamIntegrationTest.class) {
                /** Perform clean-up once after all tests are turn. */
                @Override
                public void before() throws Throwable {
                    GoogleHadoopFileSystem testInstance = new GoogleHadoopFileSystem();
                    ghfs = testInstance;
                    ghfsFileSystemDescriptor = testInstance;

                    // loadConfig needs ghfsHelper, which is normally created in
                    // postCreateInit. Create one here for it to use.
                    ghfsHelper = new HadoopFileSystemIntegrationHelper(ghfs, ghfsFileSystemDescriptor);

                    URI initUri = new URI("gs://" + ghfsHelper.getUniqueBucketName("init"));
                    ghfs.initialize(initUri, loadConfig());

                    if (GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE.get(
                            ghfs.getConf(), ghfs.getConf()::getBoolean)) {
                        testInstance.getGcsFs();
                    }

                    HadoopFileSystemTestBase.postCreateInit();
                }
            };


    @Before
    public void clearFileSystemCache() throws IOException {
        FileSystem.closeAll();
    }

    private String gsDirectory = "gs://%s/testFSInputStream/";

    @Test
    public void testSeekIllegalArgument() throws IOException {
        GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
        byte[] data = new byte[0];
        Path directory = new Path(
                String.format(
                        gsDirectory, myGhfs.getRootBucketName()));
        Path file = new Path(directory, String.format("file-%s", UUID.randomUUID()));
        ghfsHelper.writeFile(file, data, 100, /* overwrite= */ false);
        GoogleHadoopFSInputStream in =
                new GoogleHadoopFSInputStream(
                        myGhfs,
                        myGhfs.getGcsPath(file),
                        GoogleCloudStorageReadOptions.DEFAULT,
                        new FileSystem.Statistics(ghfs.getScheme())
                );
        Throwable exception = assertThrows(java.io.EOFException.class, () -> in.seek(1));
        assertTrue(exception.getMessage().contains("Invalid seek offset"));
        // Cleanup.
        assertThat(ghfs.delete(directory, true)).isTrue();
    }

    @Test
    public void testRead() throws IOException {
        GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
        Path directory = new Path(
                String.format(
                        gsDirectory, myGhfs.getRootBucketName()));
        Path file = new Path(directory, String.format("file-%s", UUID.randomUUID()));
        ghfsHelper.writeFile(file, "Some text", 100, /* overwrite= */ false);
        GoogleHadoopFSInputStream in =
                new GoogleHadoopFSInputStream(
                        myGhfs,
                        myGhfs.getGcsPath(file),
                        GoogleCloudStorageReadOptions.DEFAULT,
                        new FileSystem.Statistics(ghfs.getScheme())
                );
        assertThat(in.read(new byte[2],1, 1)).isEqualTo(1);
        assertThat(in.read(1, new byte[2],1, 1)).isEqualTo(1);
        // Cleanup.
        assertThat(ghfs.delete(directory, true)).isTrue();
    }

    @Test
    public void testAvailable() throws IOException {
        GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
        byte[] data = new byte[10];
        Path directory = new Path(
                String.format(
                        gsDirectory, myGhfs.getRootBucketName()));
        Path file = new Path(directory, String.format("file-%s", UUID.randomUUID()));
        ghfsHelper.writeFile(file, data, 100, /* overwrite= */ false);
        GoogleHadoopFSInputStream in =
                new GoogleHadoopFSInputStream(
                        myGhfs,
                        myGhfs.getGcsPath(file),
                        GoogleCloudStorageReadOptions.DEFAULT,
                        new FileSystem.Statistics(ghfs.getScheme())
                );
        assertThat(in.available()).isEqualTo(0);
        in.close();
        assertThrows(java.nio.channels.ClosedChannelException.class, () -> in.available());
        // Cleanup.
        assertThat(ghfs.delete(directory, true)).isTrue();
    }
}
