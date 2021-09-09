/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs.contract;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;

import java.io.FileNotFoundException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.contract.AbstractContractOpenTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.impl.FutureIOSupport;

/** GCS contract tests covering file open using in-memory fakes. */
public class TestInMemoryGoogleContractOpen extends AbstractContractOpenTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new InMemoryGoogleContract(conf);
  }

  @Override
  public void testChainedFailureAwaitFuture() throws Throwable {
    describe("await Future handles chained failures");
    CompletableFuture<FSDataInputStream> f =
        getFileSystem().openFile(path("testOpenFileUnknownOption")).build();
    intercept(
        RuntimeException.class,
        "exceptionally",
        () ->
            FutureIOSupport.awaitFuture(
                f.exceptionally(
                    ex -> {
                      throw new RuntimeException("exceptionally", ex);
                    })));
  }

  @Override
  public void testOpenFileExceptionallyTranslating() throws Throwable {
    describe("openFile missing file chains into exceptionally()");
    CompletableFuture<FSDataInputStream> f =
        getFileSystem().openFile(path("testOpenFileUnknownOption")).build();
    interceptFuture(
        RuntimeException.class,
        "exceptionally",
        f.exceptionally(
            ex -> {
              throw new RuntimeException("exceptionally", ex);
            }));
  }

  @Override
  public void testOpenFileLazyFail() throws Throwable {
    describe("openFile fails on a missing file in the get() and not before");
    FutureDataInputStreamBuilder builder =
        getFileSystem().openFile(path("testOpenFileLazyFail")).opt("fs.test.something", true);
    interceptFuture(FileNotFoundException.class, "", builder.build());
  }

  @Override
  public void testOpenFileFailExceptionally() throws Throwable {
    describe("openFile missing file chains into exceptionally()");
    FutureDataInputStreamBuilder builder =
        getFileSystem()
            .openFile(path("testOpenFileFailExceptionally"))
            .opt("fs.test.something", true);
    assertNull("exceptional uprating", builder.build().exceptionally(ex -> null).get());
  }

  @Override
  public void testAwaitFutureFailToFNFE() throws Throwable {
    describe("Verify that FutureIOSupport.awaitFuture extracts IOExceptions");
    FutureDataInputStreamBuilder builder =
        getFileSystem().openFile(path("testAwaitFutureFailToFNFE")).opt("fs.test.something", true);
    System.out.println("Inside test");
    intercept(FileNotFoundException.class, () -> FutureIOSupport.awaitFuture(builder.build()));
  }
}
