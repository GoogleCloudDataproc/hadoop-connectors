/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;

import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GcsAnalyticsCoreOutputStreamWrapperTest {

  @Mock private GoogleCloudStorageOutputStream mockOutputStream;

  private GcsAnalyticsCoreOutputStreamWrapper outputStreamWrapper;

  @Before
  public void setUp() {
    outputStreamWrapper = new GcsAnalyticsCoreOutputStreamWrapper(mockOutputStream);
  }

  @Test
  public void constructor_nullDelegate_throwsException() {
    NullPointerException exception =
        assertThrows(
            NullPointerException.class, () -> new GcsAnalyticsCoreOutputStreamWrapper(null));
    assertThat(exception).hasMessageThat().contains("delegate cannot be null");
  }

  @Test
  public void write_int_delegatesToOutputStream() throws IOException {
    outputStreamWrapper.write(1);
    verify(mockOutputStream).write(1);
  }

  @Test
  public void write_bytes_delegatesToOutputStream() throws IOException {
    byte[] bytes = new byte[] {1, 2, 3};
    outputStreamWrapper.write(bytes, 1, 2);
    verify(mockOutputStream).write(bytes, 1, 2);
  }

  @Test
  public void close_delegatesToOutputStream() throws IOException {
    outputStreamWrapper.close();
    verify(mockOutputStream).close();
  }

  @Test
  public void methodsAreSynchronized() throws NoSuchMethodException {
    verifyMethodIsSynchronized(GcsAnalyticsCoreOutputStreamWrapper.class, "write", int.class);
    verifyMethodIsSynchronized(
        GcsAnalyticsCoreOutputStreamWrapper.class, "write", byte[].class, int.class, int.class);
    verifyMethodIsSynchronized(GcsAnalyticsCoreOutputStreamWrapper.class, "close");
  }

  private void verifyMethodIsSynchronized(
      Class<?> clazz, String methodName, Class<?>... parameterTypes) throws NoSuchMethodException {
    Method method = clazz.getDeclaredMethod(methodName, parameterTypes);
    assertThat(Modifier.isSynchronized(method.getModifiers())).isTrue();
  }
}
