package com.google.cloud.hadoop.gcsio.storageapi;

import static org.junit.Assert.assertThrows;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ObjectsGetMediaTest {

  public static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  public static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

  private Storage storage;

  @Before
  public void setUp() {
    storage = new Storage(HTTP_TRANSPORT, JSON_FACTORY, /* httpRequestInitializer= */ null);
  }

  @Test
  public void throwExceptionWhenCallExecute() {
    String BUCKET = "TEST_BUCKET";
    String OBJECT = "TEST_OBJECT";
    assertThrows(
        WrongRequestTypeException.class,
        new ObjectsGetMedia(storage.objects(), BUCKET, OBJECT)::execute);
  }
}
