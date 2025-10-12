/*
 * Copyright 2023 Google LLC
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

package com.google.cloud.hadoop.gcsio;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Empty;
import com.google.storage.v2.BidiWriteObjectRequest;
import com.google.storage.v2.BidiWriteObjectResponse;
import com.google.storage.v2.Bucket;
import com.google.storage.v2.ComposeObjectRequest;
import com.google.storage.v2.CreateBucketRequest;
import com.google.storage.v2.DeleteBucketRequest;
import com.google.storage.v2.DeleteObjectRequest;
import com.google.storage.v2.GetBucketRequest;
import com.google.storage.v2.GetObjectRequest;
import com.google.storage.v2.ListBucketsRequest;
import com.google.storage.v2.ListBucketsResponse;
import com.google.storage.v2.MoveObjectRequest;
import com.google.storage.v2.Object;
import com.google.storage.v2.RewriteObjectRequest;
import com.google.storage.v2.RewriteResponse;
import com.google.storage.v2.StorageGrpc.StorageImplBase;
import com.google.storage.v2.UpdateObjectRequest;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

final class MockStorage extends StorageImplBase {

  private List<AbstractMessage> requests;
  private Queue<java.lang.Object> responses;

  public MockStorage() {
    requests = new ArrayList<>();
    responses = new LinkedList<>();
  }

  public List<AbstractMessage> getRequests() {
    return requests;
  }

  public void addResponse(AbstractMessage response) {
    responses.add(response);
  }

  public void addException(Exception exception) {
    responses.add(exception);
  }

  public void reset() {
    requests = new ArrayList<>();
    responses = new LinkedList<>();
  }

  @Override
  public void createBucket(CreateBucketRequest request, StreamObserver<Bucket> responseObserver) {
    java.lang.Object response = responses.poll();
    if (response instanceof Bucket) {
      requests.add(request);
      responseObserver.onNext(((Bucket) response));
      responseObserver.onCompleted();
    } else if (response instanceof Exception) {
      responseObserver.onError(((Exception) response));
    } else {
      responseObserver.onError(
          new IllegalArgumentException(
              String.format(
                  "Unrecognized response type %s for method CreateBucket, expected %s or %s",
                  response == null ? "null" : response.getClass().getName(),
                  Bucket.class.getName(),
                  Exception.class.getName())));
    }
  }

  @Override
  public void composeObject(ComposeObjectRequest request, StreamObserver<Object> responseObserver) {
    java.lang.Object response = responses.poll();
    if (response instanceof Object) {
      requests.add(request);
      responseObserver.onNext(((Object) response));
      responseObserver.onCompleted();
    } else if (response instanceof Exception) {
      responseObserver.onError(((Exception) response));
    } else {
      responseObserver.onError(
          new IllegalArgumentException(
              String.format(
                  "Unrecognized response type %s for method ComposeObject, expected %s or %s",
                  response == null ? "null" : response.getClass().getName(),
                  Object.class.getName(),
                  Exception.class.getName())));
    }
  }

  @Override
  public void deleteBucket(DeleteBucketRequest request, StreamObserver<Empty> responseObserver) {
    java.lang.Object response = responses.poll();
    if (response instanceof Empty) {
      requests.add(request);
      responseObserver.onNext(((Empty) response));
      responseObserver.onCompleted();
    } else if (response instanceof Exception) {
      responseObserver.onError(((Exception) response));
    } else {
      responseObserver.onError(
          new IllegalArgumentException(
              String.format(
                  "Unrecognized response type %s for method DeleteBucket, expected %s or %s",
                  response == null ? "null" : response.getClass().getName(),
                  Empty.class.getName(),
                  Exception.class.getName())));
    }
  }

  @Override
  public void listBuckets(
      ListBucketsRequest request, StreamObserver<ListBucketsResponse> responseObserver) {
    java.lang.Object response = responses.poll();
    if (response instanceof ListBucketsResponse) {
      requests.add(request);
      responseObserver.onNext(((ListBucketsResponse) response));
      responseObserver.onCompleted();
    } else if (response instanceof Exception) {
      responseObserver.onError(((Exception) response));
    } else {
      responseObserver.onError(
          new IllegalArgumentException(
              String.format(
                  "Unrecognized response type %s for method ListBuckets, expected %s or %s",
                  response == null ? "null" : response.getClass().getName(),
                  ListBucketsResponse.class.getName(),
                  Exception.class.getName())));
    }
  }

  @Override
  public void getBucket(GetBucketRequest request, StreamObserver<Bucket> responseObserver) {
    java.lang.Object response = responses.poll();
    if (response instanceof Bucket) {
      requests.add(request);
      responseObserver.onNext(((Bucket) response));
      responseObserver.onCompleted();
    } else if (response instanceof Exception) {
      responseObserver.onError(((Exception) response));
    } else {
      responseObserver.onError(
          new IllegalArgumentException(
              String.format(
                  "Unrecognized response type %s for method GetBucket, expected %s or %s",
                  response == null ? "null" : response.getClass().getName(),
                  Bucket.class.getName(),
                  Exception.class.getName())));
    }
  }

  @Override
  public void deleteObject(DeleteObjectRequest request, StreamObserver<Empty> responseObserver) {
    java.lang.Object response = responses.poll();
    if (response instanceof Empty) {
      requests.add(request);
      responseObserver.onNext(((Empty) response));
      responseObserver.onCompleted();
    } else if (response instanceof Exception) {
      responseObserver.onError(((Exception) response));
    } else {
      responseObserver.onError(
          new IllegalArgumentException(
              String.format(
                  "Unrecognized response type %s for method DeleteObject, expected %s or %s",
                  response == null ? "null" : response.getClass().getName(),
                  Empty.class.getName(),
                  Exception.class.getName())));
    }
  }

  @Override
  public void getObject(GetObjectRequest request, StreamObserver<Object> responseObserver) {
    java.lang.Object response = responses.poll();
    if (response instanceof Object) {
      requests.add(request);
      responseObserver.onNext(((Object) response));
      responseObserver.onCompleted();
    } else if (response instanceof Exception) {
      responseObserver.onError(((Exception) response));
    } else {
      responseObserver.onError(
          new IllegalArgumentException(
              String.format(
                  "Unrecognized response type %s for method GetObject, expected %s or %s",
                  response == null ? "null" : response.getClass().getName(),
                  Object.class.getName(),
                  Exception.class.getName())));
    }
  }

  @Override
  public void updateObject(UpdateObjectRequest request, StreamObserver<Object> responseObserver) {
    java.lang.Object response = responses.poll();
    if (response instanceof Object) {
      requests.add(request);
      responseObserver.onNext(((Object) response));
      responseObserver.onCompleted();
    } else if (response instanceof Exception) {
      responseObserver.onError(((Exception) response));
    } else {
      responseObserver.onError(
          new IllegalArgumentException(
              String.format(
                  "Unrecognized response type %s for method UpdateObject, expected %s or %s",
                  response == null ? "null" : response.getClass().getName(),
                  Object.class.getName(),
                  Exception.class.getName())));
    }
  }

  @Override
  public void rewriteObject(
      RewriteObjectRequest request, StreamObserver<RewriteResponse> responseObserver) {
    java.lang.Object response = responses.poll();
    if (response instanceof RewriteResponse) {
      requests.add(request);
      responseObserver.onNext(((RewriteResponse) response));
      responseObserver.onCompleted();
    } else if (response instanceof Exception) {
      responseObserver.onError(((Exception) response));
    } else {
      responseObserver.onError(
          new IllegalArgumentException(
              String.format(
                  "Unrecognized response type %s for method RewriteObject, expected %s or %s",
                  response == null ? "null" : response.getClass().getName(),
                  RewriteResponse.class.getName(),
                  Exception.class.getName())));
    }
  }

  @Override
  public StreamObserver<WriteObjectRequest> writeObject(
      final StreamObserver<WriteObjectResponse> responseObserver) {
    return new StreamObserver<>() {
      @Override
      public void onNext(WriteObjectRequest value) {
        requests.add(value);
        final java.lang.Object response = responses.remove();
        if (response instanceof WriteObjectResponse) {
          responseObserver.onNext(((WriteObjectResponse) response));
        } else if (response instanceof Exception) {
          responseObserver.onError(((Exception) response));
        } else {
          responseObserver.onError(
              new IllegalArgumentException(
                  String.format(
                      "Unrecognized response type %s for method WriteObject, expected %s or %s",
                      response == null ? "null" : response.getClass().getName(),
                      WriteObjectResponse.class.getName(),
                      Exception.class.getName())));
        }
      }

      @Override
      public void onError(Throwable throwable) {}

      @Override
      public void onCompleted() {}
    };
  }

  @Override
  public void moveObject(MoveObjectRequest request, StreamObserver<Object> responseObserver) {
    java.lang.Object response = responses.poll();
    if (response instanceof Object) {
      requests.add(request);
      responseObserver.onNext(((Object) response));
      responseObserver.onCompleted();
    } else if (response instanceof Exception) {
      responseObserver.onError(((Exception) response));
    } else {
      responseObserver.onError(
          new IllegalArgumentException(
              String.format(
                  "Unrecognized response type %s for method MoveObject, expected %s or %s",
                  response == null ? "null" : response.getClass().getName(),
                  Object.class.getName(),
                  Exception.class.getName())));
    }
  }

  @Override
  public StreamObserver<BidiWriteObjectRequest> bidiWriteObject(
      final StreamObserver<BidiWriteObjectResponse> responseObserver) {
    return new StreamObserver<>() {
      @Override
      public void onNext(BidiWriteObjectRequest value) {
        // Record the incoming request.
        requests.add(value);

        java.lang.Object response = responses.poll();
        if (response instanceof BidiWriteObjectResponse) {
          responseObserver.onNext(((BidiWriteObjectResponse) response));
        } else if (response instanceof Exception) {
          responseObserver.onError(((Exception) response));
        } else {
          responseObserver.onError(
              new IllegalArgumentException(
                  String.format(
                      "Unrecognized response type %s for method BidiWriteObject, expected %s or %s",
                      response == null ? "null" : response.getClass().getName(),
                      BidiWriteObjectResponse.class.getName(),
                      Exception.class.getName())));
        }
      }

      @Override
      public void onError(Throwable throwable) {
        // Mock implementation can be empty.
      }

      @Override
      public void onCompleted() {
        // The client is acknowledging the end of the stream.
        // We can now safely close our side.
        responseObserver.onCompleted();
      }
    };
  }
}
