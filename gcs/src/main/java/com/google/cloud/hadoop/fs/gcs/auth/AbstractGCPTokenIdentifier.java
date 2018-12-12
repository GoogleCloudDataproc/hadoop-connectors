/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.hadoop.fs.gcs.auth;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.UUID;

public abstract class AbstractGCPTokenIdentifier extends DelegationTokenIdentifier {

  /**
   * How long can any of the secrets, role policy be.
   * Knox DTs can be long, so set this to a big value: {@value}
   */
  protected static final int MAX_TEXT_LENGTH = 32768;

  /** Canonical URI of the bucket. */
  private URI uri;

  /**
   * Timestamp of creation.
   * This is set to the current time; it will be overridden when
   * deserializing data.
   */
  private long created = System.currentTimeMillis();

  /**
   * An origin string for diagnostics.
   */
  private String origin = "";

  /**
   * This marshalled UUID can be used in testing to verify transmission,
   * and reuse; as it is printed you can see what is happending too.
   */
  private String uuid = UUID.randomUUID().toString();


  protected AbstractGCPTokenIdentifier(Text kind) {
    super(kind);
  }

  protected AbstractGCPTokenIdentifier(Text kind, String renewer, String realUser) {
    this(kind, new Text(), new Text(renewer), new Text(realUser));
  }

  protected AbstractGCPTokenIdentifier(Text kind, Text owner, Text renewer, Text realUser) {
    super(kind, owner, renewer, realUser);
  }

  protected AbstractGCPTokenIdentifier(Text kind, URI uri, Text owner, String origin) {
    super(kind);
    this.setOwner(owner);
    this.uri = uri;
    this.origin = origin;
  }

  public URI getUri() {
    return uri;
  }

  public String getOrigin() {
    return origin.toString();
  }

  public void setOrigin(final String origin) {
    this.origin = origin;
  }

  public long getCreated() {
    return created;
  }

  /**
   * Write state.
   * {@link org.apache.hadoop.io.Writable#write(DataOutput)}.
   * @param out destination
   * @throws IOException failure
   */
  @Override
  public void write(final DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, uri.toString());
    Text.writeString(out, origin);
    Text.writeString(out, uuid);
    out.writeLong(created);
  }

  /**
   * Read state.
   * {@link org.apache.hadoop.io.Writable#readFields(DataInput)}.
   *
   * Note: this operation gets called in toString() operations on tokens, so
   * must either always succeed, or throw an IOException to trigger the
   * catch & downgrade. RuntimeExceptions (e.g. Preconditions checks) are
   * not to be used here for this reason.)
   *
   * @param in input stream
   * @throws DelegationTokenIOException if the token binding is wrong.
   * @throws IOException IO problems.
   */
  @Override
  public void readFields(final DataInput in)
      throws DelegationTokenIOException, IOException {
    super.readFields(in);
    uri = URI.create(Text.readString(in, MAX_TEXT_LENGTH));
    origin = Text.readString(in, MAX_TEXT_LENGTH);
    uuid = Text.readString(in, MAX_TEXT_LENGTH);
    created = in.readLong();
  }


  /**
   * Validate the token by looking at its fields.
   * @throws IOException on failure.
   */
  public void validate() throws IOException {
    if (uri == null) {
      throw new DelegationTokenIOException("No URI in " + this);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GCPTokenIdentifier{");
    sb.append(getKind());
    sb.append("; uri=").append(uri);
    sb.append("; timestamp=").append(created);
    sb.append("; uuid=").append(uuid);
    sb.append("; ").append(origin);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Equality check is on superclass and URI only.
   * @param o other.
   * @return true if the base class considers them equal and the URIs match.
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final AbstractGCPTokenIdentifier that = (AbstractGCPTokenIdentifier) o;
    return Objects.equals(uuid, that.uuid) && Objects.equals(uri, that.uri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), uri);
  }

  /**
   * Return the expiry time in seconds since 1970-01-01.
   * @return the time when the session credential expire.
   */
  public long getExpiryTime() {
    return 0;
  }

  /**
   * Get the UUID of this token identifier.
   * @return a UUID.
   */
  public String getUuid() {
    return uuid;
  }

  /**
   * Create the default origin text message with hostname and
   * timestamp.
   * @return a string for token diagnostics.
   */
  public static String createDefaultOriginMessage() {
    return String.format("Created on %s at time %s.",
                         NetUtils.getHostname(),
                         java.time.Instant.now());
  }

}
