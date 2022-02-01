/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs.auth;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;

import com.google.cloud.hadoop.fs.gcs.DelegationTokenStatistics;
import com.google.cloud.hadoop.fs.gcs.GhfsStatistic;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.service.AbstractService;

/** Binds file system with service and access token provider */
public abstract class AbstractDelegationTokenBinding extends AbstractService {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final String SERVICE_NAME = "DelegationTokenBinding";

  private final Text kind;

  protected SecretManager<DelegationTokenIdentifier> secretManager = new TokenSecretManager();

  private Text service;

  /** The owning filesystem. Valid after {@link #bindToFileSystem(GoogleHadoopFileSystem, Text)}. */
  private GoogleHadoopFileSystem fileSystem;

  /** Statistics for the operations. */
  private DelegationTokenStatistics stats;

  protected AbstractDelegationTokenBinding(Text kind) {
    this(SERVICE_NAME, kind);
  }

  protected AbstractDelegationTokenBinding(String name, Text kind) {
    super(name);
    this.kind = kind;
  }

  public Text getKind() {
    return kind;
  }

  /** Returns the bound file system */
  public GoogleHadoopFileSystem getFileSystem() {
    return fileSystem;
  }

  public Text getService() {
    return service;
  }

  /**
   * Perform any actions when deploying unbonded, and return a list of credentials providers.
   *
   * @throws IOException any failure.
   */
  public abstract AccessTokenProvider deployUnbonded() throws IOException;

  /**
   * Bind to the token identifier, returning the credentials providers to use for the owner to talk
   * to GCP services.
   *
   * @param retrievedIdentifier the unmarshalled data
   * @return non-empty list of GCP credentials providers to use for authenticating this client with
   *     GCP services.
   * @throws IOException any failure.
   */
  public abstract AccessTokenProvider bindToTokenIdentifier(
      DelegationTokenIdentifier retrievedIdentifier) throws IOException;

  /**
   * Bind to the filesystem. Subclasses can use this to perform their own binding operations - but
   * they must always call their superclass implementation. This <i>Must</i> be called before
   * calling {@code init()}.
   *
   * <p><b>Important:</b> This binding will happen during FileSystem.initialize(); the FS is not
   * live for actual use and will not yet have interacted with GCS services.
   *
   * @param fileSystem owning FS.
   * @param service name of the service (i.e. bucket name) for the FS.
   */
  public void bindToFileSystem(GoogleHadoopFileSystem fileSystem, Text service) {
    this.fileSystem = requireNonNull(fileSystem);
    this.service = requireNonNull(service);
  }

  /**
   * Create a delegation token for the user. This will only be called if a new DT is needed, that
   * is: the filesystem has been deployed unbound.
   *
   * @return the token
   * @throws IOException if one cannot be created
   */
  public Token<DelegationTokenIdentifier> createDelegationToken(
      String renewer, DelegationTokenStatistics stats) throws IOException {
    Text renewerText = new Text();
    this.stats = stats;
    if (renewer != null) {
      renewerText.set(renewer);
    }
    DelegationTokenIdentifier tokenIdentifier =
        requireNonNull(createTokenIdentifier(renewerText), "Token identifier");
    Token<DelegationTokenIdentifier> token =
        trackDuration(
            this.stats,
            GhfsStatistic.DELEGATION_TOKENS_ISSUED.getSymbol(),
            () -> new Token<>(tokenIdentifier, secretManager));
    token.setKind(getKind());
    token.setService(service);
    noteTokenCreated(token);

    logger.atFine().log("Created token %s with token identifier %s", token, tokenIdentifier);
    return token;
  }

  /**
   * Create a token identifier with all the information needed to be included in a delegation token.
   * This is where session credentials need to be extracted, etc. This will only be called if a new
   * DT is needed, that is: the filesystem has been deployed unbound.
   *
   * <p>If {@link #createDelegationToken} is overridden, this method can be replaced with a stub.
   *
   * @return the token data to include in the token identifier.
   * @throws IOException failure creating the token data.
   */
  public abstract DelegationTokenIdentifier createTokenIdentifier(Text renewer) throws IOException;

  /**
   * Create a token identifier with all the information needed to be included in a delegation token.
   * This is where session credentials need to be extracted, etc. This will only be called if a new
   * DT is needed, that is: the filesystem has been deployed unbound.
   *
   * <p>If {@link #createDelegationToken} is overridden, this method can be replaced with a stub.
   *
   * @return the token data to include in the token identifier.
   * @throws IOException failure creating the token data.
   */
  public abstract DelegationTokenIdentifier createTokenIdentifier() throws IOException;

  /**
   * Create a new "empty" token identifier. It is used by the "dummy" SecretManager, which requires
   * a token identifier (even one that's not real) to satisfy the contract.
   *
   * @return an empty identifier.
   */
  public abstract DelegationTokenIdentifier createEmptyIdentifier();

  /**
   * Verify that a token identifier is of a specific class. This will reject subclasses (i.e. it is
   * stricter than {@code instanceof}, then cast it to that type.
   *
   * @param identifier identifier to validate
   * @param expectedClass class of the expected token identifier.
   * @throws DelegationTokenIOException If the wrong class was found.
   */
  @SuppressWarnings("unchecked") // safe by contract of convertTokenIdentifier()
  protected <T extends DelegationTokenIdentifier> T convertTokenIdentifier(
      DelegationTokenIdentifier identifier, Class<T> expectedClass)
      throws DelegationTokenIOException {
    if (identifier.getClass().equals(expectedClass)) {
      return (T) identifier;
    }
    throw DelegationTokenIOException.wrongTokenType(expectedClass, identifier);
  }

  /**
   * The secret manager always uses the same secret; the factory for new identifiers is that of the
   * token manager.
   */
  protected class TokenSecretManager extends SecretManager<DelegationTokenIdentifier> {

    private final byte[] pwd = "not-a-password".getBytes(StandardCharsets.UTF_8);

    @Override
    protected byte[] createPassword(DelegationTokenIdentifier identifier) {
      return pwd;
    }

    @Override
    public byte[] retrievePassword(DelegationTokenIdentifier identifier) {
      return pwd;
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
      return createEmptyIdentifier();
    }
  }
  /**
   * Note that a token has been created; increment counters and statistics.
   *
   * @param token token created
   */
  private void noteTokenCreated(Token<DelegationTokenIdentifier> token) {
    stats.tokenIssued();
  }
}
