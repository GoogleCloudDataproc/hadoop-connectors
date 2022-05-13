/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.hadoop.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNullElseGet;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

/** Factory for creating HttpTransport types. */
public class HttpTransportFactory {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * Create an {@link HttpTransport} with socketKeepAlive true
   *
   * @return The resulting HttpTransport.
   * @throws IllegalArgumentException If the proxy address is invalid.
   * @throws IOException If there is an issue connecting to Google's Certification server.
   */
  public static HttpTransport createHttpTransport() throws IOException {
    return createHttpTransport(
        /* proxyAddress= */ null, /* proxyUsername= */ null, /* proxyPassword= */ null);
  }

  /**
   * Create an {@link HttpTransport} based on a type class and an optional HTTP proxy.
   *
   * @param proxyAddress The HTTP proxy to use with the transport. Of the form hostname:port. If
   *     empty no proxy will be used.
   * @param proxyUsername The HTTP proxy username to use with the transport. If empty no proxy
   *     username will be used.
   * @param proxyPassword The HTTP proxy password to use with the transport. If empty no proxy
   *     password will be used.
   * @return The resulting HttpTransport.
   * @throws IllegalArgumentException If the proxy address is invalid.
   * @throws IOException If there is an issue connecting to Google's Certification server.
   */
  public static HttpTransport createHttpTransport(
      @Nullable String proxyAddress,
      @Nullable RedactedString proxyUsername,
      @Nullable RedactedString proxyPassword)
      throws IOException {
    logger.atFiner().log(
        "createHttpTransport(%s, %s, %s)", proxyAddress, proxyUsername, proxyPassword);
    checkArgument(
        proxyAddress != null || (proxyUsername == null && proxyPassword == null),
        "if proxyAddress is null then proxyUsername and proxyPassword should be null too");
    checkArgument(
        (proxyUsername == null) == (proxyPassword == null),
        "both proxyUsername and proxyPassword should be null or not null together");
    URI proxyUri = parseProxyAddress(proxyAddress);
    try {
      PasswordAuthentication proxyAuth =
          proxyUsername != null
              ? new PasswordAuthentication(
                  proxyUsername.value(), proxyPassword.value().toCharArray())
              : null;
      return createNetHttpTransport(proxyUri, proxyAuth);
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  /**
   * Create an {@link NetHttpTransport} for calling Google APIs with an optional HTTP proxy.
   *
   * @param proxyUri Optional HTTP proxy URI to use with the transport.
   * @param proxyAuth Optional HTTP proxy credentials to authenticate with the transport proxy.
   * @return The resulting HttpTransport.
   * @throws IOException If there is an issue connecting to Google's certification server.
   * @throws GeneralSecurityException If there is a security issue with the keystore.
   */
  public static NetHttpTransport createNetHttpTransport(
      @Nullable URI proxyUri, @Nullable PasswordAuthentication proxyAuth)
      throws IOException, GeneralSecurityException {
    checkArgument(
        proxyUri != null || proxyAuth == null,
        "if proxyUri is null than proxyAuth should be null too");

    if (proxyAuth != null) {
      // Enable "Basic" authentication on JDK 8+
      System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");
      Authenticator.setDefault(
          new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
              if (getRequestorType() == RequestorType.PROXY
                  && getRequestingHost().equalsIgnoreCase(proxyUri.getHost())
                  && getRequestingPort() == proxyUri.getPort()) {
                return proxyAuth;
              }
              return null;
            }
          });
    }
    return createNetHttpTransportBuilder(proxyUri).build();
  }

  @VisibleForTesting
  static NetHttpTransport.Builder createNetHttpTransportBuilder(@Nullable URI proxyUri)
      throws IOException, GeneralSecurityException {
    NetHttpTransport.Builder builder =
        new NetHttpTransport.Builder().trustCertificates(GoogleUtils.getCertificateTrustStore());
    return builder
        .setSslSocketFactory(
            new SslKeepAliveSocketFactory(
                requireNonNullElseGet(
                    builder.getSslSocketFactory(), HttpsURLConnection::getDefaultSSLSocketFactory)))
        .setProxy(
            proxyUri == null
                ? null
                : new Proxy(
                    Proxy.Type.HTTP,
                    new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort())));
  }

  /**
   * Parse an HTTP proxy from a String address.
   *
   * @param proxyAddress The address of the proxy of the form (https?://)HOST:PORT.
   * @return The URI of the proxy.
   * @throws IllegalArgumentException If the address is invalid.
   */
  @VisibleForTesting
  static URI parseProxyAddress(@Nullable String proxyAddress) {
    if (isNullOrEmpty(proxyAddress)) {
      return null;
    }
    String uriString = (proxyAddress.contains("//") ? "" : "//") + proxyAddress;
    try {
      URI uri = new URI(uriString);
      String scheme = uri.getScheme();
      String host = uri.getHost();
      int port = uri.getPort();
      checkArgument(
          isNullOrEmpty(scheme) || scheme.matches("https?"),
          "HTTP proxy address '%s' has invalid scheme '%s'.",
          proxyAddress,
          scheme);
      checkArgument(!isNullOrEmpty(host), "Proxy address '%s' has no host.", proxyAddress);
      checkArgument(port != -1, "Proxy address '%s' has no port.", proxyAddress);
      checkArgument(
          uri.equals(new URI(scheme, null, host, port, null, null, null)),
          "Invalid proxy address '%s'.",
          proxyAddress);
      return uri;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid proxy address '%s'.", proxyAddress), e);
    }
  }

  /** Wrapper class to have socketKeepAlive property while creating the socket */
  @VisibleForTesting
  static class SslKeepAliveSocketFactory extends SSLSocketFactory {

    private final SSLSocketFactory wrappedSockedFactory;

    public SslKeepAliveSocketFactory(SSLSocketFactory wrappedSocketFactory) {
      this.wrappedSockedFactory = wrappedSocketFactory;
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return wrappedSockedFactory.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return wrappedSockedFactory.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket() throws IOException {
      return setSocketKeepAlive(wrappedSockedFactory.createSocket());
    }

    @Override
    public Socket createSocket(Socket s, InputStream consumed, boolean autoClose)
        throws IOException {
      return setSocketKeepAlive(wrappedSockedFactory.createSocket(s, consumed, autoClose));
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose)
        throws IOException {
      return setSocketKeepAlive(wrappedSockedFactory.createSocket(s, host, port, autoClose));
    }

    public Socket createSocket(String host, int port) throws IOException {
      return setSocketKeepAlive(wrappedSockedFactory.createSocket(host, port));
    }

    public Socket createSocket(InetAddress address, int port) throws IOException {
      return setSocketKeepAlive(wrappedSockedFactory.createSocket(address, port));
    }

    public Socket createSocket(String host, int port, InetAddress clientAddress, int clientPort)
        throws IOException {
      return setSocketKeepAlive(
          wrappedSockedFactory.createSocket(host, port, clientAddress, clientPort));
    }

    public Socket createSocket(
        InetAddress address, int port, InetAddress clientAddress, int clientPort)
        throws IOException {
      return setSocketKeepAlive(
          wrappedSockedFactory.createSocket(address, port, clientAddress, clientPort));
    }

    private static Socket setSocketKeepAlive(Socket socket) throws SocketException {
      socket.setKeepAlive(true);
      return socket;
    }
  }
}
