/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or typeied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.hadoop.util.HttpTransportFactory.SslKeepAliveSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import javax.net.ssl.SSLSocketFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HttpTransportFactoryTest {

  private static final FakeSslSocketFactory FAKE_SOCKET_FACTORY = new FakeSslSocketFactory();
  private static final String[] SUPPORTED_TEST_SUITES = {"testSuite"};
  private static final String[] DEFAULT_CIPHER_SUITES = {"testDefaultCipherSuite"};

  @Test
  public void testParseProxyAddress() throws Exception {
    String address = "foo-host:1234";
    URI expectedUri = getURI(null, "foo-host", 1234);
    URI uri = HttpTransportFactory.parseProxyAddress(address);
    assertThat(uri).isEqualTo(expectedUri);
  }

  @Test
  public void testParseProxyAddressHttp() throws Exception {
    String address = "http://foo-host:1234";
    URI expectedUri = getURI("http", "foo-host", 1234);
    URI uri = HttpTransportFactory.parseProxyAddress(address);
    assertThat(uri).isEqualTo(expectedUri);
  }

  @Test
  public void testParseProxyAddressHttps() throws Exception {
    String address = "https://foo-host:1234";
    URI expectedUri = getURI("https", "foo-host", 1234);
    URI uri = HttpTransportFactory.parseProxyAddress(address);
    assertThat(uri).isEqualTo(expectedUri);
  }

  @Test
  public void testParseProxyAddressInvalidScheme() throws Exception {
    String address = "socks5://foo-host:1234";

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> HttpTransportFactory.parseProxyAddress(address));
    assertThat(thrown)
        .hasMessageThat()
        .contains("HTTP proxy address 'socks5://foo-host:1234' has invalid scheme 'socks5'.");
  }

  @Test
  public void testParseProxyAddressNoHost() throws Exception {
    String address = ":1234";

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> HttpTransportFactory.parseProxyAddress(address));
    assertThat(thrown).hasMessageThat().contains("Proxy address ':1234' has no host.");
  }

  @Test
  public void testParseProxyAddressNoPort() throws Exception {
    String address = "foo-host";

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> HttpTransportFactory.parseProxyAddress(address));
    assertThat(thrown).hasMessageThat().contains("Proxy address 'foo-host' has no port.");
  }

  @Test
  public void testParseProxyAddressInvalidSyntax() throws Exception {
    String address = "foo-host-with-illegal-char^:1234";

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> HttpTransportFactory.parseProxyAddress(address));
    assertThat(thrown)
        .hasMessageThat()
        .contains("Invalid proxy address 'foo-host-with-illegal-char^:1234'.");
  }

  @Test
  public void testParseProxyAddressWithPath() throws Exception {
    String address = "foo-host:1234/some/path";

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> HttpTransportFactory.parseProxyAddress(address));
    assertThat(thrown)
        .hasMessageThat()
        .contains("Invalid proxy address 'foo-host:1234/some/path'.");
  }

  @Test
  public void testKeepAliveSocketFactoryDefaultCipherSuites() {
    SslKeepAliveSocketFactory sslKeepAliveSocketFactory =
        new SslKeepAliveSocketFactory(FAKE_SOCKET_FACTORY);

    assertThat(sslKeepAliveSocketFactory.getDefaultCipherSuites()).isEqualTo(DEFAULT_CIPHER_SUITES);
  }

  @Test
  public void testKeepAliveSocketFactorySupportedCipherSuites() {
    SslKeepAliveSocketFactory sslKeepAliveSocketFactory =
        new SslKeepAliveSocketFactory(FAKE_SOCKET_FACTORY);

    assertThat(sslKeepAliveSocketFactory.getSupportedCipherSuites())
        .isEqualTo(SUPPORTED_TEST_SUITES);
  }

  @Test
  public void testKeepAliveSettingIsNotCorrupted() throws GeneralSecurityException, IOException {
    NetHttpTransport.Builder builder =
        HttpTransportFactory.prepareNetHttpTransportBuilder(
            GoogleUtils.getCertificateTrustStore(), null);
    assertThat(builder.getSslSocketFactory()).isInstanceOf(SslKeepAliveSocketFactory.class);
  }

  @Test
  public void testKeepAliveSocketFactoryKeepAliveTrue() throws IOException {
    SslKeepAliveSocketFactory sslKeepAliveSocketFactory =
        new SslKeepAliveSocketFactory(FAKE_SOCKET_FACTORY);

    assertThat(sslKeepAliveSocketFactory.createSocket().getKeepAlive()).isTrue();

    assertThat(sslKeepAliveSocketFactory.createSocket(null, "localhost", 80, false).getKeepAlive())
        .isTrue();

    assertThat(sslKeepAliveSocketFactory.createSocket(null, null, false).getKeepAlive()).isTrue();

    assertThat(sslKeepAliveSocketFactory.createSocket("localhost", 80).getKeepAlive()).isTrue();

    assertThat(sslKeepAliveSocketFactory.createSocket("localhost", 80, null, 443).getKeepAlive())
        .isTrue();

    InetAddress fakeInet = InetAddress.getByName("10.0.0.0");
    assertThat(sslKeepAliveSocketFactory.createSocket(fakeInet, 443).getKeepAlive()).isTrue();

    assertThat(sslKeepAliveSocketFactory.createSocket(fakeInet, 443, fakeInet, 80).getKeepAlive())
        .isTrue();
  }

  private static class FakeSslSocketFactory extends SSLSocketFactory {

    @Override
    public String[] getDefaultCipherSuites() {
      return DEFAULT_CIPHER_SUITES;
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return SUPPORTED_TEST_SUITES;
    }

    @Override
    public Socket createSocket() {
      return new Socket();
    }

    @Override
    public Socket createSocket(Socket socket, String s, int i, boolean b) throws IOException {
      return createSocket();
    }

    @Override
    public Socket createSocket(Socket socket, InputStream inputStream, boolean b)
        throws IOException {
      return createSocket();
    }

    @Override
    public Socket createSocket(String s, int i) throws IOException, UnknownHostException {
      return createSocket();
    }

    @Override
    public Socket createSocket(String s, int i, InetAddress inetAddress, int i1)
        throws IOException, UnknownHostException {
      return createSocket();
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i) throws IOException {
      return createSocket();
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i, InetAddress inetAddress1, int i1)
        throws IOException {
      return createSocket();
    }
  }

  private static URI getURI(String scheme, String host, int port) throws URISyntaxException {
    return new URI(scheme, null, host, port, null, null, null);
  }
}
