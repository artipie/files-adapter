/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 artipie.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.artipie.files;

import com.artipie.asto.Content;
import com.artipie.asto.Key;
import com.artipie.asto.Storage;
import com.artipie.asto.blocking.BlockingStorage;
import com.artipie.asto.memory.InMemoryStorage;
import com.artipie.http.client.auth.Authenticator;
import com.artipie.http.client.jetty.JettyClientSlices;
import com.artipie.http.hm.RsHasBody;
import com.artipie.http.hm.SliceHasResponse;
import com.artipie.http.rq.RequestLine;
import com.artipie.http.rq.RqMethod;
import com.artipie.vertx.VertxSliceServer;
import io.vertx.reactivex.core.Vertx;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import org.apache.http.client.utils.URIBuilder;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.IsEqual;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for files adapter.
 * @since 0.5
 * @checkstyle ClassDataAbstractionCouplingCheck (500 lines)
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class FileProxySliceITCase {

    /**
     * The host to send requests to.
     */
    private static final String HOST = "localhost";

    /**
     * Vertx instance.
     */
    private Vertx vertx;

    /**
     * Storage for server.
     */
    private Storage storage;

    /**
     * Server port.
     */
    private int port;

    /**
     * Jetty HTTP client slices.
     */
    private final JettyClientSlices clients = new JettyClientSlices();

    /**
     * Slice server.
     */
    private VertxSliceServer server;

    @BeforeEach
    void setUp() throws Exception {
        this.vertx = Vertx.vertx();
        this.storage = new InMemoryStorage();
        this.server = new VertxSliceServer(this.vertx, new FilesSlice(this.storage));
        this.port = this.server.start();
        this.clients.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        this.server.stop();
        this.vertx.close();
        this.clients.stop();
    }

    @Test
    void sendsRequestsViaProxy() throws Exception {
        final String data = "hello";
        new BlockingStorage(this.storage)
            .save(new Key.From("foo/bar"), data.getBytes(StandardCharsets.UTF_8));
        MatcherAssert.assertThat(
            new FileProxySlice(
                this.clients,
                new URIBuilder().setScheme("http")
                    .setHost(FileProxySliceITCase.HOST)
                    .setPort(this.port)
                    .setPath("/foo")
                    .build()
            ),
            new SliceHasResponse(
                new RsHasBody(data.getBytes(StandardCharsets.UTF_8)),
                new RequestLine(RqMethod.GET, "/bar")
            )
        );
    }

    @Test
    void savesDataInCache() throws URISyntaxException {
        final byte[] data = "xyz098".getBytes(StandardCharsets.UTF_8);
        new BlockingStorage(this.storage).save(new Key.From("foo/any"), data);
        final Storage cache = new InMemoryStorage();
        MatcherAssert.assertThat(
            "Does not return content from proxy",
            new FileProxySlice(
                this.clients,
                new URIBuilder().setScheme("http")
                    .setHost(FileProxySliceITCase.HOST)
                    .setPort(this.port)
                    .setPath("/foo")
                    .build(),
                Authenticator.ANONYMOUS,
                cache
            ),
            new SliceHasResponse(
                new RsHasBody(data),
                new RequestLine(RqMethod.GET, "/any")
            )
        );
        MatcherAssert.assertThat(
            "Does not cache data",
            new BlockingStorage(cache).value(new Key.From("any")),
            new IsEqual<>(data)
        );
    }

    @Test
    void getsFromCacheIfInRemoteNotFound() throws URISyntaxException {
        final Storage cache = new InMemoryStorage();
        final byte[] data = "abc123".getBytes(StandardCharsets.UTF_8);
        final String key = "abc";
        cache.save(new Key.From(key), new Content.From(data)).join();
        MatcherAssert.assertThat(
            new FileProxySlice(
                this.clients,
                new URIBuilder().setScheme("http")
                    .setHost(FileProxySliceITCase.HOST)
                    .setPort(this.port)
                    .setPath("/foo")
                    .build(),
                Authenticator.ANONYMOUS,
                cache
            ),
            new SliceHasResponse(
                new RsHasBody(data),
                new RequestLine(RqMethod.GET, String.format("/%s", key))
            )
        );
    }
}
