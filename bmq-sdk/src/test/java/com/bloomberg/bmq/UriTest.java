/*
 * Copyright 2022 Bloomberg Finance L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bloomberg.bmq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class UriTest {

    @Test
    public void testBasic() {

        String uriStr = "bmq://my.domain/queue-foo-bar?id=my.app";

        Uri uri = new Uri(uriStr);

        assertEquals("bmq", uri.scheme());
        assertEquals("my.domain", uri.authority());
        assertEquals("queue-foo-bar", uri.path());
        assertEquals("my.domain", uri.domain());
        assertEquals("queue-foo-bar", uri.queue());
        assertEquals("my.app", uri.id());
        assertEquals("bmq://my.domain/queue-foo-bar", uri.canonical());
        assertEquals(uriStr, uri.toString());
    }

    @Test
    public void testValidParsing() {
        {
            String uriStr = "bmq://ts.trades.myapp/my.queue";
            Uri obj = new Uri(uriStr);

            assertEquals("bmq", obj.scheme());
            assertEquals("ts.trades.myapp", obj.authority());
            assertEquals("ts.trades.myapp", obj.domain());
            assertEquals("my.queue", obj.path());
            assertEquals("my.queue", obj.queue());
            assertEquals("", obj.id());
            assertEquals("bmq://ts.trades.myapp/my.queue", obj.canonical());
        }
        {
            String uriStr = "bmq://ts.trades.myapp/my.queue?id=my.app";
            Uri obj = new Uri(uriStr);

            assertEquals("bmq", obj.scheme());
            assertEquals("ts.trades.myapp", obj.authority());
            assertEquals("ts.trades.myapp", obj.domain());
            assertEquals("my.queue", obj.queue());
            assertEquals("my.app", obj.id());
            assertEquals("bmq://ts.trades.myapp/my.queue", obj.canonical());
        }
        {
            String uriStr = "bmq://ts.trades.myapp.~tst/my.queue";
            Uri obj = new Uri(uriStr);

            assertEquals("bmq", obj.scheme());
            assertEquals("ts.trades.myapp.~tst", obj.authority());
            assertEquals("ts.trades.myapp", obj.domain());
            assertEquals("ts.trades.myapp.~tst", obj.qualifiedDomain());
            assertEquals("tst", obj.tier());
            assertEquals("my.queue", obj.queue());
            assertEquals("bmq://ts.trades.myapp.~tst/my.queue", obj.canonical());
        }
        {
            String uriStr = "bmq://ts.trades.myapp.~lcl-fooBar/my.queue";
            Uri obj = new Uri(uriStr);

            assertEquals("bmq", obj.scheme());
            assertEquals("ts.trades.myapp.~lcl-fooBar", obj.authority());
            assertEquals("ts.trades.myapp", obj.domain());
            assertEquals("ts.trades.myapp.~lcl-fooBar", obj.qualifiedDomain());
            assertEquals("lcl-fooBar", obj.tier());
            assertEquals("my.queue", obj.queue());
            assertEquals("bmq://ts.trades.myapp.~lcl-fooBar/my.queue", obj.canonical());
        }
    }

    @Test
    public void testInvalidParsing() {
        String[] URIs = {
            "",
            "foobar",
            "bb://",
            "bmq://",
            "bmq://a/",
            "bmq://$%@/ts.trades.myapp/queue@sss",
            "bb:///ts.trades.myapp/myqueue",
            "bmq://ts.trades.myapp/",
            "bmq://ts.trades.myapp/queue?id=",
            "bmq://ts.trades.myapp/queue?bs=a",
            "bmq://ts.trades.myapp/queue?",
            "bmq://ts.trades.myapp/queue?id=",
            "bmq://ts.trades.myapp/queue&id==",
            "bmq://ts.trades.myapp/queue&id=foo",
            "bmq://ts.trades.myapp/queue?id=foo&",
            "bmq://ts.trades.myapp/queue?pid=foo",
            "bmq://ts.trades.myapp.~/queue",
            "bmq://ts.trades~myapp/queue",
            "bmq://ts.trades.myapp.~a_b/queue"
        };

        for (String uriStr : URIs) {
            try {
                new Uri(uriStr);
                // Shouldn't be here
                fail();
            } catch (RuntimeException e) {
                // OK
            }
        }
    }

    @Test
    public void testEquals() {
        // Equals with the same string URI
        {
            String uriStr = "bmq://my.domain/queue-foo-bar?id=my.app";

            Uri uri1 = new Uri(uriStr);
            Uri uri2 = new Uri(uriStr);

            assertEquals(uri1, uri2);
            assertEquals(uri1.hashCode(), uri2.hashCode());
        }
        // Equals with identical string URIs
        {
            String uriStr1 = "bmq://my.domain/queue-foo-bar?id=my.app";
            String uriStr2 = "bmq://my.domain/queue-foo-bar?id=my.app";

            Uri uri1 = new Uri(uriStr1);
            Uri uri2 = new Uri(uriStr2);

            assertEquals(uri1, uri2);
            assertEquals(uri1.hashCode(), uri2.hashCode());
        }
        // Not equal with different string URIs
        {
            String uriStr1 = "bmq://my.domain/queue-foo-bar?id=my.app1";
            String uriStr2 = "bmq://my.domain/queue-foo-bar?id=my.app2";

            Uri uri1 = new Uri(uriStr1);
            Uri uri2 = new Uri(uriStr2);

            assertNotEquals(uri1, uri2);
            assertNotEquals(uri1.hashCode(), uri2.hashCode());
        }
    }
}
