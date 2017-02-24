/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.http2.internal.types.stream;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;

import static org.junit.Assert.assertEquals;

public class HpackHuffmanTest {

    // Tests examples from RFC 7541 (HPACK)
    @Test
    public void decode() {
        decode("f1e3c2e5f23a6ba0ab90f4ff", "www.example.com");
        decode("a8eb10649cbf", "no-cache");
        decode("25a849e95ba97d7f", "custom-key");
        decode("25a849e95bb8e8b4bf", "custom-value");
        decode("6402", "302");
        decode("aec3771a4b", "private");
        decode("d07abe941054d444a8200595040b8166e082a62d1bff", "Mon, 21 Oct 2013 20:13:21 GMT");
        decode("9d29ad171863c78f0b97c8e9ae82ae43d3", "https://www.example.com");
        decode("640eff", "307");
        decode("d07abe941054d444a8200595040b8166e084a62d1bff", "Mon, 21 Oct 2013 20:13:22 GMT");
        decode("9bd9ab", "gzip");
        decode("94e7821dd7f2e6c7b335dfdfcd5b3960d5af27087f3672c1ab270fb5291f9587316065c003ed4ee5b1063d5007",
                "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1");
    }

    private void decode(String encoded, String expected) {
        byte[] bytes = DatatypeConverter.parseHexBinary("00" + encoded);    // +00 to test offset
        DirectBuffer buf = new UnsafeBuffer(bytes, 1, bytes.length - 1);
        String got = HpackHuffman.decode(buf);
        assertEquals(expected, got);
    }

}