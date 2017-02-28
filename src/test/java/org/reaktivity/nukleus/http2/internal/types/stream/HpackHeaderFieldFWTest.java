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
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW.HeaderFieldType;

import javax.xml.bind.DatatypeConverter;

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW.HeaderFieldType.LITERAL;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.INCREMENTAL_INDEXING;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.NEVER_INDEXED;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.WITHOUT_INDEXING;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.NameType.INDEXED;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.NameType.NEW;

public class HpackHeaderFieldFWTest {

    @Test
    public void decodeC_2_1() {
        byte[] bytes = DatatypeConverter.parseHexBinary(
                "00" +  // +00 to test offset
                        // Header field  begin
                        "400a637573746f6d2d6b65790d637573746f6d2d686561646572" +
                        // Header field  end
                        "00");
        DirectBuffer buf = new UnsafeBuffer(bytes);

        HpackHeaderFieldFW fw = new HpackHeaderFieldFW().wrap(buf, 1, buf.capacity() - 1);
        assertEquals(27, fw.limit());

        assertEquals(LITERAL, fw.type());
        HpackLiteralHeaderFieldFW literalRO = fw.literal();

        assertEquals(INCREMENTAL_INDEXING, literalRO.literalType());
        assertEquals(NEW, literalRO.nameType());

        HpackStringFW nameRO = literalRO.nameLiteral();
        DirectBuffer name = nameRO.payload();
        assertEquals("custom-key", name.getStringWithoutLengthUtf8(0, name.capacity()));

        HpackStringFW valueRO = literalRO.valueLiteral();
        DirectBuffer value = valueRO.payload();
        assertEquals("custom-header", value.getStringWithoutLengthUtf8(0, value.capacity()));
    }

    @Test
    public void decodeC_2_2() {
        byte[] bytes = DatatypeConverter.parseHexBinary(
                "00" +  // +00 to test offset
                        // Header field  begin
                        "040c2f73616d706c652f70617468" +
                        // Header field  end
                        "00");
        DirectBuffer buf = new UnsafeBuffer(bytes);

        HpackHeaderFieldFW fw = new HpackHeaderFieldFW().wrap(buf, 1, buf.capacity() - 1);
        assertEquals(15, fw.limit());

        assertEquals(LITERAL, fw.type());
        HpackLiteralHeaderFieldFW literalRO = fw.literal();

        assertEquals(WITHOUT_INDEXING, literalRO.literalType());
        assertEquals(INDEXED, literalRO.nameType());

        int index = literalRO.nameIndex();
        assertEquals(":path", HpackContext.STATIC_TABLE[index][0]);

        HpackStringFW valueRO = literalRO.valueLiteral();
        DirectBuffer value = valueRO.payload();
        assertEquals("/sample/path", value.getStringWithoutLengthUtf8(0, value.capacity()));
    }

    @Test
    public void decodeC_2_3() {
        byte[] bytes = DatatypeConverter.parseHexBinary(
                "00" +  // +00 to test offset
                        // Header field  begin
                        "100870617373776f726406736563726574" +
                        // Header field  end
                        "00");
        DirectBuffer buf = new UnsafeBuffer(bytes);

        HpackHeaderFieldFW fw = new HpackHeaderFieldFW().wrap(buf, 1, buf.capacity() - 1);
        assertEquals(18, fw.limit());

        assertEquals(LITERAL, fw.type());
        HpackLiteralHeaderFieldFW literalRO = fw.literal();

        assertEquals(NEVER_INDEXED, literalRO.literalType());
        assertEquals(NEW, literalRO.nameType());

        HpackStringFW nameRO = literalRO.nameLiteral();
        DirectBuffer name = nameRO.payload();
        assertEquals("password", name.getStringWithoutLengthUtf8(0, name.capacity()));

        HpackStringFW valueRO = literalRO.valueLiteral();
        DirectBuffer value = valueRO.payload();
        assertEquals("secret", value.getStringWithoutLengthUtf8(0, value.capacity()));
    }

    @Test
    public void decodeC_2_4() {
        byte[] bytes = DatatypeConverter.parseHexBinary(
                "00" +  // +00 to test offset
                        // Header field  begin
                        "82" +
                        // Header field  end
                        "00");
        DirectBuffer buf = new UnsafeBuffer(bytes);

        HpackHeaderFieldFW fw = new HpackHeaderFieldFW().wrap(buf, 1, buf.capacity() - 1);
        assertEquals(2, fw.limit());

        assertEquals(HeaderFieldType.INDEXED, fw.type());
        int index = fw.index();
        assertEquals(":method", HpackContext.STATIC_TABLE[index][0]);
        assertEquals("GET", HpackContext.STATIC_TABLE[index][1]);
    }

}