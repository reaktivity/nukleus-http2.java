package org.reaktivity.nukleus.http2.internal.types.stream;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public class HpackContext
{

    private static final String[][] STATIC_TABLE =
    {
        /* 0  */ {null, null},
        /* 1  */ {":authority", null},
        /* 2  */ {":method", "GET"},
        /* 3  */ {":method", "POST"},
        /* 4  */ {":path", "/"},
        /* 5  */ {":path", "/index.html"},
        /* 6  */ {":scheme", "http"},
        /* 7  */ {":scheme", "https"},
        /* 8  */ {":status", "200"},
        /* 9  */ {":status", "204"},
        /* 10 */ {":status", "206"},
        /* 11 */ {":status", "304"},
        /* 12 */ {":status", "400"},
        /* 13 */ {":status", "404"},
        /* 14 */ {":status", "500"},
        /* 15 */ {"accept-charset", null},
        /* 16 */ {"accept-encoding", "gzip, deflate"},
        /* 17 */ {"accept-language", null},
        /* 18 */ {"accept-ranges", null},
        /* 19 */ {"accept", null},
        /* 20 */ {"access-control-allow-origin", null},
        /* 21 */ {"age", null},
        /* 22 */ {"allow", null},
        /* 23 */ {"authorization", null},
        /* 24 */ {"cache-control", null},
        /* 25 */ {"content-disposition", null},
        /* 26 */ {"content-encoding", null},
        /* 27 */ {"content-language", null},
        /* 28 */ {"content-length", null},
        /* 29 */ {"content-location", null},
        /* 30 */ {"content-range", null},
        /* 31 */ {"content-type", null},
        /* 32 */ {"cookie", null},
        /* 33 */ {"date", null},
        /* 34 */ {"etag", null},
        /* 35 */ {"expect", null},
        /* 36 */ {"expires", null},
        /* 37 */ {"from", null},
        /* 38 */ {"host", null},
        /* 39 */ {"if-match", null},
        /* 40 */ {"if-modified-since", null},
        /* 41 */ {"if-none-match", null},
        /* 42 */ {"if-range", null},
        /* 43 */ {"if-unmodified-since", null},
        /* 44 */ {"last-modified", null},
        /* 45 */ {"link", null},
        /* 46 */ {"location", null},
        /* 47 */ {"max-forwards", null},
        /* 48 */ {"proxy-authenticate", null},
        /* 49 */ {"proxy-authorization", null},
        /* 50 */ {"range", null},
        /* 51 */ {"referer", null},
        /* 52 */ {"refresh", null},
        /* 53 */ {"retry-after", null},
        /* 54 */ {"server", null},
        /* 55 */ {"set-cookie", null},
        /* 56 */ {"strict-transport-security", null},
        /* 57 */ {"transfer-encoding", null},
        /* 58 */ {"user-agent", null},
        /* 59 */ {"vary", null},
        /* 60 */ {"via", null},
        /* 61 */ {"www-authenticate", null},
    };

    // TODO use a ring buffer to avoid moving entries
    // TODO eviction of entries
    private final List<HeaderField> table = new ArrayList<>();

    private final Map<String, Integer> name2Index = new HashMap<>();
    private final Map<NameValueKey, HeaderField> namebuf2Index = new HashMap<>();


    private static final class HeaderField
    {
        private String name;
        private DirectBuffer nameBuffer;
        private String value;
        private DirectBuffer valueBuffer;
        private int index;
        private int size;

        HeaderField(String name, String value)
        {
            this.name = name;
            this.value = value;
        }

        HeaderField(DirectBuffer nameBuffer, DirectBuffer valueBuffer)
        {
            this.nameBuffer = nameBuffer;
            this.valueBuffer = valueBuffer;
        }

        private String name()
        {
            if (name == null && nameBuffer != null)
            {
                name = nameBuffer.getStringWithoutLengthUtf8(0, nameBuffer.capacity());
            }
            return name;
        }

        private DirectBuffer nameBuffer()
        {
            if (nameBuffer == null && name != null)
            {
                nameBuffer = new UnsafeBuffer(name.getBytes(UTF_8));
            }
            return nameBuffer;
        }


        private String value()
        {
            if (value == null && valueBuffer != null)
            {
                value = valueBuffer.getStringWithoutLengthUtf8(0, valueBuffer.capacity());
            }
            return value;
        }

        private DirectBuffer valueBuffer()
        {
            if (valueBuffer == null && value != null)
            {
                valueBuffer = new UnsafeBuffer(value.getBytes(UTF_8));
            }
            return valueBuffer;
        }
    }

    public HpackContext()
    {
        for (int i = 0; i < STATIC_TABLE.length; i++)
        {
            String[] field = STATIC_TABLE[i];
            String value = field[1] == null ? "" : field[1];
            table.add(new HeaderField(field[0], value));
        }
    }

    public void add(String name, String value)
    {
        table.add(STATIC_TABLE.length, new HeaderField(name, value));
    }

    public void add(DirectBuffer nameBuffer, DirectBuffer valueBuffer)
    {
        table.add(STATIC_TABLE.length, new HeaderField(nameBuffer, valueBuffer));
    }

    public String name(int index)
    {
        return table.get(index).name();
    }

    public DirectBuffer nameBuffer(int index)
    {
        return table.get(index).nameBuffer();
    }

    public String value(int index)
    {
        return table.get(index).value();
    }

    public DirectBuffer valueBuffer(int index)
    {
        return table.get(index).valueBuffer();
    }

    public int index(String name)
    {
        Integer index = name2Index.get(name);
        return index == null ? -1 : index;
    }

    public int index(String name, String value)
    {
        throw new UnsupportedOperationException("TODO");
    }

    public int index(DirectBuffer name)
    {
        for(int i=0; i < table.size(); i++)
        {
            HeaderField hf = table.get(i);
            DirectBuffer nameBuffer = hf.nameBuffer();
            if (nameBuffer != null && nameBuffer.equals(name))
            {
                return i;
            }
        }
        return -1;
    }


    public int index(DirectBuffer name, DirectBuffer value)
    {
        for(int i=0; i < table.size(); i++)
        {
            HeaderField hf = table.get(i);
            DirectBuffer nameBuffer = hf.nameBuffer();
            if (nameBuffer != null && nameBuffer.equals(name))
            {
                DirectBuffer valueBuffer = hf.valueBuffer();
                if (valueBuffer != null && valueBuffer.equals(value))
                {
                    return i;
                }
            }
        }
        return -1;
    }

    private static class NameValueKey
    {
        private final DirectBuffer name;
        private final DirectBuffer value;

        NameValueKey(DirectBuffer name, DirectBuffer value)
        {
            this.name = name;
            this.value = value;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof NameValueKey)
            {
                NameValueKey other = (NameValueKey) obj;
                return Objects.equals(name, other.name) && Objects.equals(value, other.value);
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, value);
        }
    }

}
