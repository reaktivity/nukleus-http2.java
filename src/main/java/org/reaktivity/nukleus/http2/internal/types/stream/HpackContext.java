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

    private static final HeaderField[] STATIC_TABLE =
    {
        /* 0  */ new HeaderField((String)null, null),
        /* 1  */ new HeaderField(":authority", null),
        /* 2  */ new HeaderField(":method", "GET"),
        /* 3  */ new HeaderField(":method", "POST"),
        /* 4  */ new HeaderField(":path", "/"),
        /* 5  */ new HeaderField(":path", "/index.html"),
        /* 6  */ new HeaderField(":scheme", "http"),
        /* 7  */ new HeaderField(":scheme", "https"),
        /* 8  */ new HeaderField(":status", "200"),
        /* 9  */ new HeaderField(":status", "204"),
        /* 10 */ new HeaderField(":status", "206"),
        /* 11 */ new HeaderField(":status", "304"),
        /* 12 */ new HeaderField(":status", "400"),
        /* 13 */ new HeaderField(":status", "404"),
        /* 14 */ new HeaderField(":status", "500"),
        /* 15 */ new HeaderField("accept-charset", null),
        /* 16 */ new HeaderField("accept-encoding", "gzip, deflate"),
        /* 17 */ new HeaderField("accept-language", null),
        /* 18 */ new HeaderField("accept-ranges", null),
        /* 19 */ new HeaderField("accept", null),
        /* 20 */ new HeaderField("access-control-allow-origin", null),
        /* 21 */ new HeaderField("age", null),
        /* 22 */ new HeaderField("allow", null),
        /* 23 */ new HeaderField("authorization", null),
        /* 24 */ new HeaderField("cache-control", null),
        /* 25 */ new HeaderField("content-disposition", null),
        /* 26 */ new HeaderField("content-encoding", null),
        /* 27 */ new HeaderField("content-language", null),
        /* 28 */ new HeaderField("content-length", null),
        /* 29 */ new HeaderField("content-location", null),
        /* 30 */ new HeaderField("content-range", null),
        /* 31 */ new HeaderField("content-type", null),
        /* 32 */ new HeaderField("cookie", null),
        /* 33 */ new HeaderField("date", null),
        /* 34 */ new HeaderField("etag", null),
        /* 35 */ new HeaderField("expect", null),
        /* 36 */ new HeaderField("expires", null),
        /* 37 */ new HeaderField("from", null),
        /* 38 */ new HeaderField("host", null),
        /* 39 */ new HeaderField("if-match", null),
        /* 40 */ new HeaderField("if-modified-since", null),
        /* 41 */ new HeaderField("if-none-match", null),
        /* 42 */ new HeaderField("if-range", null),
        /* 43 */ new HeaderField("if-unmodified-since", null),
        /* 44 */ new HeaderField("last-modified", null),
        /* 45 */ new HeaderField("link", null),
        /* 46 */ new HeaderField("location", null),
        /* 47 */ new HeaderField("max-forwards", null),
        /* 48 */ new HeaderField("proxy-authenticate", null),
        /* 49 */ new HeaderField("proxy-authorization", null),
        /* 50 */ new HeaderField("range", null),
        /* 51 */ new HeaderField("referer", null),
        /* 52 */ new HeaderField("refresh", null),
        /* 53 */ new HeaderField("retry-after", null),
        /* 54 */ new HeaderField("server", null),
        /* 55 */ new HeaderField("set-cookie", null),
        /* 56 */ new HeaderField("strict-transport-security", null),
        /* 57 */ new HeaderField("transfer-encoding", null),
        /* 58 */ new HeaderField("user-agent", null),
        /* 59 */ new HeaderField("vary", null),
        /* 60 */ new HeaderField("via", null),
        /* 61 */ new HeaderField("www-authenticate", null)
    };

    // Entries are added at the end (since it is in reverse order, need
    // to calculate the index accordingly)
    /* private */ final List<HeaderField> table = new ArrayList<>();
    /* private */ int tableSize;

    // No need to update the following index maps for decoding context
    private final boolean encoding;

    // name --> uniquie id (stable across evictions). Used during encoding
    /* private */ final Map<DirectBuffer, Long> name2Index = new HashMap<>();

    // (name, value) --> uniquie id (stable across evictions). Used during encoding
    /* private */ final Map<NameValue, Long> namevalue2Index = new HashMap<>();

    private int maxTableSize;

    // Keeps track of number of evictions and used in calculation of unique id
    // (No need to worry about overflow as it takes many years to overflow in practice)
    private long noEvictions;

    private static final class HeaderField
    {
        private final DirectBuffer name;
        private final DirectBuffer value;
        private int size;

        HeaderField(String name, String value)
        {
            this(buffer(name), buffer(value));
        }

        HeaderField(DirectBuffer name, DirectBuffer value)
        {
            this.name = name;
            this.value = value;
            this.size = size(name) + size(value) + 32;
        }

        private static DirectBuffer buffer(String str)
        {
            return str == null ? null : new UnsafeBuffer(str.getBytes(UTF_8));
        }

        private static int size(DirectBuffer buffer)
        {
            return buffer == null ? 0 : buffer.capacity();
        }
    }

    public HpackContext()
    {
        this(4096, true);
    }

    public HpackContext(int maxTableSize, boolean encoding)
    {
        this.maxTableSize = maxTableSize;
        this.encoding = encoding;
    }

    void add(String name, String value)
    {
        DirectBuffer nameBuffer = new UnsafeBuffer(name.getBytes(UTF_8));
        DirectBuffer valueBuffer = new UnsafeBuffer(value.getBytes(UTF_8));

        add(nameBuffer, valueBuffer);
    }

    public void add(DirectBuffer nameBuffer, DirectBuffer valueBuffer)
    {
        HeaderField header = new HeaderField(nameBuffer, valueBuffer);

        // See if the header can be added to dynamic table. Calculate the
        // number of entries to be evicted to make space in the table.
        int noEntries = 0;
        int wouldbeSize = tableSize + header.size;
        while (noEntries < table.size() && wouldbeSize > maxTableSize)
        {
            wouldbeSize -= table.get(noEntries).size;
            noEntries++;
        }
System.out.println("Evict i items ***** " + noEntries);
        if (noEntries > 0)
        {
            evict(noEntries);
        }

        boolean enoughSpace = wouldbeSize <= maxTableSize;
        if (enoughSpace)
        {
            table.add(header);
            tableSize += header.size;
            if (encoding)
            {
                long id = noEvictions + table.size();
                name2Index.put(header.name, id);
                NameValue nameValue = new NameValue(header.name, header.value);
                namevalue2Index.put(nameValue, id);
            }
        }
    }

    // Evicts oldest entries from dynamic table
    private void evict(int noEntries)
    {
        for(int i=0; i < noEntries; i++)
        {
            HeaderField header = table.get(i);
            tableSize -= header.size;

            if (encoding)
            {
                Long id = noEvictions + i;
                if (id.equals(name2Index.get(header.name)))
                {
System.out.println("Evict name2Index ***** " + header.name.getStringWithoutLengthUtf8(0, header.name.capacity()));

                    name2Index.remove(header.name, id);

                }
                NameValue nameValue = new NameValue(header.name, header.value);
                if (id.equals(namevalue2Index.get(nameValue)))
                {
System.out.println("Evict namevalue2Index ***** " + header.name.getStringWithoutLengthUtf8(0, header.name.capacity()));

                    namevalue2Index.remove(nameValue, id);
                }
            }
        }

        table.subList(0, noEntries).clear();
        noEvictions += noEntries;
    }

    public boolean valid(int index)
    {
        return index != 0 && index < STATIC_TABLE.length + table.size();
    }

    String name(int index)
    {
        DirectBuffer nameBuffer = nameBuffer(index);
        return nameBuffer.getStringWithoutLengthUtf8(0, nameBuffer.capacity());
    }

    public DirectBuffer nameBuffer(int index)
    {
        if (!valid(index))
        {
            throw new IllegalArgumentException();
        }
        return index < STATIC_TABLE.length
                ? STATIC_TABLE[index].name
                : table.get(table.size()-(index-STATIC_TABLE.length)-1).name;
    }

    String value(int index)
    {
        DirectBuffer valueBuffer = valueBuffer(index);
        return valueBuffer.getStringWithoutLengthUtf8(0, valueBuffer.capacity());
    }

    public DirectBuffer valueBuffer(int index)
    {
        if (!valid(index))
        {
            throw new IllegalArgumentException();
        }
        return index < STATIC_TABLE.length
                ? STATIC_TABLE[index].value
                : table.get(table.size()-(index-STATIC_TABLE.length)-1).value;
    }

    int index(String name)
    {
        DirectBuffer nameBuffer = new UnsafeBuffer(name.getBytes(UTF_8));
        return index(nameBuffer);
    }

    int index(String name, String value)
    {
        DirectBuffer nameBuffer = new UnsafeBuffer(name.getBytes(UTF_8));
        DirectBuffer valueBuffer = new UnsafeBuffer(value.getBytes(UTF_8));

        return index(nameBuffer, valueBuffer);
    }

    public int index(DirectBuffer name)
    {
        for(int i=1; i < STATIC_TABLE.length; i++)
        {
            HeaderField hf = STATIC_TABLE[i];
            DirectBuffer nameBuffer = hf.name;
            if (nameBuffer.equals(name))
            {
                return i;
            }
        }

        Long id = name2Index.get(name);
        return  (id != null) ? idToIndex(id) : -1;
    }


    public int index(DirectBuffer name, DirectBuffer value)
    {
        for(int i=1; i < STATIC_TABLE.length; i++)
        {
            HeaderField hf = STATIC_TABLE[i];
            DirectBuffer nameBuffer = hf.name;
            if (nameBuffer != null && nameBuffer.equals(name))
            {
                DirectBuffer valueBuffer = hf.value;
                if (valueBuffer != null && valueBuffer.equals(value))
                {
                    return i;
                }
            }
        }
        Long id = namevalue2Index.get(new NameValue(name, value));
        return  (id != null) ? idToIndex(id) : -1;
    }

    private int idToIndex(long id)
    {
        int index = (int) (id - noEvictions);
        return STATIC_TABLE.length + table.size() - index -1;
    }

    private static final class NameValue
    {
        private final DirectBuffer name;
        private final DirectBuffer value;

        NameValue(DirectBuffer name, DirectBuffer value)
        {
            this.name = name;
            this.value = value;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof NameValue)
            {
                NameValue other = (NameValue) obj;
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
