package org.reaktivity.nukleus.http2.internal;

import java.util.Properties;
import java.util.function.IntSupplier;

public enum InternalSystemProperty
{

    // Maximum window size for nuklei writing data to the http2 nukleus
    WINDOW_SIZE("nukleus.http2.window.size", "65535"),

    MAXIMUM_STREAMS_WITH_PENDING_WRITES("nukleus.http2.maximum.pending.write.streams", "1000");

    private final String name;
    private final String defaultValue;

    InternalSystemProperty(String propertyName)
    {
        this(propertyName, null);
    }

    InternalSystemProperty(String name, String defaultValue)
    {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    public String stringValue(Properties configuration)
    {
        return System.getProperty(name, defaultValue);
    }

    public Integer intValue()
    {
        return Integer.getInteger(name, Integer.parseInt(defaultValue));
    }

    public Integer intValue(IntSupplier defaultValue)
    {
        return Integer.getInteger(name, defaultValue.getAsInt());
    }

    public String propertyName()
    {
        return name;
    }

}
