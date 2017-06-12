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
package org.reaktivity.nukleus.http2.internal;

import java.util.Properties;
import java.util.function.IntSupplier;

public enum InternalSystemProperty
{

    // Maximum window size for nuklei writing data to the http2 nukleus
    WINDOW_SIZE("nukleus.http2.window.size", "65535"),

    MAXIMUM_SLOTS("nukleus.http2.maximum.slots", "1000");

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
