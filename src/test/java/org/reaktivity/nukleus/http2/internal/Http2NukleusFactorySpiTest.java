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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.util.Properties;

import org.junit.Test;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusFactory;

public class Http2NukleusFactorySpiTest
{
    @Test
    public void shouldCreateHttp2Nukleus()
    {
        NukleusFactory factory = NukleusFactory.instantiate();
        Properties properties = new Properties();
        properties.setProperty(Configuration.DIRECTORY_PROPERTY_NAME, "target/nuklei-tests");
        Configuration config = new Configuration(properties);
        Nukleus nukleus = factory.create("http2", config);
        assertThat(nukleus, instanceOf(Nukleus.class));
    }

}
