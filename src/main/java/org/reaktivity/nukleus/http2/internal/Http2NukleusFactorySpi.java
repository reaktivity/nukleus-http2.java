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

import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.nukleus.http2.internal.conductor.Conductor;
import org.reaktivity.nukleus.http2.internal.router.Router;
import org.reaktivity.nukleus.http2.internal.watcher.Watcher;

public final class Http2NukleusFactorySpi implements NukleusFactorySpi
{
    @Override
    public String name()
    {
        return Http2Nukleus.NAME;
    }

    @Override
    public Nukleus create(Configuration config)
    {
        Context context = new Context();
        context.conclude(config);

        Conductor conductor = new Conductor(context);
        Watcher watcher = new Watcher(context);
        Router router = new Router(context);

        conductor.setRouter(router);
        watcher.setRouter(router);
        router.setConductor(conductor);

        return new Http2Nukleus(conductor, watcher, router, context);
    }
}
