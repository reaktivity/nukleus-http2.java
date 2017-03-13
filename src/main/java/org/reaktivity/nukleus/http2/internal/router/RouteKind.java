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
package org.reaktivity.nukleus.http2.internal.router;

import java.util.function.LongSupplier;

import org.agrona.concurrent.status.AtomicCounter;
import org.reaktivity.nukleus.http2.internal.types.control.Role;
import org.reaktivity.nukleus.http2.internal.types.control.State;

public enum RouteKind
{
    INPUT
    {
        @Override
        protected final long nextRef(
            LongSupplier getAndIncrement,
            LongSupplier get)
        {
            // positive, even, non-zero
            getAndIncrement.getAsLong();
            return get.getAsLong() << 1L;
        }
    },

    OUTPUT_ESTABLISHED
    {
        @Override
        protected final long nextRef(
            LongSupplier getAndIncrement,
            LongSupplier get)
        {
            // negative, even, non-zero
            getAndIncrement.getAsLong();
            return get.getAsLong() << 1L | 0x8000000000000000L;
        }
    },

    OUTPUT
    {
        @Override
        protected final long nextRef(
            LongSupplier getAndIncrement,
            LongSupplier get)
        {
            // positive, odd
            return (getAndIncrement.getAsLong() << 1L) | 1L;
        }
    },

    INPUT_ESTABLISHED
    {
        @Override
        protected final long nextRef(
            LongSupplier getAndIncrement,
            LongSupplier get)
        {
            // negative, odd
            return (getAndIncrement.getAsLong() << 1L) | 0x8000000000000001L;
        }
    };

    public final long nextRef(
        AtomicCounter counter)
    {
        return nextRef(counter::increment, counter::get);
    }

    protected abstract long nextRef(
        LongSupplier getAndIncrement,
        LongSupplier get);

    public static RouteKind match(
        long referenceId)
    {
        switch ((int)referenceId & 0x01 | (int)(referenceId >> 32) & 0x80000000)
        {
        case 0x00000001:
            return OUTPUT;
        case 0x80000001:
            return INPUT_ESTABLISHED;
        case 0x00000000:
            return INPUT;
        case 0x80000000:
            return OUTPUT_ESTABLISHED;
        default:
            throw new IllegalArgumentException();
        }
    }

    public static boolean valid(
        long referenceId)
    {
        switch ((int)referenceId & 0x01 | (int)(referenceId >> 32) & 0x80000000)
        {
        case 0x00000001:
        case 0x80000001:
        case 0x00000000:
        case 0x80000000:
            return true;
        default:
            return false;
        }
    }

    public static RouteKind valueOf(
        Role role,
        State state)
    {
        switch (role)
        {
        case INPUT:
            switch (state)
            {
            case NEW:
            case NONE:
                return INPUT;
            case ESTABLISHED:
                return INPUT_ESTABLISHED;
            }
        case OUTPUT:
            switch (state)
            {
            case NEW:
            case NONE:
                return OUTPUT;
            case ESTABLISHED:
                return OUTPUT_ESTABLISHED;
            }
        }

        throw new IllegalArgumentException("Unexpected role and state");
    }
}
