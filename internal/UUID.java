//
// You received this file as part of Finroc
// A framework for intelligent robot control
//
// Copyright (C) Finroc GbR (finroc.org)
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
//
//----------------------------------------------------------------------
package org.finroc.plugins.tcp.internal;

import org.rrlib.serialization.BinaryInputStream;
import org.rrlib.serialization.BinaryOutputStream;
import org.rrlib.serialization.BinarySerializable;

/**
 * @author Max Reichardt
 *
 * UUID of TCP peer in network.
 * Currently consists of host name and network port - which seems optimal and simple.
 */
class UUID implements BinarySerializable {

    /** Host name */
    public String hostName;

    /** Server Port (positive numbers) or process id (negative number; for clients) */
    public int port;

    @Override
    public boolean equals(Object other) {
        if (other instanceof UUID) {
            UUID otherUuid = (UUID)other;
            return port == otherUuid.port && hostName.equals(otherUuid.hostName);
        }
        return false;
    }

    @Override
    public void serialize(BinaryOutputStream stream) {
        stream.writeString(hostName);
        stream.writeInt(port);
    }

    @Override
    public void deserialize(BinaryInputStream stream) {
        hostName = stream.readString();
        port = stream.readInt();
    }

    @Override
    public String toString() {
        if (port >= 0) {
            return hostName + ":" + port;
        }
        return hostName + "<" + -port + ">";
    }
}
