/**
 * You received this file as part of an advanced experimental
 * robotics framework prototype ('finroc')
 *
 * Copyright (C) 2007-2010 Max Reichardt,
 *   Robotics Research Lab, University of Kaiserslautern
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.finroc.plugin.tcp;

import org.finroc.core.buffer.CoreOutput;
import org.finroc.core.buffer.CoreInput;
import org.finroc.core.portdatabase.SerializableReusable;

/**
 * @author max
 *
 * A single asynchronous TCP command such as: SUBSCRIBE or UNSUBSCRIBE
 */
public class TCPCommand extends SerializableReusable {

    /** OpCode - see TCP class */
    public byte opCode;

    /** Handle of remote port */
    public int remoteHandle;

    /** Strategy to use/request */
    public int strategy;

    /** Minimum network update interval */
    public short updateInterval;

    /** Handle of local port */
    public int localIndex;

    /** Data type uid */
    public short datatypeuid;

    /** Subscribe with reverse push strategy? */
    public boolean reversePush;

    @Override
    public void deserialize(CoreInput is) {
        throw new RuntimeException("Unsupported - not needed - server decodes directly (more efficient)");
    }

    @Override
    public void serialize(CoreOutput os) {
        os.writeByte(opCode);
        switch (opCode) {
        case TCP.SUBSCRIBE:
            os.writeInt(remoteHandle);
            os.writeShort(strategy);
            os.writeBoolean(reversePush);
            os.writeShort(updateInterval);
            os.writeInt(localIndex);
            break;
        case TCP.UNSUBSCRIBE:
            os.writeInt(remoteHandle);
            break;
        case TCP.UPDATETIME:
            os.writeInt(datatypeuid);
            os.writeInt(updateInterval);
            break;
        }
    }

    /*Cpp
    virtual void customDelete(bool b) {
        Reusable::customDelete(b);
    }
     */
}
