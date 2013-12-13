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

import org.finroc.plugins.tcp.TCPSettings;
import org.rrlib.finroc_core_utils.jc.container.Queueable;
import org.rrlib.serialization.BinaryOutputStream;
import org.rrlib.serialization.MemoryBuffer;

/**
 * @author Max Reichardt
 *
 * A single asynchronous TCP command such as: SUBSCRIBE or UNSUBSCRIBE
 * Is allocated on heap for reasons of simplicity.
 * Should not be used for regular performance-critical TCP calls.
 */
class SerializedTCPCommand extends Queueable {

    /** Buffer storing data */
    private final MemoryBuffer buffer;

    /** Stream for writing to buffer */
    private final BinaryOutputStream stream;

    /** Command's opcode */
    private final TCP.OpCode opcode;

    /**
     * @param opcode Opcode of command
     * @param estimatedSize Estimated size (should rather be a little bit to large to avoid reallocation)
     */
    public SerializedTCPCommand(TCP.OpCode opcode, int estimatedSize) {
        buffer = new MemoryBuffer(estimatedSize);
        stream = new BinaryOutputStream(buffer);
        this.opcode = opcode;
    }

    /**
     * @return Stream for writing to buffer
     */
    public BinaryOutputStream getWriteStream() {
        return stream;
    }

    /**
     * Writes message to stream
     *
     * @param stream Stream to TCP command to
     */
    public void serialize(BinaryOutputStream stream) {
        this.stream.flush();
        assert(TCP.MESSAGE_SIZES[opcode.ordinal()] != TCP.MessageSize.VARIABLE_UP_TO_255_BYTE);
        //System.out.println("Sending TCP Command " + opcode.toString());
        stream.writeEnum(opcode);
        boolean writeSize = TCP.MESSAGE_SIZES[opcode.ordinal()] == TCP.MessageSize.VARIABLE_UP_TO_4GB;
        if (writeSize) {
            stream.writeSkipOffsetPlaceholder();
        }
        stream.write(buffer.getBuffer(), 0, buffer.getSize());
        if (TCPSettings.DEBUG_TCP) {
            stream.writeByte(TCP.DEBUG_TCP_NUMBER);
        }
        if (writeSize) {
            stream.skipTargetHere();
        }
    }


}
