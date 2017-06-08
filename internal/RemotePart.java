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

import java.util.concurrent.atomic.AtomicInteger;

import org.finroc.core.FrameworkElement;
import org.finroc.core.RuntimeEnvironment;
import org.finroc.core.net.generic_protocol.Connection;
import org.finroc.core.net.generic_protocol.RemoteRuntime;
import org.finroc.core.remote.BufferedModelChanges;
import org.finroc.core.remote.Definitions;
import org.finroc.core.remote.ModelHandler;
import org.rrlib.logging.Log;
import org.rrlib.logging.LogLevel;
import org.rrlib.serialization.BinaryInputStream;

/**
 * @author Max Reichardt
 *
 * Class that represent a remote runtime environment.
 * It creates a proxy port for each shared port in the remote runtime.
 */
public class RemotePart extends RemoteRuntime {

    /** Peer info that this part belongs to */
    final PeerInfo peerInfo;

    /** Peer implementation that this remote part belongs to */
    final TCPPeer peerImplementation;

    /** Number of times disconnect was called, since last connect */
    private final AtomicInteger disconnectCalls = new AtomicInteger(0);


    /** Peer info this part is associated with */
    RemotePart(PeerInfo peerInfo, FrameworkElement parent, TCPPeer peerImplementation, int partnerHandleStampWidth) {
        super(parent, peerInfo.toString(), peerImplementation.structureExchange == Definitions.StructureExchange.FINSTRUCT, partnerHandleStampWidth);
        this.peerInfo = peerInfo;
        this.peerImplementation = peerImplementation;
    }

    /** Whether there is currently a connection to this peer */
    public boolean isConnected() {
        return getPrimaryConnection() != null && (!((TCPConnection)getPrimaryConnection()).isDisconnected());
    }

    @Override
    public void createNewModel() {
        newModelNode = new org.finroc.core.remote.RemoteRuntime(peerInfo.toString(), peerInfo.uuid.toString(), getAdminInterface(), getPrimaryConnection().getReadBufferStream(), handleStampWidth);
        newModelNode.setFlags(RuntimeEnvironment.getInstance().getAllFlags());
    }

    @Override
    public void disconnect() {
        // make sure that disconnect is only called once... prevents deadlocks cleaning up all the threads
        int calls = disconnectCalls.incrementAndGet();
        if (calls > 1) {
            return;
        }

        synchronized (peerImplementation.connectTo) {
            if (getPrimaryConnection() != null) {
                getPrimaryConnection().disconnect();
            }
            if (getExpressConnection() != null) {
                getExpressConnection().disconnect();
            }
            disconnectCalls.set(0);
        }
    }

    @Override
    public void processMessage(TCP.OpCode opCode, BinaryInputStream stream, Connection connection, BufferedModelChanges modelChanges) throws Exception {
        if (opCode == TCP.OpCode.PEER_INFO) {
            while (stream.readBoolean()) {
                PeerInfo peer = peerImplementation.deserializePeerInfo(stream);
                Log.log(LogLevel.DEBUG, this, "Received peer info: " + peer.toString());
                peerImplementation.processIncomingPeerInfo(peer);
            }
        } else {
            super.processMessage(opCode, stream, connection, modelChanges);
        }
    }

    @Override
    public void removeConnection(Connection connection) {
        super.removeConnection(connection);
        peerInfo.lastConnection = System.currentTimeMillis();
    }

    @Override
    public ModelHandler getModelHandler() {
        return peerImplementation.connectionElement.getModelHandler();
    }
}
