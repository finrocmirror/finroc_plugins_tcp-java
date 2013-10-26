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
package org.finroc.plugins.tcp;

import org.finroc.core.datatype.FrameworkElementInfo;
import org.finroc.core.plugin.ExternalConnection;
import org.finroc.plugins.tcp.internal.TCP;
import org.finroc.plugins.tcp.internal.TCPPeer;
import org.rrlib.finroc_core_utils.log.LogLevel;

/**
 * @author Max Reichardt
 *
 * A TCP Peer contains a TCP Client and a TCP Server.
 * It is a single peer in a Peer2Peer network.
 */
public class Peer extends ExternalConnection {

    /** Default network name */
    public static final String DEFAULT_CONNECTION_NAME = "localhost:4444";

    /** Peer implementation */
    private final TCPPeer implementation;

    /**
     * Creates full peer with client and server
     *
     * @param peerName Name of peer. Will be displayed in tooling and status messages. Does not need to be unique. Typically the program/process name.
     * @param networkConnection Name of network that peer belongs to OR network address of one peer that belongs to P2P network
     * @param preferredServerPort Port that we will try to open for server. Will try the next ones if not available (SERVER and FULL only)
     * @param tryNextPortsIfOccupied Try the following ports, if specified port is already occupied?
     * @param autoConnectToAllPeers Auto-connect to all peers that become known?
     * @param serverListenAddress The address that server is supposed to listen on ("" will enable IPv6)
     */
    public Peer(String peerName, String networkConnection, int preferredServerPort, boolean tryNextPortsIfOccupied, boolean autoConnectToAllPeers, String serverListenAddress) {
        super("TCP", networkConnection);
        if (serverListenAddress == null) {
            serverListenAddress = "0.0.0.0";
        }
        implementation = new TCPPeer(this, peerName, TCP.PeerType.FULL, FrameworkElementInfo.StructureExchange.SHARED_PORTS, networkConnection,
                                     preferredServerPort, tryNextPortsIfOccupied, autoConnectToAllPeers, serverListenAddress);
    }

    /**
     * Creates server-only peer
     *
     * @param peerName Name of peer. Will be displayed in tooling and status messages. Does not need to be unique. Typically the program/process name.
     * @param preferredServerPort Port that we will try to open for server. Will try the next ones if not available (SERVER and FULL only)
     * @param tryNextPortsIfOccupied Try the following ports, if specified port is already occupied?
     * @param serverListenAddress The address that server is supposed to listen on ("" will enable IPv6)
     */
    public Peer(String peerName, int preferredServerPort, boolean tryNextPortsIfOccupied, String serverListenAddress) {
        super("TCP", "127.0.0.1");
        if (serverListenAddress == null) {
            serverListenAddress = "0.0.0.0";
        }
        implementation = new TCPPeer(this, peerName, TCP.PeerType.SERVER_ONLY, FrameworkElementInfo.StructureExchange.SHARED_PORTS, "",
                                     preferredServerPort, tryNextPortsIfOccupied, false, serverListenAddress);
    }

    /**
     * Creates client-only peer
     *
     * @param peerName Name of peer. Will be displayed in tooling and status messages. Does not need to be unique. Typically the program/process name.
     * @param networkConnection Name of network that peer belongs to OR network address of one peer that belongs to P2P network
     * @param structureExchange Amount of structure the client is interested in
     * @param autoConnectToAllPeers Auto-connect to all peers that become known?
     */
    public Peer(String peerName, String networkConnection, FrameworkElementInfo.StructureExchange structureExchange, boolean autoConnectToAllPeers) {
        super("TCP", networkConnection);
        implementation = new TCPPeer(this, peerName, TCP.PeerType.CLIENT_ONLY, structureExchange, networkConnection, -1, false, autoConnectToAllPeers, "");
    }


    /** Starts actively connecting to the specified network */
    public void connect() throws Exception {
        implementation.connect();
    }

    @Override
    protected void connectImpl(String address, boolean sameAddress) throws Exception {
        implementation.connectImpl(address, sameAddress);
    }

    @Override
    protected void disconnectImpl() throws Exception {
        implementation.disconnectImpl();
    }

    @Override
    public float getConnectionQuality() {
        return implementation.getConnectionQuality();
    }

    @Override
    public String getStatus(boolean detailed) {
        return implementation.getStatus(detailed);
    }

    @Override
    protected synchronized void prepareDelete() {
        /*if (isServer() && tracker != null) {
            tracker.unregisterServer(networkName, name);
        }*/
        implementation.prepareDelete();
        try {
            this.disconnect();
        } catch (Exception e) {
            log(LogLevel.DEBUG_WARNING, logDomain, e);
        }
        /*if (tracker != null) {
            tracker.delete();
        }*/
    }
}
