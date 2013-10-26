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

import java.io.IOException;
import java.net.Socket;

import org.rrlib.finroc_core_utils.jc.log.LogDefinitions;
import org.rrlib.finroc_core_utils.jc.net.TCPConnectionHandler;
import org.rrlib.finroc_core_utils.log.LogDomain;
import org.rrlib.finroc_core_utils.log.LogLevel;

import org.finroc.core.FrameworkElement;
import org.finroc.core.FrameworkElementFlags;
import org.finroc.core.LockOrderLevels;
import org.finroc.plugins.tcp.TCPSettings;

/**
 * @author Max Reichardt
 *
 * TCP Server instance.
 *
 * Module to provide local ports to other robots using a P2P-TCP based
 * communication mechanism.
 */
class TCPServer extends FrameworkElement implements org.rrlib.finroc_core_utils.jc.net.TCPServer {

    /** Port Server runs on */
    private int port;

    /** Try the following ports, if specified port is already occupied? */
    private boolean tryNextPortsIfOccupied;

    /** Is server ready and serving requests? */
    private boolean serving;

    /** Peer that this server belongs to */
    private final TCPPeer peer;

    /** Log domain for this class */
    public static final LogDomain logDomain = LogDefinitions.finroc.getSubDomain("tcp");

    /**
     * @param port Port Server runs on
     * @param tryNextPortsIfOccupied Try the following ports, if specified port is already occupied?
     * @param peer Peer that this server belongs to
     * @param serverListenAddress The address that server is supposed to listen on ("" will enable IPv6)
     */
    public TCPServer(int port, boolean tryNextPortsIfOccupied, TCPPeer peer, String serverListenAddress) {
        //super("tcpserver_" + name + "_" + networkName);
        super(peer.connectionElement, "TCP Server", FrameworkElementFlags.NETWORK_ELEMENT, LockOrderLevels.LEAF_GROUP);
        this.peer = peer;
        TCPSettings.initInstance();
        this.port = port;
        this.tryNextPortsIfOccupied = tryNextPortsIfOccupied;
    }

    @Override
    protected synchronized void prepareDelete() {
        TCPConnectionHandler.removeServer(this, port);
    }

    @Override
    protected void postChildInit() {
        super.postChildInit();
        while (true) {
            serving = TCPConnectionHandler.addServer(this, port);
            if (serving || (!tryNextPortsIfOccupied)) {
                break;
            }
            int nextPort = port + 1;
            log(LogLevel.USER, logDomain, "Port " + port + " occupied - trying " + nextPort);
            port++;
        }

        //AbstractPeerTracker.registerServer(networkName, name, port);
    }

    @Override
    public synchronized void acceptConnection(Socket s, byte firstByte) {
        if (isDeleted()) {
            try {
                s.close();
            } catch (IOException e) {
                log(LogLevel.DEBUG_WARNING, logDomain, e);
            }
            return;
        }
        try {
            //@SuppressWarnings("unused")
            //TCPServerConnection connection = new TCPServerConnection(s, firstByte, this, peer); // TODO
        } catch (Exception e) {
            log(LogLevel.DEBUG_WARNING, logDomain, e);
        }
    }

    @Override
    public boolean accepts(byte firstByte) {
        return (firstByte == TCP.TCP_P2P_ID_BULK) || (firstByte == TCP.TCP_P2P_ID_EXPRESS);
    }

    /**
     * @return Port that server (finally) listens on
     */
    public int getPort() {
        assert(isInitialized()) : "Port is not fixed yet";
        return port;
    }

}
