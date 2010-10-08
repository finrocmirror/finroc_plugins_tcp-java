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

import org.finroc.jc.annotation.CppInclude;
import org.finroc.jc.annotation.CppUnused;
import org.finroc.jc.annotation.InCpp;
import org.finroc.jc.annotation.Managed;
import org.finroc.jc.annotation.Ptr;
import org.finroc.jc.log.LogDefinitions;
import org.finroc.jc.net.IOException;
import org.finroc.jc.net.NetSocket;
import org.finroc.jc.net.TCPConnectionHandler;
import org.finroc.log.LogDomain;
import org.finroc.log.LogLevel;

import org.finroc.core.CoreFlags;
import org.finroc.core.FrameworkElement;
import org.finroc.core.LockOrderLevels;

/**
 * @author max
 *
 * TCP Server instance.
 *
 * Module to provide local ports to other robots using a P2P-TCP based
 * communication mechanism.
 */
@CppInclude("TCPPeer.h")
public class TCPServer extends FrameworkElement implements org.finroc.jc.net.TCPServer {

    /** Port Server runs on */
    private int port;

    /** Try the following ports, if specified port is already occupied? */
    private boolean tryNextPortsIfOccupied;

    /** Is server ready and serving requests? */
    private boolean serving;

    /** Peer that this server belongs to */
    private final TCPPeer peer;

    /** Log domain for this class */
    @InCpp("_RRLIB_LOG_CREATE_NAMED_DOMAIN(logDomain, \"tcp\");")
    public static final LogDomain logDomain = LogDefinitions.finroc.getSubDomain("tcp");

    /**
     * @param port Port Server runs on
     * @param tryNextPortsIfOccupied Try the following ports, if specified port is already occupied?
     * @param peer Peer that this server belongs to
     */
    public TCPServer(int port, boolean tryNextPortsIfOccupied, TCPPeer peer) {
        //super("tcpserver_" + name + "_" + networkName);
        super("TCP Server", peer, CoreFlags.ALLOWS_CHILDREN | CoreFlags.NETWORK_ELEMENT, LockOrderLevels.LEAF_GROUP);
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
            log(LogLevel.LL_USER, logDomain, "Port " + port + " occupied - trying " + nextPort);
            port++;
        }

        //AbstractPeerTracker.registerServer(networkName, name, port);
    }

    @Override
    public synchronized void acceptConnection(NetSocket s, byte firstByte) {
        if (isDeleted()) {
            try {
                s.close();
            } catch (IOException e) {
                log(LogLevel.LL_DEBUG_WARNING, logDomain, e);
            }
            return;
        }
        try {
            @SuppressWarnings("unused") @CppUnused
            @Ptr @Managed TCPServerConnection connection = new TCPServerConnection(s, firstByte, this, peer);
        } catch (Exception e) {
            log(LogLevel.LL_DEBUG_WARNING, logDomain, e);
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
