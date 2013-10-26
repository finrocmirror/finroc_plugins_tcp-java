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

import java.net.InetAddress;
import java.util.ArrayList;


/**
 * @author Max Reichardt
 *
 * Info on TCP peer.
 * This is info is exchanged among different processes (and possibly combined).
 */
class PeerInfo {

    /**
     * UUID of peer.
     *
     * A set of interconnected finroc parts maintains a list of peers.
     * To avoid duplicates in this list, the UUID of a part must be uniquely
     * defined among these connected parts.
     *
     * This UUID is not unique with respect to time:
     * If a process is started on the same system and listens on the same port, it will receive the same UUID.
     * This property can be useful - as parts can be terminated and restarted
     * and the others will attempt to reconnect automatically.
     */
    public UUID uuid = new UUID();

    /** Type of peer */
    public final TCP.PeerType peerType;

    /**
     * Vector containing all network addresses of this peer (IPv6 and IPv4),
     * The priority is in descending order (first element is address that should be preferred)
     */
    public ArrayList<InetAddress> addresses = new ArrayList<InetAddress>();

    /** Are we currently connected with this peer? */
    public boolean connected;

    /** True, if there are ongoing attempts to connect to this peer */
    public volatile boolean connecting;

    /** If we're not connected: When was the last time we were connected with this peer? */
    public long lastConnection;

    /** Never forget this peer (typically true for peers that user specified) */
    public boolean neverForget;

    /**
     * Pointer to RemotePart object that represents remote part in this runtime.
     * Set when connection is established
     */
    public RemotePart remotePart;

    /** Program/process name of remote peer */
    public String name;


    public PeerInfo(TCP.PeerType peerType) {
        this.peerType = peerType;
    }

    /** Host name of peer */
    public String hostname() {
        return uuid.hostName;
    }

    public String toString() {
        return name != null ? (name + " (" + uuid.toString() + ")") : uuid.toString();
    }
}
