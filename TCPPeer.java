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
package org.finroc.plugins.tcp;

import org.rrlib.finroc_core_utils.jc.ArrayWrapper;
import org.rrlib.finroc_core_utils.jc.annotation.InCpp;
import org.rrlib.finroc_core_utils.jc.annotation.InCppFile;
import org.rrlib.finroc_core_utils.jc.annotation.IncludeClass;
import org.rrlib.finroc_core_utils.jc.annotation.Ptr;
import org.rrlib.finroc_core_utils.jc.annotation.SizeT;
import org.rrlib.finroc_core_utils.jc.container.SafeConcurrentlyIterableList;
import org.rrlib.finroc_core_utils.jc.container.SimpleList;
import org.rrlib.finroc_core_utils.jc.log.LogDefinitions;
import org.rrlib.finroc_core_utils.jc.net.IPSocketAddress;
import org.rrlib.finroc_core_utils.log.LogDomain;
import org.rrlib.finroc_core_utils.log.LogLevel;
import org.finroc.core.CoreFlags;
import org.finroc.core.FrameworkElement;
import org.finroc.core.FrameworkElementTreeFilter;
import org.finroc.core.plugin.ExternalConnection;
import org.finroc.core.port.net.AbstractPeerTracker;

/**
 * @author max
 *
 * A TCP Peer contains a TCP Client and a TCP Server.
 * It is a single peer in a Peer2Peer network.
 *
 * The current implementation is quite crude (many connections and threads).
 * TODO: improve this (client and server should use the same TCPConnections
 * to communicate with another peer).
 */
@IncludeClass(TCPConnection.class)
public class TCPPeer extends ExternalConnection implements AbstractPeerTracker.Listener {

    /** Modes for TCP Peer */
    public enum Mode { FULL, SERVER, CLIENT }

    /** Mode (see enum above) */
    private Mode mode;

    /** TCPServer - if this peer contains a server */
    private TCPServer server;

    /** Name of network server belongs to */
    private final String networkName;

    /** Unique Name of peer in network (empty string, if it has no unique name) */
    private final String name;

    /** Child iterator for internal purposes */
    private ChildIterator ci = new ChildIterator(this);

    /** Filter that specifies which elements in remote runtime environment we're interested in */
    private final FrameworkElementTreeFilter filter;

    /** Peer tracker that we use for discovering network nodes */
    private PeerList tracker;

    /** Delete all ports when client disconnects? */
    private final boolean deletePortsOnDisconnect;

    /** TreeFilter for different applications */
    public static final FrameworkElementTreeFilter GUI_FILTER = new FrameworkElementTreeFilter(CoreFlags.STATUS_FLAGS | CoreFlags.NETWORK_ELEMENT, CoreFlags.READY | CoreFlags.PUBLISHED);
    public static final FrameworkElementTreeFilter DEFAULT_FILTER = new FrameworkElementTreeFilter(CoreFlags.STATUS_FLAGS | CoreFlags.NETWORK_ELEMENT | CoreFlags.SHARED | CoreFlags.IS_PORT, CoreFlags.READY | CoreFlags.PUBLISHED | CoreFlags.SHARED | CoreFlags.IS_PORT);
    public static final FrameworkElementTreeFilter ALL_AND_EDGE_FILTER = new FrameworkElementTreeFilter(CoreFlags.STATUS_FLAGS, CoreFlags.READY | CoreFlags.PUBLISHED, true);

    /** All active connections connected to this peer */
    public SafeConcurrentlyIterableList<TCPConnection> connections = new SafeConcurrentlyIterableList<TCPConnection>(10, 4);

    /** Log domain for this class */
    @InCpp("_RRLIB_LOG_CREATE_NAMED_DOMAIN(logDomain, \"tcp\");")
    public static final LogDomain logDomain = LogDefinitions.finroc.getSubDomain("tcp");

    /**
     * Constructor for client connections
     *
     * @param networkName Name of network that peer belongs to OR network address of one peer that belongs to P2P network
     * @param filter Filter that specifies which elements in remote runtime environment we're interested in. (CLIENT and FULL only)
     */
    public TCPPeer(String networkName, FrameworkElementTreeFilter filter) {
        this(networkName, "", Mode.CLIENT, -1, filter, filter.isAcceptAllFilter());
    }

    /**
     * @param networkName Name of network that peer belongs to OR network address of one peer that belongs to P2P network
     * @param uniquePeerName Unique Name of TCP server in network (provide empty string, if its not practical to provide unique one)
     * @param mode Mode (see enum above)
     * @param preferredServerPort Port that we will try to open for server. Will try the next ones if not available. (SERVER and FULL only)
     * @param filter Filter that specifies which elements in remote runtime environment we're interested in. (CLIENT and FULL only)
     * @param deletePortsOnDisconnect Delete all ports when client disconnects?
     */
    public TCPPeer(String networkName, String uniquePeerName, Mode mode, int preferredServerPort, FrameworkElementTreeFilter filter, boolean deletePortsOnDisconnect) {
        super("TCP", networkName);
        this.networkName = networkName;
        this.name = uniquePeerName;
        this.mode = mode;
        this.filter = filter;
        this.deletePortsOnDisconnect = deletePortsOnDisconnect;

        if (isServer()) {
            server = new TCPServer(preferredServerPort, true, this);
        }

    }

    /**
     * @return Does peer provide a server for connecting?
     */
    public boolean isServer() {
        return mode == Mode.SERVER || mode == Mode.FULL;
    }

    /**
     * @return Does peer act as a client?
     */
    public boolean isClient() {
        return mode == Mode.CLIENT || mode == Mode.FULL;
    }

    public void postChildInit() {
        tracker = new PeerList(isServer() ? server.getPort() : -1, getLockOrder() + 1);
        if (isServer()) {
            tracker.registerServer(networkName, name, server.getPort());
        }
    }

    /** Start connecting
     * @throws Exception */
    public void connect() throws Exception {
        assert(isReady());
        connectImpl(networkName, false);
        postConnect(networkName);
    }

    @Override
    protected synchronized void prepareDelete() {
        if (isServer() && tracker != null) {
            tracker.unregisterServer(networkName, name);
        }
        try {
            disconnect();
        } catch (Exception e) {
            log(LogLevel.LL_DEBUG_WARNING, logDomain, e);
        }
        if (tracker != null) {
            tracker.delete();
        }
    }

    @Override
    public void nodeDiscovered(IPSocketAddress isa, String name) {
        synchronized (tracker) {
            if (getFlag(CoreFlags.DELETED)) {
                return;
            }

            // add port & connect
            RemoteServer rs = new RemoteServer(isa, name, this, filter, this);
            rs.init();
        }
    }

    @Override
    public Object nodeRemoved(IPSocketAddress isa, String name) {
        synchronized (tracker) {
            if (getFlag(CoreFlags.DELETED)) {
                return null;
            }

            // remove port & disconnect
            ci.reset(this, false);
            for (FrameworkElement fe = ci.next(); fe != null; fe = ci.next()) {
                if (fe == server || fe.isPort()) {
                    continue;
                }
                RemoteServer rs = (RemoteServer)fe;
                if (rs.getPartnerAddress().equals(isa) && (!rs.deletedSoon()) && (!rs.isDeleted())) {
                    rs.earlyDeletingPreparations();
                    return rs;
                }
            }
            log(LogLevel.LL_WARNING, logDomain, "TCPClient warning: Node " + name + " not found");
            return null;
        }
    }

    @Override
    @InCppFile
    public void nodeRemovedPostLockProcess(Object obj) {
        ((RemoteServer)obj).managedDelete();
    }

    @Override
    protected synchronized void connectImpl(String address, boolean sameAddress) throws Exception {

        synchronized (tracker) {

            assert(isReady());
            tracker.addListener(this);

            if (sameAddress) {

                ci.reset(this);
                FrameworkElement fe = null;
                while ((fe = ci.next()) != null) {
                    if (fe instanceof RemoteServer) {
                        RemoteServer rs = (RemoteServer)fe;
                        synchronized (rs) {
                            if (rs.isReady() && (!rs.deletedSoon())) {
                                rs.reconnect();
                            }
                        }
                    }
                }

            } else {

                // is this an ip address?
                int idx = address.indexOf(":");
                boolean ip = false;
                if (idx > 0) {
                    String host = address.substring(0, idx);
                    String port = address.substring(idx + 1);

                    ip = true;
                    for (@SizeT int i = 0; i < port.length(); i++) {
                        if (!Character.isDigit(port.charAt(i))) {
                            ip = false;
                        }
                    }

                    // we don't want to connect to ourselves
                    if ((host.equals("localhost") || host.startsWith("127.0")) && server != null && Integer.parseInt(port) == server.getPort()) {
                        return;
                    }

                    if (ip) {
                        IPSocketAddress isa = new IPSocketAddress(host, Integer.parseInt(port));
                        tracker.addPeer(isa, false);
                        RemoteServer rs = new RemoteServer(isa, address, this, filter, this);
                        rs.init();
                        return;
                    }
                }
            }
        }

        //this.setDescription("tcp_" + address);
        //  tracker = new PeerTracker(address, this);
        //  tracker = new FixedPeerList();
    }

    @Override
    protected synchronized void disconnectImpl() throws Exception {
        if (tracker != null) {
            synchronized (tracker) {
                tracker.removeListener(this);
            }
            // now we can be sure that no new nodes will be added
        }
        //tracker.delete();

        ci.reset(this);
        FrameworkElement fe = null;
        while ((fe = ci.next()) != null) {
            if (fe.isReady() && (fe instanceof RemoteServer)) {
                RemoteServer rs = (RemoteServer)fe;
                synchronized (rs) {
                    if (rs.isReady() && (!rs.deletedSoon())) {
                        rs.temporaryDisconnect();
                    }
                }
            }
        }
    }

    /**
     * @return Peer's list of other peers
     */
    public PeerList getPeerList() {
        return tracker;
    }

    /**
     * @param connection Active connection
     */
    public void addConnection(TCPConnection connection) {
        connections.add(connection, false);
    }

    /**
     * @param connection Active connection
     */
    public void removeConnection(TCPConnection connection) {
        connections.remove(connection);
    }

    /**
     *  Notifies writers of all active connections connected to this peer
     */
    public void notifyAllWriters() {
        @Ptr ArrayWrapper<TCPConnection> it = connections.getIterable();
        for (int i = 0, n = it.size(); i < n; i++) {
            TCPConnection tc = it.get(i);
            if (tc != null) {
                tc.notifyWriter();
            }
        }
    }

    @Override
    public float getConnectionQuality() {
        float worst = 1.0f;
        ChildIterator ci = new ChildIterator(this);
        for (FrameworkElement fe = ci.next(); fe != null; fe = ci.next()) {
            if (fe == server || fe.isPort()) {
                continue;
            }
            RemoteServer rs = (RemoteServer)fe;
            if (rs.isReady() && (!rs.deletedSoon())) {
                worst = Math.min(worst, rs.getConnectionQuality());
            }
        }
        return worst;
    }

    @Override
    public String getStatus(boolean detailed) {
        String s = super.getConnectionAddress();
        if (!detailed) {
            return s;
        } else {
            SimpleList<String> addStuff = new SimpleList<String>();
            ChildIterator ci = new ChildIterator(this);
            for (FrameworkElement fe = ci.next(); fe != null; fe = ci.next()) {
                if (fe == server || (!fe.isReady()) || fe.isPort()) {
                    continue;
                }
                RemoteServer rs = (RemoteServer)fe;
                if (rs.deletedSoon()) {
                    continue;
                }
                String tmp = rs.getPartnerAddress().toString();
                if (tmp.equals(s)) {
                    addStuff.insert(0, rs.getPingString());
                } else {
                    addStuff.add(rs.getPartnerAddress().toString() + " " + rs.getPingString());
                }
            }
            for (@SizeT int i = 0; i < addStuff.size(); i++) {
                s += (i == 0) ? " (" : "; ";
                s += addStuff.get(i);
            }
            return s + ")";
        }
    }

    /**
     * @return Is this a connection/client used for administration?
     */
    public boolean isAdminConnection() {
        return filter.isAcceptAllFilter();
    }

    /**
     * @return Delete all ports when client disconnects?
     */
    public boolean deletePortsOnDisconnect() {
        return deletePortsOnDisconnect;
    }
}
