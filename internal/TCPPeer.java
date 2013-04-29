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
package org.finroc.plugins.tcp.internal;

import java.lang.management.ManagementFactory;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.rrlib.finroc_core_utils.jc.log.LogDefinitions;
import org.rrlib.finroc_core_utils.jc.log.LogUser;
import org.rrlib.finroc_core_utils.jc.thread.LoopThread;
import org.rrlib.finroc_core_utils.log.LogDomain;
import org.rrlib.finroc_core_utils.log.LogLevel;
import org.finroc.core.FrameworkElement;
import org.finroc.core.FrameworkElement.ChildIterator;
import org.finroc.core.datatype.FrameworkElementInfo;
import org.finroc.core.plugin.ExternalConnection;
import org.finroc.core.remote.ModelNode;
import org.finroc.plugins.tcp.internal.TCP.PeerType;

/**
 * @author Max Reichardt
 *
 * A TCP Peer contains a TCP Client and a TCP Server.
 * It is a single peer in a Peer2Peer network.
 *
 * The current implementation is quite crude (many connections and threads).
 * TODO: improve this (client and server should use the same TCPConnections
 * to communicate with another peer).
 */
public class TCPPeer extends LogUser { /*implements AbstractPeerTracker.Listener*/

    /** Framework element associated with server */
    final ExternalConnection connectionElement;

    /** Name of network that peer belongs to OR network address of one peer that belongs to P2P network */
    final String networkConnection;

    /** Vector containing all network addresses this peer should try to connect to */
    final ArrayList<InetSocketAddress> connectTo = new ArrayList<InetSocketAddress>();

    /** Info on this peer */
    final PeerInfo thisPeer;

    /** Amount of structure this client is interested in */
    final FrameworkElementInfo.StructureExchange structureExchange;

    /**
     * List of network peers that can be connected to
     * Each entry contains reference to RemotePart instance as soon as a
     * connection to remote part is established.
     */
    final ArrayList<PeerInfo> otherPeers = new ArrayList<PeerInfo>();

    /** Revision of peer information */
    int peerListRevision;

    /** Primary TCP thread that does all the socket related work for this peer */
    final TCPManagementThread managementThread;

    /** TCPServer - if this peer contains a server */
    private final TCPServer server;

    /** Child iterator for internal purposes */
    private final ChildIterator ci;

    /** Delete all ports when client disconnects? */
    private final boolean deletePortsOnDisconnect;

    /**
     * Actively connect to specified network?
     * False initially; true after connect() has been called
     */
    boolean activelyConnect = false;

    /** Root node of of model of remote runtime environments */
    ModelNode modelRootNode;

    /** Log domain for this class */
    public static final LogDomain logDomain = LogDefinitions.finroc.getSubDomain("tcp");


    /**
     * @param frameworkElement Framework element associated with peer
     * @param peerName Name of peer. Will be displayed in tooling and status messages. Does not need to be unique. Typically the program/process name.
     * @param peerType Type of peer to be created
     * @param structureExchange Amount of structure the client is interested in
     * @param networkConnection Name of network that peer belongs to OR network address of one peer that belongs to P2P network
     * @param preferredServerPort Port that we will try to open for server. Will try the next ones if not available (SERVER and FULL only)
     * @param tryNextPortsIfOccupied Try the following ports, if specified port is already occupied?
     * @param autoConnectToAllPeers Auto-connect to all peers that become known?
     * @param serverListenAddress The address that server is supposed to listen on ("" will enable IPv6)
     */
    public TCPPeer(ExternalConnection frameworkElement, String peerName, TCP.PeerType peerType, FrameworkElementInfo.StructureExchange structureExchange, String networkConnection,
                   int preferredServerPort, boolean tryNextPortsIfOccupied, boolean autoConnectToAllPeers, String serverListenAddress) {
        this.connectionElement = frameworkElement;
        thisPeer = new PeerInfo(peerType);
        thisPeer.name = peerName;
        this.structureExchange = structureExchange;
        this.networkConnection = networkConnection;
        this.deletePortsOnDisconnect = true; // TODO
        ci = new ChildIterator(frameworkElement);

        server = isServer() ? new TCPServer(preferredServerPort, tryNextPortsIfOccupied, this, serverListenAddress) : null;

        try {
            thisPeer.uuid.hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            thisPeer.uuid.hostName = "No host name";
            log(LogLevel.LL_ERROR, logDomain, "Error retrieving host name.", e);
        }
        if (peerType == TCP.PeerType.CLIENT_ONLY) {
            // set to negative process id
            try {
                String pid = ManagementFactory.getRuntimeMXBean().getName();
                thisPeer.uuid.port = -Integer.parseInt(pid.substring(0, pid.indexOf("@")));
            } catch (Exception e) {
                thisPeer.uuid.port = -1;
                log(LogLevel.LL_ERROR, logDomain, "Error retrieving process id.", e);
            }
        } else {
            thisPeer.uuid.port = server.getPort();
        }

        managementThread = new TCPManagementThread();
        managementThread.start();
    }

    /**
     * @return Does peer provide a server for connecting?
     */
    public boolean isServer() {
        return thisPeer.peerType == TCP.PeerType.SERVER_ONLY || thisPeer.peerType == TCP.PeerType.FULL;
    }

    /**
     * @return Does peer act as a client?
     */
    public boolean isClient() {
        return thisPeer.peerType == TCP.PeerType.CLIENT_ONLY || thisPeer.peerType == TCP.PeerType.FULL;
    }

//    @Override
//    public void postChildInit() {
//        tracker = new PeerList(isServer() ? server.getPort() : -1, frameworkElement.getLockOrder() + 1);
//        if (isServer()) {
//            tracker.registerServer(networkName, name, server.getPort());
//        }
//    }

    /** Start connecting */
    public void connect() throws Exception {
        assert(connectionElement.isReady());
        if (!activelyConnect) {
            connectImpl(networkConnection, false);
            connectionElement.postConnect(networkConnection);
        }
    }

//    @Override
//    public void nodeDiscovered(IPSocketAddress isa, String name) {
//        synchronized (tracker) {
//            if (getFlag(CoreFlags.DELETED)) {
//                return;
//            }
//
//            // add port & connect
//            RemoteServer rs = new RemoteServer(isa, name, this, filter, this);
//            rs.init();
//        }
//    }
//
//    @Override
//    public Object nodeRemoved(IPSocketAddress isa, String name) {
//        synchronized (tracker) {
//            if (getFlag(CoreFlags.DELETED)) {
//                return null;
//            }
//
//            // remove port & disconnect
//            ci.reset(this, false);
//            for (FrameworkElement fe = ci.next(); fe != null; fe = ci.next()) {
//                if (fe == server || fe.isPort()) {
//                    continue;
//                }
//                RemoteServer rs = (RemoteServer)fe;
//                if (rs.getPartnerAddress().equals(isa) && (!rs.deletedSoon()) && (!rs.isDeleted())) {
//                    rs.earlyDeletingPreparations();
//                    return rs;
//                }
//            }
//            log(LogLevel.LL_WARNING, logDomain, "TCPClient warning: Node " + name + " not found");
//            return null;
//        }
//    }
//
//    @Override
//    public void nodeRemovedPostLockProcess(Object obj) {
//        ((RemoteServer)obj).managedDelete();
//    }

    public synchronized void connectImpl(String address, boolean sameAddress) throws Exception {

        synchronized (connectTo) {

            assert(connectionElement.isReady());
            activelyConnect = true;
            modelRootNode = new ModelNode("TCP");
            connectionElement.getModelHandler().setModelRoot(modelRootNode);

            if (address.length() > 0) {
                int idx = address.lastIndexOf(":");
                boolean ip = false;
                if (idx > 0) {
                    String host = address.substring(0, idx);
                    String port = address.substring(idx + 1);

                    ip = true;
                    for (int i = 0; i < port.length(); i++) {
                        if (!Character.isDigit(port.charAt(i))) {
                            ip = false;
                        }
                    }

                    // we don't want to connect to ourselves
                    if ((host.equals("localhost") || host.startsWith("127.0")) && server != null && Integer.parseInt(port) == server.getPort()) {
                        return;
                    }

                    if (ip) {
                        InetSocketAddress isa = new InetSocketAddress(host, Integer.parseInt(port));
                        if (!connectTo.contains(isa)) {
                            connectTo.add(isa);
                        }
                        return;
                    }
                }
            }

            //tracker.addListener(this);
//            if (sameAddress) {
//
//                ci.reset(frameworkElement);
//                FrameworkElement fe = null;
//                while ((fe = ci.next()) != null) {
//                    if (fe instanceof RemoteServer) {
//                        RemoteServer rs = (RemoteServer)fe;
//                        synchronized (rs) {
//                            if (rs.isReady() && (!rs.deletedSoon())) {
//                                rs.reconnect();
//                            }
//                        }
//                    }
//                }
//
//            } else {
//
//                // is this an ip address?
//                int idx = address.indexOf(":");
//                boolean ip = false;
//                if (idx > 0) {
//                    String host = address.substring(0, idx);
//                    String port = address.substring(idx + 1);
//
//                    ip = true;
//                    for (int i = 0; i < port.length(); i++) {
//                        if (!Character.isDigit(port.charAt(i))) {
//                            ip = false;
//                        }
//                    }
//
//                    // we don't want to connect to ourselves
//                    if ((host.equals("localhost") || host.startsWith("127.0")) && server != null && Integer.parseInt(port) == server.getPort()) {
//                        return;
//                    }
//
//                    if (ip) {
//                        InetSocketAddress isa = new InetSocketAddress(host, Integer.parseInt(port));
//                        //tracker.addPeer(isa, false);
//                        RemoteServer rs = new RemoteServer(isa, address, frameworkElement, this);
//                        //rs.init(); done when connected (to name after uuid)
//                        return;
//                    }
//                }
//            }
        }

        //this.setDescription("tcp_" + address);
        //  tracker = new PeerTracker(address, this);
        //  tracker = new FixedPeerList();
    }

    public synchronized void disconnectImpl() throws Exception {
        /*if (tracker != null) {
            synchronized (tracker) {
                tracker.removeListener(this);
            }
            // now we can be sure that no new nodes will be added
        }*/
        //tracker.delete();
        activelyConnect = false;

        ci.reset(connectionElement);
        FrameworkElement fe = null;
        while ((fe = ci.next()) != null) {
            if (fe.isReady() && (fe instanceof RemotePart)) {
                RemotePart rs = (RemotePart)fe;
                synchronized (connectTo) {
                    synchronized (rs) {
                        if (rs.isReady()) {
                            rs.disconnect();
                        }
                    }
                }
            }
        }
    }

//    /**
//     * @return Peer's list of other peers
//     */
//    public PeerList getPeerList() {
//        return tracker;
//    }

    public float getConnectionQuality() {
        float worst = 1.0f;
        int childCount = 0;
        ChildIterator ci = new ChildIterator(connectionElement);
        for (FrameworkElement fe = ci.next(); fe != null; fe = ci.next()) {
            if (fe instanceof RemotePart && fe.isReady()) {
                RemotePart rs = (RemotePart)fe;
                if (!rs.peerInfo.connected) {
                    return 0;
                }
                worst = Math.min(worst, rs.getConnectionQuality());
                childCount++;
            }
        }
        return childCount == 0 ? 0 : worst;
    }

    public String getStatus(boolean detailed) {
        String s = connectionElement.getConnectionAddress();
        if (!detailed) {
            return s;
        } else {
            ArrayList<String> addStuff = new ArrayList<String>();
            ChildIterator ci = new ChildIterator(connectionElement);
            for (FrameworkElement fe = ci.next(); fe != null; fe = ci.next()) {
                if (fe instanceof RemotePart && fe.isReady()) {
                    RemotePart rs = (RemotePart)fe;
                    if (!rs.peerInfo.connected) {
                        continue;
                    }
                    String tmp = rs.peerInfo.uuid.toString();
                    if (tmp.equals(s)) {
                        addStuff.add(0, rs.getPingString());
                    } else {
                        addStuff.add(rs.peerInfo.uuid.toString() + " " + rs.getPingString());
                    }
                }
            }
            for (int i = 0; i < addStuff.size(); i++) {
                s += (i == 0) ? " (" : "; ";
                s += addStuff.get(i);
            }
            return s + (addStuff.size() == 0 ? "" : ")");
        }
    }

    /**
     * @return Is this a connection/client used for administration?
     */
    public boolean isAdminConnection() {
        return structureExchange == FrameworkElementInfo.StructureExchange.FINSTRUCT;
    }

    /**
     * @return Delete all ports when client disconnects?
     */
    public boolean deletePortsOnDisconnect() {
        return deletePortsOnDisconnect;
    }

    /**
     * Adds a address for this peer (if not already set)
     *
     * @param myAddress Address to add
     */
    public void addOwnAddress(InetAddress myAddress) {
        synchronized (connectTo) {
            if (thisPeer.addresses.contains(myAddress)) {
                return;
            }
            thisPeer.addresses.add(myAddress);
            peerListRevision++;
        }
    }

    /**
     * Gets remote part with specified UUID.
     * If no such part has been registered yet, creates a new one.
     *
     * @param uuid UUID of remote part
     * @param peerType Peer type
     * @param peerName Name of peer. Will be displayed in tooling and status messages. Does not need to be unique. Typically the program/process name.
     * @param address IP address of remote part
     * @param neverForget Is this a remote peer to never forget?
     * @return Pointer to remote part
     */
    public RemotePart getRemotePart(UUID uuid, TCP.PeerType peerType, String peerName, InetAddress address, boolean neverForget) throws Exception {
        synchronized (connectTo) {
            if (uuid.equals(this.thisPeer.uuid)) {
                log(LogLevel.LL_ERROR, logDomain, "Remote part has the same UUID as this one: " + uuid.toString());
                throw new ConnectException("Remote part has the same UUID as this one: " + uuid.toString());
            }

            for (PeerInfo info : otherPeers) {
                if (uuid.equals(info.uuid)) {
                    if (info.remotePart == null) {
                        info.remotePart = new RemotePart(info, connectionElement, this);
                        info.remotePart.init();
                    }
                    info.neverForget |= neverForget;
                    info.name = peerName;
                    if (!info.addresses.contains(address)) {
                        info.addresses.add(address);
                    }
                    return info.remotePart;
                }
            }

            PeerInfo info = new PeerInfo(peerType);
            otherPeers.add(info);
            info.addresses.add(address);
            info.uuid = uuid;
            info.name = peerName;
            info.neverForget = neverForget;
            info.remotePart = new RemotePart(info, connectionElement, this);
            info.remotePart.init();
            peerListRevision++;
            return info.remotePart;
        }
    }

    /**
     * @param address An internet address
     * @return Any peer associated with this address. Null if no info on peer with this address is available.
     */
    private PeerInfo findPeerInfoFor(InetSocketAddress address) {
        synchronized (connectTo) {
            for (PeerInfo info : otherPeers) {
                if (info.uuid.port == address.getPort()) {
                    for (InetAddress peerAddress : info.addresses) {
                        if (peerAddress.equals(address.getAddress())) {
                            return info;
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Called when Peer is deleted
     */
    public void prepareDelete() {
        managementThread.stopThread();
        try {
            managementThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (modelRootNode != null) {
            connectionElement.getModelHandler().removeNode(modelRootNode);
        }
        modelRootNode = null;
    }

    /** Thread that does all the management work for this peer */
    class TCPManagementThread extends LoopThread {

        public TCPManagementThread() {
            super(500);
            setName("TCP Thread");
        }

        @Override
        public void mainLoopCallback() throws Exception {

            // Connect to other peers
            if (activelyConnect) {
                synchronized (connectTo) {
                    for (final InetSocketAddress address : connectTo) {
                        new ConnectorThread(address).start();
                    }
                    connectTo.clear();

                    for (PeerInfo info : otherPeers) {
                        if ((!info.connected) && (!info.connecting) && (info.peerType != PeerType.CLIENT_ONLY)) {
                            new PeerConnectorThread(info).start();
                        }
                    }
                }
            }
        }
    }

    /** Connects to an address */
    class ConnectorThread extends LoopThread {

        /** address to connect to */
        final InetSocketAddress connectTo;

        /** model node for status and element we're connecting to */
        final ModelNode modelNode;

        ConnectorThread(InetSocketAddress connectTo) {
            super(500);
            setName("ConnectorThread " + TCP.formatInetSocketAddress(connectTo));
            this.connectTo = connectTo;
            modelNode = new ModelNode("Looking for " + TCP.formatInetSocketAddress(connectTo) + "...");
            connectionElement.getModelHandler().addNode(modelRootNode, modelNode);
        }

        @Override
        public void mainLoopCallback() throws Exception {
            PeerInfo peerInfo = findPeerInfoFor(connectTo);
            if ((!activelyConnect) || (peerInfo != null)) {
                stopThread();
                if (modelRootNode != null) {
                    connectionElement.getModelHandler().removeNode(modelNode);
                }
                if (peerInfo != null) {
                    peerInfo.connecting = false;
                }
                return;
            }

            Socket socket = new Socket();
            try {
                socket.connect(connectTo);
                new TCPConnection(TCPPeer.this, socket, TCP.BULK_DATA, false, modelNode);
                //new TCPConnection(TCPPeer.this, socket, TCP.MANAGEMENT_DATA | TCP.EXPRESS_DATA, true, modelNode);
                socket = new Socket();
                socket.connect(connectTo);
                //new TCPConnection(TCPPeer.this, socket, TCP.BULK_DATA, false, modelNode);
                new TCPConnection(TCPPeer.this, socket, TCP.MANAGEMENT_DATA | TCP.EXPRESS_DATA, true, modelNode);
                peerInfo = findPeerInfoFor(connectTo);
                peerInfo.remotePart.initAndCheckForAdminPort(modelNode);
                stopThread();
                peerInfo.connecting = false;
            } catch (Exception e) {
                peerInfo = findPeerInfoFor(connectTo);
                if (peerInfo != null && peerInfo.remotePart != null) {
                    peerInfo.remotePart.deleteAllChildren();
                }
                connectionElement.getModelHandler().changeNodeName(modelNode, "Looking for " + TCP.formatInetSocketAddress(connectTo) + "...");
                //e.printStackTrace();
                socket.close();
                // simply try again
            }
        }
    }

    /** Connects to another peer */
    class PeerConnectorThread extends LoopThread {

        /** peer to connect to */
        final PeerInfo peer;

        /** model node for status and element we're connecting to */
        final ModelNode modelNode;

        PeerConnectorThread(PeerInfo peer) {
            super(500);
            setName("PeerConnectorThread " + peer.uuid.toString());
            this.peer = peer;
            peer.connecting = true;
            modelNode = new ModelNode("Looking for " + peer.uuid.toString() + "...");
            connectionElement.getModelHandler().addNode(modelRootNode, modelNode);
        }

        @Override
        public void mainLoopCallback() throws Exception {
            if ((!activelyConnect) || peer.connected) {
                stopThread();
                if (modelRootNode != null) {
                    connectionElement.getModelHandler().removeNode(modelNode);
                }
                peer.connecting = false;
                return;
            }

            for (InetAddress address : peer.addresses) {
                InetSocketAddress socketAddress = new InetSocketAddress(address, peer.uuid.port);
                Socket socket = new Socket();
                try {
                    if (peer.remotePart.bulkConnection == null) {
                        socket.connect(socketAddress);
                        //new TCPConnection(TCPPeer.this, socket, TCP.MANAGEMENT_DATA | TCP.EXPRESS_DATA, false, modelNode);
                        new TCPConnection(TCPPeer.this, socket, TCP.BULK_DATA, false, modelNode);
                    }
                    if (peer.remotePart.managementConnection == null) {
                        socket = new Socket();
                        socket.connect(socketAddress);
                        //new TCPConnection(TCPPeer.this, socket, TCP.BULK_DATA, false, modelNode);
                        new TCPConnection(TCPPeer.this, socket, TCP.MANAGEMENT_DATA | TCP.EXPRESS_DATA, false, modelNode);
                    }
                    peer.connecting = false;
                    peer.remotePart.initAndCheckForAdminPort(modelNode);
                    stopThread();
                } catch (Exception e) {
                    peer.remotePart.deleteAllChildren();
                    connectionElement.getModelHandler().changeNodeName(modelNode, "Looking for " + peer.uuid.toString() + "...");
                    socket.close();
                    // simply try again
                }
            }
        }
    }
}
