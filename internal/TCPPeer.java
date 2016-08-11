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

import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.rrlib.finroc_core_utils.jc.thread.LoopThread;
import org.rrlib.logging.Log;
import org.rrlib.logging.LogLevel;
import org.finroc.core.FrameworkElement;
import org.finroc.core.FrameworkElement.ChildIterator;
import org.finroc.core.RuntimeSettings;
import org.finroc.core.datatype.FrameworkElementInfo;
import org.finroc.core.plugin.ExternalConnection;
import org.finroc.core.remote.ModelNode;
import org.finroc.plugins.tcp.internal.TCP.PeerType;

import org.rrlib.serialization.BinaryInputStream;
import org.rrlib.serialization.BinaryOutputStream;


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
public class TCPPeer { /*implements AbstractPeerTracker.Listener*/

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

    /** Set to true if peer list has changed and needs to be sent */
    boolean peerListChanged;

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
            Log.log(LogLevel.ERROR, this, "Error retrieving host name.", e);
        }
        if (peerType == TCP.PeerType.CLIENT_ONLY) {
            // set to negative process id
            if (RuntimeSettings.isRunningInApplet()) {
                thisPeer.uuid.port = -(((int)System.currentTimeMillis()) & 0x7FFFFFFF);
            } else {
                try {
                    try {
                        Class<?> managementFactoryClass = Class.forName("java.lang.management.ManagementFactory");
                        Object bean = managementFactoryClass.getMethod("getRuntimeMXBean").invoke(null);
                        Method getNameMethod = bean.getClass().getMethod("getName");
                        getNameMethod.setAccessible(true);
                        String pid = getNameMethod.invoke(bean).toString();
                        thisPeer.uuid.port = -Integer.parseInt(pid.substring(0, pid.indexOf("@")));
                    } catch (ClassNotFoundException e) {
                        // maybe we're on Android (?)
                        Class<?> processClass = Class.forName("android.os.Process");
                        thisPeer.uuid.port = (Integer)processClass.getMethod("myPid()").invoke(null);
                    }
                } catch (Exception e) {
                    thisPeer.uuid.port = -1;
                    Log.log(LogLevel.ERROR, this, "Error retrieving process id.", e);
                }
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
            setPeerListChanged();
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
                Log.log(LogLevel.ERROR, this, "Remote part has the same UUID as this one: " + uuid.toString());
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

    /**
     * Serialize the peer to stream
     * @param stream the stream to serialize to
     * @param peer the peer to serialize
     */
    private void serializePeerInfo(BinaryOutputStream stream, PeerInfo peer) {
        if ((peer == thisPeer || peer.connected) && peer.peerType != TCP.PeerType.CLIENT_ONLY) {
            stream.writeBoolean(true);

            peer.uuid.serialize(stream);
            stream.writeEnum(peer.peerType);

            stream.writeString(peer.name);
            int addressCount = 0;
            for (InetAddress address : peer.addresses) {
                if (!address.isLoopbackAddress()) {
                    addressCount++;
                }
            }
            stream.writeInt(addressCount);

            for (InetAddress address : peer.addresses) {
                if (!address.isLoopbackAddress()) {
                    TCP.serializeInetAddress(stream, address);
                }
            }
        }
    }

    /**
     * Deserialize the peer from stream
     * @param stream the stream to deserialize from
     * @return the deserialized peer
     */
    public PeerInfo deserializePeerInfo(BinaryInputStream stream) {
        UUID uuid = new UUID();
        uuid.deserialize(stream);

        TCP.PeerType peerType = stream.readEnum(TCP.PeerType.class);
        PeerInfo peer = new PeerInfo(peerType);
        peer.uuid = uuid;

        peer.name = stream.readString();
        int size = stream.readInt();
        peer.addresses.clear();
        for (int i = 0; i < size; ++i) {
            InetAddress address = TCP.deserializeInetAddress(stream);

            peer.addresses.add(address);
        }

        return peer;
    }

    /**
     * Process the incoming peer information
     * @param peer the new peer
     */
    public void processIncomingPeerInfo(PeerInfo peer) {
        PeerInfo existingPeer = null;
        if (peer.uuid.equals(thisPeer.uuid)) {
            existingPeer = thisPeer;
        }

        synchronized (connectTo) {

            for (PeerInfo it : otherPeers) {
                if (peer.uuid.equals(it.uuid)) {
                    existingPeer = it;
                }
            }

            if (existingPeer != null) {
                if (!existingPeer.peerType.equals(peer.peerType)) {
                    Log.log(LogLevel.WARNING, this, "Peer type of existing peer has changed, will not update it.");
                }
                addPeerAddresses(existingPeer, peer.addresses);
            } else {
                otherPeers.add(peer);
            }

        }
    }

    /**
     * Adds all the provided addresses to the specified peer info
     * (if they have not been added already)
     *
     * @param existingPeer Peer to add addresses to
     * @param addresses Addresses to check and possibly add
     */
    private void addPeerAddresses(PeerInfo existingPeer, ArrayList<InetAddress> addresses) {
        for (InetAddress address : addresses) {
            boolean found = false;
            for (InetAddress existingAddress : existingPeer.addresses) {
                if (existingAddress.equals(address)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                existingPeer.addresses.add(address);
                setPeerListChanged();
            }
        }
    }

    /**
     * Scans current peer list and adds missing addresses
     * (e.g. if two peers have the same host, they must both have the same IP addresses)
     */
    public void inferMissingAddresses() {
        for (PeerInfo info : otherPeers) {
            if (info.addresses.size() == 0) {
                for (PeerInfo otherInfo : otherPeers) {
                    if (otherInfo != info && otherInfo.uuid.hostName.equals(info.uuid.hostName)) {
                        addPeerAddresses(info, otherInfo.addresses);
                    }
                }
            }
        }
    }

    /**
     * Mark the peer list as changed
     */
    void setPeerListChanged() {
        peerListChanged = true;
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
                TCPConnection connection1 = new TCPConnection(TCPPeer.this, socket, TCP.BULK_DATA, false, modelNode);
                //new TCPConnection(TCPPeer.this, socket, TCP.MANAGEMENT_DATA | TCP.EXPRESS_DATA, true, modelNode);
                socket = new Socket();
                socket.connect(connectTo);
                //new TCPConnection(TCPPeer.this, socket, TCP.BULK_DATA, false, modelNode);
                TCPConnection connection2 = new TCPConnection(TCPPeer.this, socket, TCP.MANAGEMENT_DATA | TCP.EXPRESS_DATA, true, modelNode);
                peerInfo = findPeerInfoFor(connectTo);
                peerInfo.remotePart.initAndCheckForAdminPort(modelNode);
                stopThread();
                peerInfo.connecting = false;
                connection1.StartThreads();
                connection2.StartThreads();
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

            inferMissingAddresses();

            for (InetAddress address : peer.addresses) {
                InetSocketAddress socketAddress = new InetSocketAddress(address, peer.uuid.port);
                Socket socket = new Socket();
                try {
                    TCPConnection connection1 = null, connection2 = null;
                    if (peer.remotePart == null || peer.remotePart.bulkConnection == null) {
                        socket.connect(socketAddress);
                        //new TCPConnection(TCPPeer.this, socket, TCP.MANAGEMENT_DATA | TCP.EXPRESS_DATA, false, modelNode);
                        connection1 = new TCPConnection(TCPPeer.this, socket, TCP.BULK_DATA, false, modelNode);
                    }
                    if (peer.remotePart == null || peer.remotePart.managementConnection == null) {
                        socket = new Socket();
                        socket.connect(socketAddress);
                        //new TCPConnection(TCPPeer.this, socket, TCP.BULK_DATA, false, modelNode);
                        connection2 = new TCPConnection(TCPPeer.this, socket, TCP.MANAGEMENT_DATA | TCP.EXPRESS_DATA, false, modelNode);
                    }
                    peer.remotePart.initAndCheckForAdminPort(modelNode);
                    stopThread();
                    peer.connecting = false;
                    if (connection1 != null) {
                        connection1.StartThreads();
                    }
                    if (connection2 != null) {
                        connection2.StartThreads();
                    }
                } catch (NullPointerException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    if (peer.remotePart != null) {
                        peer.remotePart.deleteAllChildren();
                    }
                    connectionElement.getModelHandler().changeNodeName(modelNode, "Looking for " + peer.uuid.toString() + "...");
                    socket.close();
                    // simply try again
                }
            }
        }
    }
}
