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

import org.finroc.jc.AtomicInt;
import org.finroc.jc.Time;
import org.finroc.jc.annotation.AtFront;
import org.finroc.jc.annotation.Const;
import org.finroc.jc.annotation.CppInclude;
import org.finroc.jc.annotation.Friend;
import org.finroc.jc.annotation.InCpp;
import org.finroc.jc.annotation.PassByValue;
import org.finroc.jc.annotation.Ptr;
import org.finroc.jc.annotation.Ref;
import org.finroc.jc.annotation.SharedPtr;
import org.finroc.jc.annotation.SizeT;
import org.finroc.jc.net.ConnectException;
import org.finroc.jc.net.IOException;
import org.finroc.jc.net.IPSocketAddress;
import org.finroc.jc.net.NetSocket;

import org.finroc.jc.stream.LargeIntermediateStreamBuffer;
import org.finroc.jc.thread.ThreadUtil;

import org.finroc.core.CoreFlags;
import org.finroc.core.FrameworkElement;
import org.finroc.core.FrameworkElementTreeFilter;
import org.finroc.core.LockOrderLevels;
import org.finroc.core.RuntimeEnvironment;
import org.finroc.core.RuntimeListener;
import org.finroc.core.buffer.CoreInput;
import org.finroc.core.buffer.CoreOutput;
import org.finroc.core.datatype.FrameworkElementInfo;
import org.finroc.core.port.AbstractPort;
import org.finroc.core.port.PortCreationInfo;
import org.finroc.core.port.PortFlags;
import org.finroc.core.port.net.NetPort;
import org.finroc.core.port.net.RemoteCoreRegister;
import org.finroc.core.port.net.RemoteTypes;
import org.finroc.core.portdatabase.DataTypeRegister;
import org.finroc.core.thread.CoreLoopThreadBase;

/**
 * @author max
 *
 * Class that stores information about and can be used to access
 * TCP Server running in another runtime environment.
 *
 * Thread safety considerations:
 *  Many Threads are operating on our objects. Before deleting connection or when disconnecting they
 *  should all be stopped to avoid any race conditions.
 *  Furthermore, ports may be deleted via remote Runtime changes. This can happen while other threads
 *  are using these ports. Therefore, critical port operations should be executed in synchronized context
 *  - as well as any deleting.
 */
@CppInclude("rrlib/finroc_core_utils/GarbageCollector.h")
public class RemoteServer extends FrameworkElement implements RuntimeListener {

    /** Network address */
    private final IPSocketAddress address;

    /** TCP Client Module => in parent */
    //private final TCPClient client;

    /** Bulk and Express Connections to server */
    private @SharedPtr Connection bulk, express;

    /** Cached reference to runtime environment */
    //private static final RuntimeEnvironment runtime = RuntimeEnvironment.getInstance();

    /** This thread reconnects disconnected Remote Nodes and updates subscriptions */
    private final @SharedPtr ConnectorThread connectorThread;

    /** Temporary buffer with port information */
    private @PassByValue FrameworkElementInfo tmpInfo = new FrameworkElementInfo();

    /** Filter that specifies which framework element we're interested in */
    private FrameworkElementTreeFilter filter;

    /** Lookup for remote framework elements (currently not ports) - similar to remote CoreRegister */
    @PassByValue private final RemoteCoreRegister<ProxyPort> remotePortRegister = new RemoteCoreRegister<ProxyPort>();

    /** Lookup for remote framework elements (currently not ports) - similar to remote CoreRegister */
    @PassByValue private final RemoteCoreRegister<ProxyFrameworkElement> remoteElementRegister = new RemoteCoreRegister<ProxyFrameworkElement>();

    /** Iterator for port register (only used by reader thread) */
    private final RemoteCoreRegister<ProxyPort>.Iterator portIterator = remotePortRegister.getIterator();

    /** Iterator for framework element register (only used by bulk reader thread) */
    private final RemoteCoreRegister<ProxyFrameworkElement>.Iterator elemIterator = remoteElementRegister.getIterator();

    /** Temporary buffer for match checks (only used by bulk reader or connector thread) */
    private final StringBuilder tmpMatchBuffer = new StringBuilder();

    /** Timestamp of when server was created - used to identify whether we are still communicating with same instance after connection loss */
    private long serverCreationTime = -1;

    /** Peer that this server belongs to */
    private final TCPPeer peer;

    /** If this is a port-only-client: Framework element that contains all global links */
    private final FrameworkElement globalLinks;

    /** Number of times disconnect was called, since last connect */
    private final AtomicInt disconnectCalls = new AtomicInt(0);

    /** Set to true when server will soon be deleted */
    private boolean deletedSoon = false;

    /**
     * @param isa Network address
     * @param name Unique server name
     * @param parent Parent framework element
     * @param filter Filter that specifies which framework element we're interested in
     * @param peer Peer that this server belongs to
     */
    public RemoteServer(IPSocketAddress isa, String name, FrameworkElement parent, @Const @Ref FrameworkElementTreeFilter filter, TCPPeer peer) {
        super(name, parent, CoreFlags.NETWORK_ELEMENT | CoreFlags.ALLOWS_CHILDREN | (filter.isPortOnlyFilter() ? 0 : CoreFlags.ALTERNATE_LINK_ROOT), LockOrderLevels.REMOTE); // manages ports itself
        this.filter = filter;
        this.peer = peer;
        globalLinks = filter.isPortOnlyFilter() ? new FrameworkElement("global", this, CoreFlags.ALLOWS_CHILDREN | CoreFlags.NETWORK_ELEMENT | CoreFlags.GLOBALLY_UNIQUE_LINK | CoreFlags.ALTERNATE_LINK_ROOT, -1) : null;
        address = isa;
        RuntimeEnvironment.getInstance().addListener(this);
        connectorThread = ThreadUtil.getThreadSharedPtr(new ConnectorThread());
        connectorThread.start();
    }

    /**
     * Connect to remote server
     */
    private synchronized void connect() throws Exception {

        // reset disconnect count
        disconnectCalls.set(0);

        // try connecting...
        @SharedPtr NetSocket socketExpress = NetSocket.createInstance(address);
        @SharedPtr NetSocket socketBulk = NetSocket.createInstance(address);

        // connect
        //Cpp finroc::util::GarbageCollector::Functor deleter;
        @InCpp("std::tr1::shared_ptr<Connection> express(new Connection(this, _T_TCP::TCP_P2P_ID_EXPRESS), deleter);")
        @SharedPtr Connection express = new Connection(TCP.TCP_P2P_ID_EXPRESS);
        @InCpp("std::tr1::shared_ptr<Connection> bulk(new Connection(this, _T_TCP::TCP_P2P_ID_BULK), deleter);")
        @SharedPtr Connection bulk = new Connection(TCP.TCP_P2P_ID_BULK);

        // Set bulk and express here, because of other threads that might try to access them
        this.bulk = bulk;
        this.express = express;
        connectorThread.ctBulk = bulk;
        connectorThread.ctExpress = express;

        // init connections...
        try {
            express.connect(socketExpress, express); // express first, since it's required for retrieving ports, which is done in bulk connection
            bulk.connect(socketBulk, bulk);
        } catch (Exception e) {
            this.bulk = null;
            this.express = null;
            throw e;
        }
    }

    /**
     * Fetches ports and possibly runtime element from remote runtime environment
     *
     * @param cis CoreInput to use
     * @param cos CoreOutput to write request to
     * @param typeLookup Remote Type Database
     * @param Are we communicating with a new server?
     */
    private void retrieveRemotePorts(@Ptr CoreInput cis, @Ptr CoreOutput cos, @Ptr RemoteTypes typeLookup, boolean newServer) {

        // recreate/reset monitoring lists if there has already been a connection
        portIterator.reset();
        for (ProxyPort pp = portIterator.next(); pp != null; pp = portIterator.next()) {
            if (newServer) {
                pp.managedDelete(); // Delete all ports if we are talking to a new server
            } else {
                pp.reset();
            }
        }
        elemIterator.reset();
        for (ProxyFrameworkElement pp = elemIterator.next(); pp != null; pp = elemIterator.next()) {
            if (newServer) {
                pp.managedDelete();
            } else {
                pp.refound = false; // reset refound flag
            }
        }

        // send opcode & filter information
        //cos.writeByte(TCP.REQUEST_PORT_UPDATE);
        filter.serialize(cos);
        //if (TCPSettings.DEBUG_TCP) {
        //  cos.writeInt(TCPSettings.DEBUG_TCP_NUMBER);
        //}
        cos.flush();

        // retrieve initial port information
        while (cis.readByte() != 0) {
            tmpInfo.deserialize(cis, typeLookup);
            //System.out.println("Received info: " + tmpInfo.toString());
            processPortUpdate(tmpInfo);
        }
        init();

        portIterator.reset();
        for (ProxyPort pp = portIterator.next(); pp != null; pp = portIterator.next()) {
            if (!pp.refound) {
                pp.managedDelete(); // portUpdated & register remove is performed here
            }
        }
        elemIterator.reset();
        for (ProxyFrameworkElement pp = elemIterator.next(); pp != null; pp = elemIterator.next()) {
            if (!pp.refound) {
                pp.managedDelete(); // framework element register remove is performed here
            }
        }

    }

    /**
     * Process incoming framework element change
     *
     * @param Framework element change information
     */
    private void processPortUpdate(@Ref FrameworkElementInfo info) {

        // these variables will store element to update
        ProxyFrameworkElement fe = null;
        ProxyPort port = null;

        // find current element
        if (info.isPort()) {
            port = remotePortRegister.get(info.getHandle());
        } else {
            fe = remoteElementRegister.get(-info.getHandle());
        }

        if (info.opCode == RuntimeListener.ADD) {

            // create new framework element
            if (info.isPort()) { // delete old port?
                if (info.getDataType() == null) { // Unknown type... skip
                    return;
                }

                if (port != null && port.remoteHandle != info.getHandle()) {
                    port.managedDelete();
                    port = null;
                }
                if (port == null) { // normal case
                    port = new ProxyPort(info);
                } else { // refound port
                    //Cpp printf("refound network port %p %s\n", port, port->getPort()->getCDescription());
                    synchronized (port) {
                        port.refound = true;
                        port.connection = (info.getFlags() & PortFlags.IS_EXPRESS_PORT) > 0 ? express : bulk;
                        assert(port.matches(info)) : "Structure in server changed - that shouldn't happen";
                        info.opCode = RuntimeListener.CHANGE;
                        port.updateFromPortInfo(info);
                    }
                }
            } else {
                if (fe != null && fe.remoteHandle != info.getHandle()) { // delete old frameworkElement
                    fe.managedDelete();
                    fe = null;
                }
                if (fe == null || fe.yetUnknown) { // normal
                    fe = (ProxyFrameworkElement)getFrameworkElement(info.getHandle(), info.getFlags(), false, info.getLink(0).parent);
                    fe.updateFromPortInfo(info);
                    //fe.yetUnknown = false;
                } else if (fe != null) { // refound
                    synchronized (fe) {
                        //Cpp printf("refound network framework element %p %s\n", fe, fe->getCDescription());
                        fe.refound = true;
                        assert(fe.matches(info)) : "Structure in server changed - that shouldn't happen";
                        info.opCode = RuntimeListener.CHANGE;
                        fe.updateFromPortInfo(info);
                    }
                }
            }

        } else if (info.opCode == RuntimeListener.CHANGE) {

            // we're dealing with an existing framework element
            assert(fe != null || port != null);
            if (info.isPort()) {
                port.updateFromPortInfo(info);
            } else {
                fe.updateFromPortInfo(info);
            }

        } else if (info.opCode == RuntimeListener.REMOVE) {

            // we're dealing with an existing framework element
            assert(fe != null || port != null);
            if (info.isPort()) {
                port.managedDelete();
            } else {
                fe.managedDelete();
            }

        }
    }

    @Override
    protected synchronized void prepareDelete() {
        RuntimeEnvironment.getInstance().removeListener(this);
        System.out.println("RemoteServer: Stopping ConnectorThread");
        connectorThread.stopThread();
        try {
            connectorThread.join();
        } catch (InterruptedException e) {

            //JavaOnlyBlock
            e.printStackTrace();

            //Cpp _printf("warning: RemoteServer::prepareDelete() - Interrupted waiting for connector thread.\n");
        }
        System.out.println("RemoteServer: Disconnecting");
        disconnect();

        // delete all elements created by this remote server (should be done automatically, actually)
        /*portIterator.reset();
        for (ProxyPort pp = portIterator.next(); pp != null; pp = portIterator.next()) {
            pp.managedDelete();
        }
        elemIterator.reset();
        for (ProxyFrameworkElement pp = elemIterator.next(); pp != null; pp = elemIterator.next()) {
            pp.managedDelete();
        }*/

        super.prepareDelete();
    }

    /**
     * Disconnect from remote server
     */
    private void disconnect() {

        // make sure that disconnect is only called once... prevents deadlocks cleaning up all the threads
        int calls = disconnectCalls.incrementAndGet();
        if (calls > 1) {
            return;
        }

        synchronized (this) {
            if (bulk != null) {
                bulk.disconnect();
                bulk = null; // needed afterwards so commmented out
            }
            if (express != null) {
                express.disconnect();
                express = null; // needed afterwards so commmented out
            }

            // reset subscriptions
            portIterator.reset();
            for (ProxyPort pp = portIterator.next(); pp != null; pp = portIterator.next()) {
                pp.reset();
            }
            portIterator.reset();
            //      for (ProxyPort pp = portIterator.next(); pp != null; pp = portIterator.next()) {
            //          pp.subscriptionQueueLength = 0;
            //      }
        }
    }

    /**
     * Returns framework element with specified handle.
     * Creates one if it doesn't exist.
     *
     * @param handle Remote Handle of parent
     * @param extraFlags Any extra flags of parent to keep
     * @param portParent Parent of a port?
     * @param parentHandle Handle of parent (only necessary, when not unknown parent of a port)
     * @return Framework element.
     */
    public FrameworkElement getFrameworkElement(int handle, int extraFlags, boolean portParent, int parentHandle) {
        if (handle == RuntimeEnvironment.getInstance().getHandle()) { // if parent is runtime environment - should be added as child of remote server
            return this;
        }
        ProxyFrameworkElement pxe = remoteElementRegister.get(-handle);
        if (pxe != null) {
            assert(pxe.remoteHandle == handle);
        } else {
            if (portParent) {
                pxe = new ProxyFrameworkElement(handle, extraFlags, LockOrderLevels.REMOTE_PORT - 10);
            } else {
                FrameworkElement parent = (parentHandle == RuntimeEnvironment.getInstance().getHandle()) ? (FrameworkElement)this : (FrameworkElement)remoteElementRegister.get(-parentHandle);
                assert(parent != null) : "Framework elements published in the wrong order - server's fault (programming error)";
                pxe = new ProxyFrameworkElement(handle, extraFlags, parent.getLockOrder() + 1);
            }
        }
        return pxe;
    }

    /**
     * @author max
     *
     * Dummy framework element for clients which are interested in remote structure
     */
    @Friend(RemoteServer.class)
    public class ProxyFrameworkElement extends FrameworkElement {

        /** Has port been found again after reconnect? */
        private boolean refound = true;

        /** Handle in remote runtime environment */
        private int remoteHandle = 0;

        /** Is this a place-holder framework element for info that we will receive later? */
        private boolean yetUnknown;

        /** Constructor for yet anonymous element */
        public ProxyFrameworkElement(int handle, int extraFlags, int lockOrder) {
            super("(yet unknown)", null, CoreFlags.ALLOWS_CHILDREN | CoreFlags.NETWORK_ELEMENT | (extraFlags & FrameworkElementInfo.PARENT_FLAGS_TO_STORE), lockOrder);
            this.remoteHandle = handle;
            remoteElementRegister.put(-remoteHandle, this);
            yetUnknown = true;
        }

        public synchronized boolean matches(@Const @Ref FrameworkElementInfo info) {
            if (remoteHandle != info.getHandle() || info.getLinkCount() != getLinkCount()) {
                return false;
            }
            if ((getAllFlags() & CoreFlags.CONSTANT_FLAGS) != (info.getFlags() & CoreFlags.CONSTANT_FLAGS)) {
                return false;
            }
            if (getDescription().equals(info.getLink(0).name)) {
                return false;
            }
            return true;
        }

//      public ProxyFrameworkElement(@Const @Ref FrameworkElementInfo info) {
//          super("(yet unknown)", null, CoreFlags.ALLOWS_CHILDREN | CoreFlags.NETWORK_ELEMENT | (info.getFlags() & FrameworkElementInfo.PARENT_FLAGS_TO_STORE));
//          this.remoteHandle = info.getHandle();
//          remoteElementRegister.put(-remoteHandle, this);
//          updateFromPortInfo(info);
//          yetUnknown = false;
//      }

        /**
         * Update information about framework element
         *
         * @param info Information
         */
        public synchronized void updateFromPortInfo(@Const @Ref FrameworkElementInfo info) {
            if (!isReady()) {
                assert(info.opCode == RuntimeListener.ADD) : "only add operation may change framework element before initialization";
                assert(info.getLinkCount() == 1) : "Framework elements currently may not be linked";
//              for (int i = 1; i < info.getLinks().size(); i++) {
//                  ProxyFrameworkElement pxe = getFrameworkElement(info.getParents().get(i));
//                  pxe.link(this, info.getLinks().get(i));
//              }
                setDescription(info.getLink(0).name);
                getFrameworkElement(info.getLink(0).parent, info.getLink(0).extraFlags, false, info.getLink(0).parent).addChild(this);
            }
            yetUnknown = false;
        }

        @Override
        protected void prepareDelete() {
            remoteElementRegister.remove(-remoteHandle);
            super.prepareDelete();
        }
    }

    /**
     * Local port that acts as proxy for ports on remote machines
     */
    @Friend(RemoteServer.class) @Ptr
    public class ProxyPort extends TCPPort {

        /** Has port been found again after reconnect? */
        private boolean refound = true;

        /** >= 0 when port has subscribed to server; value of current subscription */
        private short subscriptionStrategy = -1;

        /** true, if current subscription includes reverse push strategy */
        private boolean subscriptionRevPush = false;

        /** Update time of current subscription */
        private short subscriptionUpdateTime = -1;

        /**
         * Is port the one that is described by this information?
         *
         * @param info Port information
         * @return Answer
         */
        public boolean matches(@Const @Ref FrameworkElementInfo info) {
            synchronized (getPort()) {
                if (remoteHandle != info.getHandle() || info.getLinkCount() != getPort().getLinkCount()) {
                    return false;
                }
                if ((getPort().getAllFlags() & CoreFlags.CONSTANT_FLAGS) != (info.getFlags() & CoreFlags.CONSTANT_FLAGS)) {
                    return false;
                }
                for (@SizeT int i = 0; i < info.getLinkCount(); i++) {
                    if (filter.isPortOnlyFilter()) {
                        getPort().getQualifiedLink(tmpMatchBuffer, i);
                    } else {
                        tmpMatchBuffer.delete(0, tmpMatchBuffer.length());
                        tmpMatchBuffer.append(getPort().getLink(i).getDescription());
                    }
                    if (!tmpMatchBuffer.equals(info.getLink(i).name)) {
                        return false;
                    }
                    // parents are negligible if everything else, matches
                }
                return true;
            }
        }

        public void reset() {
            connection = null; // set connection to null
            monitored = false; // reset monitored flag
            refound = false; // reset refound flag
            propagateStrategyFromTheNet((short)0);
            subscriptionRevPush = false;
            subscriptionUpdateTime = -1;
            subscriptionStrategy = -1;
        }

        /**
         * @param portInfo Port information
         */
        public ProxyPort(@Const @Ref FrameworkElementInfo portInfo) {
            super(createPCI(portInfo), (portInfo.getFlags() & PortFlags.IS_EXPRESS_PORT) > 0 ? express : bulk);
            remoteHandle = portInfo.getHandle();
            remotePortRegister.put(remoteHandle, this);
            updateFromPortInfo(portInfo);
        }

        /**
         * Update port properties/information from received port information
         *
         * @param portInfo Port info
         */
        private void updateFromPortInfo(@Const @Ref FrameworkElementInfo portInfo) {
            synchronized (getPort().getRegistryLock()) {
                updateFlags(portInfo.getFlags());
                getPort().setMinNetUpdateInterval(portInfo.getMinNetUpdateInterval());
                updateIntervalPartner = portInfo.getMinNetUpdateInterval(); // TODO redundant?
                propagateStrategyFromTheNet(portInfo.getStrategy());
                portInfo.getConnections(connections);
                if (TCPSettings.DISPLAY_INCOMING_PORT_UPDATES.get()) {
                    System.out.println("Updating port info: " + portInfo.toString());
                }
                if (portInfo.opCode == RuntimeListener.ADD) {
                    assert(!getPort().isReady());
                    if (filter.isPortOnlyFilter()) {
                        for (int i = 1, n = portInfo.getLinkCount(); i < n; i++) {
                            FrameworkElement parent = (portInfo.getLink(i).extraFlags & CoreFlags.GLOBALLY_UNIQUE_LINK) > 0 ? globalLinks : (FrameworkElement)RemoteServer.this;
                            getPort().link(parent, portInfo.getLink(i).name);
                        }
                        FrameworkElement parent = (portInfo.getLink(0).extraFlags & CoreFlags.GLOBALLY_UNIQUE_LINK) > 0 ? globalLinks : (FrameworkElement)RemoteServer.this;
                        getPort().setDescription(portInfo.getLink(0).name);
                        parent.addChild(getPort());
                    } else {
                        for (@SizeT int i = 1; i < portInfo.getLinkCount(); i++) {
                            FrameworkElement parent = getFrameworkElement(portInfo.getLink(i).parent, portInfo.getLink(i).extraFlags, true, 0);
                            getPort().link(parent, portInfo.getLink(i).name);
                        }
                        getPort().setDescription(portInfo.getLink(0).name);
                        getFrameworkElement(portInfo.getLink(0).parent, portInfo.getLink(0).extraFlags, true, 0).addChild(getPort());
                    }
                }
                checkSubscription();
            }
        }

        @Override
        protected void prepareDelete() {
            remotePortRegister.remove(remoteHandle);
            getPort().disconnectAll();
            checkSubscription();
            super.prepareDelete();
        }

        @Override
        protected void connectionRemoved() {
            checkSubscription();
        }

        @Override
        protected void newConnection() {
            checkSubscription();
        }

        @Override
        protected void propagateStrategyOverTheNet() {
//          if (getPort().getStrategy() == -1) {
//              ((Connection)connection).unsubscribe(remoteHandle);
//              connected = false;
//          } else {
//              ((Connection)connection).subscribe(remoteHandle, getPort().getStrategy(), getPort().isConnectedToReversePushSources(), getUpdateIntervalForNet(), getPort().getHandle());
//              connected = true;
//          }
            checkSubscription();
        }

        /* (non-Javadoc)
         * @see core.plugin.tcp2.TCPPort#checkSubscription()
         */
        @Override
        protected void checkSubscription() {
            synchronized (getPort().getRegistryLock()) {
                AbstractPort p = getPort();
                boolean revPush = p.isInputPort() && p.isConnectedToReversePushSources();
                short time = getUpdateIntervalForNet();
                short strategy = p.isInputPort() ? 0 : p.getStrategy();
                if (!p.isConnected()) {
                    strategy = -1;
                }

                @Ptr Connection c = (Connection)connection;

                if (c == null) {
                    subscriptionStrategy = -1;
                    subscriptionRevPush = false;
                    subscriptionUpdateTime = -1;
                } else if (strategy == -1 && subscriptionStrategy > -1) { // disconnect
                    c.unsubscribe(remoteHandle);
                    subscriptionStrategy = -1;
                    subscriptionRevPush = false;
                    subscriptionUpdateTime = -1;
                } else if (strategy == -1) {
                    // still disconnected
                } else if (strategy != subscriptionStrategy || time != subscriptionUpdateTime || revPush != subscriptionRevPush) {
                    c.subscribe(remoteHandle, strategy, revPush, time, p.getHandle());
                    subscriptionStrategy = strategy;
                    subscriptionRevPush = revPush;
                    subscriptionUpdateTime = time;
                }
            }
        }
    }

    /**
     * (Belongs to ProxyPort)
     *
     * Create Port Creation info from PortInfo class.
     * Except from Shared flag port will be identical to original port.
     *
     * @param portInfo Port Information
     * @return Port Creation info
     */
    private static PortCreationInfo createPCI(@Const @Ref FrameworkElementInfo portInfo) {
        PortCreationInfo pci = new PortCreationInfo(portInfo.getFlags());
        pci.flags = portInfo.getFlags();

        // unset shared flag
        //pci.setFlag(PortFlags.NETWORK_PORT, true);

        // unset copy data flags (data is always copied)
        //pci.flags = PortFlags.setFlag(pci.flags, PortFlags.COPY_DATA, false);
        //pci.flags = PortFlags.setFlag(pci.flags, PortFlags.COPY_REVERSE_DATA, false);

        // always create send buffers (for deserialization)
        //pci.flags = PortFlags.setFlag(pci.flags, PortFlags.OWNS_SEND_BUFFERS, true);

        // set queue size
        pci.maxQueueSize = portInfo.getStrategy();

        pci.dataType = portInfo.getDataType();
        pci.lockOrder = LockOrderLevels.REMOTE_PORT;

        return pci;
    }

    /**
     * Client TCP Connection.
     *
     * This class is used on client side and
     * represents a single I/O TCP Connection with
     * own socket.
     */
    @Friend(RemoteServer.class)
    public class Connection extends TCPConnection {

        /** Command buffer for subscriptions etc. */
        //private final CoreByteBuffer commandBuffer = new CoreByteBuffer(2000, ByteOrder.BIG_ENDIAN);

        /**
         * Client side constructor
         *
         * @param type Connection type
         */
        public Connection(byte type) {
            super(type, type == TCP.TCP_P2P_ID_BULK ? RemoteServer.this.peer : null, type == TCP.TCP_P2P_ID_BULK);
        }

        public void connect(NetSocket socket_, @SharedPtr @Ref Connection connection) throws ConnectException, IOException {
            super.socket = socket_;

            // write stream id
            @SharedPtr LargeIntermediateStreamBuffer lmBuf = new LargeIntermediateStreamBuffer(socket_.getSink());
            cos = new CoreOutput(lmBuf);
            cos.writeByte(type);
            RemoteTypes.serializeLocalDataTypes(DataTypeRegister.getInstance(), cos);
            boolean bulk = type == TCP.TCP_P2P_ID_BULK;
            String typeString = getConnectionTypeString();
            cos.writeBoolean(bulk);
            cos.flush();

            // initialize core streams
            cis = new CoreInput(socket_.getSource());
            cis.setTypeTranslation(updateTimes);
            timeBase = cis.readLong(); // Timestamp that remote runtime was created - and relative to which time is encoded in this stream
            updateTimes.deserialize(cis);

            @SharedPtr Reader listener = ThreadUtil.getThreadSharedPtr(new Reader("TCP Client " + typeString + "-Listener for " + getDescription()));
            super.reader = listener;
            @SharedPtr Writer writer = ThreadUtil.getThreadSharedPtr(new Writer("TCP Client " + typeString + "-Writer for " + getDescription()));
            super.writer = writer;

            if (bulk) {
                boolean newServer = (serverCreationTime < 0) || (serverCreationTime != timeBase);
                if (!newServer) {
                    System.out.print("Re-");
                } /*else {
                    serverCreationTime = timeBase;
                }*/
                System.out.println("Connecting to server " + socket_.getRemoteSocketAddress().toString() + "...");
                retrieveRemotePorts(cis, cos, updateTimes, newServer);
            }

            // start incoming data listener thread
            listener.lockObject(connection);
            listener.start();

            // start writer thread
            writer.lockObject(connection);
            writer.start();
        }

        @Override
        public void handleDisconnect() {
            RemoteServer.this.disconnect();
        }

        @Override
        public void processRequest(byte opCode) throws Exception {

            int portIndex = 0;
            NetPort p = null;
            //MethodCall mc = null;
            AbstractPort ap = null;

            switch (opCode) {
            case TCP.CHANGE_EVENT:

                // read port index and retrieve proxy port
                portIndex = cis.readInt();
                cis.readSkipOffset();
                ap = RuntimeEnvironment.getInstance().getPort(portIndex);
                if (ap != null) {
                    p = ap.asNetPort();
                    assert(p != null);
                }

                // read time stamp... will be optimized in the future to save space
                //long timestamp = readTimestamp();

                // write to proxy port
                if (ap != null) {

                    // make sure, "our" port is not deleted while we use it
                    synchronized (ap) {
                        if (!ap.isReady()) {
                            cis.toSkipTarget();
                        } else {
                            byte changedFlag = cis.readByte();
                            cis.setBufferSource(p.getPort());
                            p.receiveDataFromStream(cis, Time.getCoarse(), changedFlag);
                            cis.setBufferSource(null);
                        }
                    }
                } else {
                    cis.toSkipTarget();
                }
                break;

            case TCP.PORT_UPDATE:

                tmpInfo.deserialize(cis, updateTimes);
                processPortUpdate(tmpInfo);
                init();
                break;

            default:
                throw new Exception("Client Listener does not accept this opcode: " + opCode);
            }
        }

        /**
         * Subscribe to port changes on remote server
         *
         * @param index Port index in remote runtime
         * @param strategy Strategy to use/request
         * @param updateInterval Minimum interval in ms between notifications (values <= 0 mean: use server defaults)
         * @param localIndex Local Port Index
         * @param dataType DataType got from server
         */
        public void subscribe(int index, short strategy, boolean reversePush, short updateInterval, int localIndex) {
            TCPCommand command = TCP.getUnusedTCPCommand();
            command.opCode = TCP.SUBSCRIBE;
            command.remoteHandle = index;
            command.strategy = strategy;
            command.reversePush = reversePush;
            command.updateInterval = updateInterval;
            command.localIndex = localIndex;
            sendCall(command);
            //command.genericRecycle();
        }

        /**
         * Unsubscribe from port changes on remote server
         *
         * @param index Port index in remote runtime
         */
        public void unsubscribe(int index) {
            TCPCommand command = TCP.getUnusedTCPCommand();
            command.opCode = TCP.UNSUBSCRIBE;
            command.remoteHandle = index;
            sendCall(command);
            //command.genericRecycle();
        }

        @Override
        public boolean sendData(long startTime) throws Exception {

            // send port data
            return super.sendDataPrototype(startTime, TCP.SET);

        }

        @Override
        public void handlePingTimeExceed() {
            System.out.println("TCPClient warning: critical ping time exceeded");
        }

        @Override
        protected TCPPort lookupPortForCallHandling(int portIndex) {
            AbstractPort ap = RuntimeEnvironment.getInstance().getPort(portIndex);
            TCPPort p = null;
            if (ap != null) {
                p = (TCPPort)ap.asNetPort();
                assert(p != null);
            }
            return p;
        }
    }


    /**
     * This thread reconnects if connection was interrupted and updates subscriptions.
     */
    @AtFront @Friend(RemoteServer.class)
    private class ConnectorThread extends CoreLoopThreadBase {

        /** Timestamp of last subscription update */
        private long lastSubscriptionUpdate = 0;

        /** Bulk and Express Connections to server - copy for connector thread */
        private @SharedPtr Connection ctBulk, ctExpress;

//      /** Static ChildIterator */
//      private RemoteCoreRegister<ProxyPort>.Iterator ci = remotePortRegister.getIterator();

        public ConnectorThread() {
            super(TCPSettings.CONNECTOR_THREAD_LOOP_INTERVAL, false, false);
            setName("TCP Connector Thread for " + getDescription());
            System.out.println("Creating " + getName());
            //this.setPriority(1); // low priority
        }

        @Override
        public void mainLoopCallback() throws Exception {

            if (ctBulk != null && ctBulk.disconnecting()) {
                ctBulk = null;
            }
            if (ctExpress != null && ctExpress.disconnecting()) {
                ctExpress = null;
            }

            if (ctBulk == null && ctExpress == null && bulk == null && express == null) {

                // Try connecting
                try {
                    connect();
                } catch (ConnectException e) {
                    Thread.sleep(2000);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } else if (ctBulk != null && ctExpress != null) {

                try {

                    // check ping times
                    long startTime = System.currentTimeMillis();
                    long mayWait = TCPSettings.criticalPingThreshold.get();
                    mayWait = Math.min(mayWait, ctExpress.checkPingForDisconnect());
                    mayWait = Math.min(mayWait, ctBulk.checkPingForDisconnect());

                    if (startTime > lastSubscriptionUpdate + TCPSettings.CONNECTOR_THREAD_SUBSCRIPTION_UPDATE_INTERVAL) {

                        lastSubscriptionUpdate = startTime;

                    }

                    // wait remaining uncritical time
                    long waitFor = mayWait - (System.currentTimeMillis() - startTime);
                    if (waitFor > 0) {
                        Thread.sleep(waitFor);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void runtimeChange(byte changeType, FrameworkElement element) {
        if (element.isPort() && changeType != RuntimeListener.PRE_INIT) {
            NetPort np = ((AbstractPort)element).findNetPort(express);
            if (np == null) {
                np = ((AbstractPort)element).findNetPort(bulk);
            }
            if (np != null) {
                ((ProxyPort)np).checkSubscription();
            }
        }
    }

    /**
     * Disconnects and pauses connector thread
     */
    public synchronized void temporaryDisconnect() {

        connectorThread.pauseThread();
        disconnect();
    }

    /**
     * Reconnect after temporary disconnect
     */
    public synchronized void reconnect() {
        connectorThread.continueThread();
    }

    /**
     * @return Address of connection partner
     */
    public IPSocketAddress getPartnerAddress() {
        return address;
    }

    /**
     * @return Connection quality (see ExternalConnection)
     */
    public synchronized float getConnectionQuality() {
        if (bulk == null && express == null) {
            return 0;
        }
        float pingTime = 0;
        for (int i = 0; i < 2; i++) {
            Connection c = (i == 0) ? bulk : express;
            if (c != null) {
                if (c.pingTimeExceeed()) {
                    return 0;
                }
                pingTime = Math.max(pingTime, (float)c.getAvgPingTime());
            }
        }
        if (pingTime < 300) {
            return 1;
        } else if (pingTime > 1300) {
            return 0;
        } else {
            return ((float)pingTime - 300.0f) / 1000.0f;
        }
    }

    /**
     * @return String containing ping times
     */
    public synchronized String getPingString() {
        int pingAvg = 0;
        int pingMax = 0;
        int dataRate = 0;
        String s = "ping (avg/max/Rx): ";
        if (bulk == null && express == null) {
            return s + "- ";
        }
        for (int i = 0; i < 2; i++) {
            Connection c = (i == 0) ? bulk : express;
            if (c != null) {
                if (c.pingTimeExceeed()) {
                    return s + "- ";
                }
                pingAvg = Math.max(pingAvg, c.getAvgPingTime());
                pingMax = Math.max(pingMax, c.getMaxPingTime());
                dataRate += c.getRx();
            }
        }
        return s + pingAvg + "ms/" + pingMax + "ms/" + formatRate(dataRate);
    }

    /**
     * @param dataRate Data Rate
     * @return Formatted Data Rate
     */
    private static String formatRate(int dataRate) {
        if (dataRate < 1000) {
            return "" + dataRate;
        } else if (dataRate < 10000000) {
            return (dataRate / 1000) + "k";
        } else {
            return (dataRate / 1000000) + "M";
        }
    }

    /**
     * Early preparations for deleting this
     */
    public void earlyDeletingPreparations() {
        connectorThread.stopThread();
        deletedSoon = true;
    }

    /**
     * @return true when server will soon be deleted
     */
    public boolean deletedSoon() {
        return deletedSoon;
    }
}
