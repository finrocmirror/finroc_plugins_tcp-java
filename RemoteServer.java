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

import java.util.ArrayList;
import java.util.List;

import org.rrlib.finroc_core_utils.jc.AtomicInt;
import org.rrlib.finroc_core_utils.jc.Time;
import org.rrlib.finroc_core_utils.jc.annotation.AtFront;
import org.rrlib.finroc_core_utils.jc.annotation.Const;
import org.rrlib.finroc_core_utils.jc.annotation.CppInclude;
import org.rrlib.finroc_core_utils.jc.annotation.CppType;
import org.rrlib.finroc_core_utils.jc.annotation.Friend;
import org.rrlib.finroc_core_utils.jc.annotation.InCpp;
import org.rrlib.finroc_core_utils.jc.annotation.JavaOnly;
import org.rrlib.finroc_core_utils.jc.annotation.PassByValue;
import org.rrlib.finroc_core_utils.jc.annotation.Ptr;
import org.rrlib.finroc_core_utils.jc.annotation.Ref;
import org.rrlib.finroc_core_utils.jc.annotation.SharedPtr;
import org.rrlib.finroc_core_utils.jc.annotation.SizeT;
import org.rrlib.finroc_core_utils.jc.container.SimpleList;
import org.rrlib.finroc_core_utils.jc.log.LogDefinitions;
import org.rrlib.finroc_core_utils.jc.net.ConnectException;
import org.rrlib.finroc_core_utils.jc.net.IOException;
import org.rrlib.finroc_core_utils.jc.net.IPSocketAddress;
import org.rrlib.finroc_core_utils.jc.net.NetSocket;

import org.rrlib.finroc_core_utils.jc.stream.LargeIntermediateStreamBuffer;
import org.rrlib.finroc_core_utils.jc.thread.ThreadUtil;
import org.rrlib.finroc_core_utils.log.LogDomain;
import org.rrlib.finroc_core_utils.log.LogLevel;
import org.rrlib.finroc_core_utils.rtti.DataTypeBase;
import org.rrlib.finroc_core_utils.serialization.InputStreamBuffer;
import org.rrlib.finroc_core_utils.serialization.OutputStreamBuffer;
import org.rrlib.finroc_core_utils.serialization.Serialization;

import org.finroc.core.CoreFlags;
import org.finroc.core.FrameworkElement;
import org.finroc.core.FrameworkElementTreeFilter;
import org.finroc.core.LockOrderLevels;
import org.finroc.core.RuntimeEnvironment;
import org.finroc.core.RuntimeListener;
import org.finroc.core.admin.AdminClient;
import org.finroc.core.admin.AdminServer;
import org.finroc.core.datatype.CoreNumber;
import org.finroc.core.datatype.FrameworkElementInfo;
import org.finroc.core.port.AbstractPort;
import org.finroc.core.port.PortCreationInfo;
import org.finroc.core.port.PortFlags;
import org.finroc.core.port.net.NetPort;
import org.finroc.core.port.net.RemoteCoreRegister;
import org.finroc.core.port.net.RemoteHandleLookup;
import org.finroc.core.port.net.RemoteRuntime;
import org.finroc.core.port.net.RemoteTypes;
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
public class RemoteServer extends FrameworkElement implements RuntimeListener, RemoteHandleLookup {

    /** Network address */
    private final IPSocketAddress address;

    /** Bulk and Express Connections to server */
    private @SharedPtr Connection bulk, express;

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

    /** Administration interface client port */
    @JavaOnly
    private final AdminClient adminInterface;

    /** String to display when connecting */
    @CppType("const char*")
    private static final String CONNECTING = "connecting";

    /** String to display when disconnecting */
    @CppType("const char*")
    private static final String DISCONNECTING = "disconnecting";

    /** Not null when currently connecting or disconnecting */
    @InCpp("const char* volatile statusString;")
    private volatile String statusString = null;

    /** Log domain for this class */
    @InCpp("_RRLIB_LOG_CREATE_NAMED_DOMAIN(logDomain, \"tcp\");")
    public static final LogDomain logDomain = LogDefinitions.finroc.getSubDomain("tcp");

    /**
     * @param isa Network address
     * @param name Unique server name
     * @param parent Parent framework element
     * @param filter Filter that specifies which framework element we're interested in
     * @param peer Peer that this server belongs to
     */
    public RemoteServer(IPSocketAddress isa, String name, FrameworkElement parent, @Const @Ref FrameworkElementTreeFilter filter, TCPPeer peer) {
        super(parent, name, CoreFlags.NETWORK_ELEMENT | CoreFlags.ALLOWS_CHILDREN | (filter.isPortOnlyFilter() ? 0 : CoreFlags.ALTERNATE_LINK_ROOT), LockOrderLevels.REMOTE); // manages ports itself
        this.filter = filter;
        this.peer = peer;
        globalLinks = filter.isPortOnlyFilter() ? new FrameworkElement(this, "global", CoreFlags.ALLOWS_CHILDREN | CoreFlags.NETWORK_ELEMENT | CoreFlags.GLOBALLY_UNIQUE_LINK | CoreFlags.ALTERNATE_LINK_ROOT, -1) : null;
        address = isa;

        //JavaOnlyBlock
        adminInterface = filter.isAcceptAllFilter() ? new AdminClient("AdminClient " + getName(), peer) : null;
        addAnnotation(new RemoteRuntime(adminInterface, null, this));

        RuntimeEnvironment.getInstance().addListener(this);
        connectorThread = ThreadUtil.getThreadSharedPtr(new ConnectorThread());
        connectorThread.start();
    }

    /**
     * Connect to remote server
     */
    private void connect() throws Exception {
        synchronized (this) {
            statusString = CONNECTING;

            // reset disconnect count
            disconnectCalls.set(0);
        }

        // try connecting...
        @SharedPtr NetSocket socketExpress = NetSocket.createInstance(address);
        @SharedPtr NetSocket socketBulk = NetSocket.createInstance(address);

        synchronized (this) {
            if (disconnectCalls.get() > 0) {
                socketExpress.close();
                socketBulk.close();
                return;
            }

            // connect
            //Cpp finroc::util::GarbageCollector::Functor deleter;
            @InCpp("std::shared_ptr<Connection> express(new Connection(this, _T_TCP::TCP_P2P_ID_EXPRESS), deleter);")
            @SharedPtr Connection express = new Connection(TCP.TCP_P2P_ID_EXPRESS);
            @InCpp("std::shared_ptr<Connection> bulk(new Connection(this, _T_TCP::TCP_P2P_ID_BULK), deleter);")
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
                statusString = null;
            } catch (Exception e) {
                this.bulk = null;
                this.express = null;
                socketExpress.close();
                socketBulk.close();
                throw e;
            }
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
    private void retrieveRemotePorts(@Ptr InputStreamBuffer cis, @Ptr OutputStreamBuffer cos, @Ptr RemoteTypes typeLookup, boolean newServer) {

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

        logDomain.log(LogLevel.LL_DEBUG_VERBOSE_2, getLogDescription(), "Received updated FrameworkElementInfo: " + info.toString());

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

                if (port != null && port.getRemoteHandle() != info.getHandle()) {
                    port.managedDelete();
                    port = null;
                }
                if (port == null) { // normal case
                    port = new ProxyPort(info);
                } else { // refound port
                    //Cpp printf("refound network port %p %s\n", port, port->getPort()->getCName());
                    synchronized (port.getPort()) {
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
                        //Cpp printf("refound network framework element %p %s\n", fe, fe->getCName());
                        fe.refound = true;
                        assert(fe.matches(info)) : "Structure in server changed - that shouldn't happen";
                        info.opCode = RuntimeListener.CHANGE;
                        fe.updateFromPortInfo(info);
                    }
                }
            }

        } else if (info.opCode == RuntimeListener.CHANGE || info.opCode == FrameworkElementInfo.EDGE_CHANGE) {

            // we're dealing with an existing framework element
            if (info.isPort()) {
                if (port == null && info.getDataType() == null) { // ignore ports that we did not create, because of unknown type
                    return;
                }
                assert(port != null);
                port.updateFromPortInfo(info);
            } else {
                assert(fe != null);
                fe.updateFromPortInfo(info);
            }

        } else if (info.opCode == RuntimeListener.REMOVE) {

            // we're dealing with an existing framework element
            if (info.isPort()) {
                if (port == null && info.getDataType() == null) { // ignore ports that we did not create, because of unknown type
                    return;
                }
                assert(port != null);
                port.managedDelete();
            } else {
                assert(fe != null);
                fe.managedDelete();
            }

        }
    }

    @Override
    protected synchronized void prepareDelete() {
        RuntimeEnvironment.getInstance().removeListener(this);
        log(LogLevel.LL_DEBUG_VERBOSE_1, logDomain, "RemoteServer: Stopping ConnectorThread");
        connectorThread.stopThread();
        try {
            connectorThread.join(3000); // If we need to wait longer, then Java socket connect hangs. It's no problem to continue in this case.
        } catch (InterruptedException e) {
            log(LogLevel.LL_WARNING, logDomain, "warning: RemoteServer::prepareDelete() - Interrupted waiting for connector thread.");
        }

        //JavaOnlyBlock
        if (adminInterface != null && adminInterface.isReady()) {
            adminInterface.delete();
        }

        log(LogLevel.LL_DEBUG, logDomain, "RemoteServer: Disconnecting");
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
            statusString = DISCONNECTING;
            if (bulk != null) {
                bulk.disconnect();
                bulk = null; // needed afterwards so commmented out
            }
            if (express != null) {
                express.disconnect();
                express = null; // needed afterwards so commmented out
            }

            // reset subscriptions - possibly delete elements
            portIterator.reset();
            for (ProxyPort pp = portIterator.next(); pp != null; pp = portIterator.next()) {
                if (peer.deletePortsOnDisconnect()) {
                    pp.managedDelete();
                } else {
                    pp.reset();
                }
            }
            portIterator.reset();

            if (peer.deletePortsOnDisconnect()) {
                elemIterator.reset();
                for (ProxyFrameworkElement pp = elemIterator.next(); pp != null; pp = elemIterator.next()) {
                    pp.managedDelete();
                }
                elemIterator.reset();
            }
            statusString = null;
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
            super(null, "(yet unknown)", CoreFlags.ALLOWS_CHILDREN | CoreFlags.NETWORK_ELEMENT | FrameworkElementInfo.filterParentFlags(extraFlags), lockOrder);
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
            if (getName().equals(info.getLink(0).name)) {
                return false;
            }
            return true;
        }

        /**
         * Update information about framework element
         *
         * @param info Information
         */
        public synchronized void updateFromPortInfo(@Const @Ref FrameworkElementInfo info) {
            if (!isReady()) {
                assert(info.opCode == RuntimeListener.ADD) : "only add operation may change framework element before initialization";
                assert(info.getLinkCount() == 1) : "Framework elements currently may not be linked";
                setName(info.getLink(0).name);
                getFrameworkElement(info.getLink(0).parent, info.getLink(0).extraFlags, false, info.getLink(0).parent).addChild(this);
            }
            if ((info.getFlags() & CoreFlags.FINSTRUCTED) > 0) {
                setFlag(CoreFlags.FINSTRUCTED);
            }
            if ((info.getFlags() & CoreFlags.FINSTRUCTABLE_GROUP) > 0) {
                setFlag(CoreFlags.FINSTRUCTABLE_GROUP);
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

        /** Handles (remote) of port's outgoing connections */
        @JavaOnly
        protected SimpleList<FrameworkElementInfo.ConnectionInfo> connections = new SimpleList<FrameworkElementInfo.ConnectionInfo>();

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
                        tmpMatchBuffer.append(getPort().getLink(i).getName());
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

                //JavaOnlyBlock
                portInfo.getConnections(connections);

                log(LogLevel.LL_DEBUG_VERBOSE_2, logDomain, "Updating port info: " + portInfo.toString());
                if (portInfo.opCode == RuntimeListener.ADD) {
                    assert(!getPort().isReady());
                    if (filter.isPortOnlyFilter()) {
                        for (int i = 1, n = portInfo.getLinkCount(); i < n; i++) {
                            FrameworkElement parent = (portInfo.getLink(i).extraFlags & CoreFlags.GLOBALLY_UNIQUE_LINK) > 0 ? globalLinks : (FrameworkElement)RemoteServer.this;
                            getPort().link(parent, portInfo.getLink(i).name);
                        }
                        FrameworkElement parent = (portInfo.getLink(0).extraFlags & CoreFlags.GLOBALLY_UNIQUE_LINK) > 0 ? globalLinks : (FrameworkElement)RemoteServer.this;
                        getPort().setName(portInfo.getLink(0).name);
                        parent.addChild(getPort());
                    } else {
                        for (@SizeT int i = 1; i < portInfo.getLinkCount(); i++) {
                            FrameworkElement parent = getFrameworkElement(portInfo.getLink(i).parent, portInfo.getLink(i).extraFlags, true, 0);
                            getPort().link(parent, portInfo.getLink(i).name);
                        }
                        getPort().setName(portInfo.getLink(0).name);
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
            checkSubscription();
        }

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
                    c.subscribe(remoteHandle, strategy, revPush, time, p.getHandle(), getEncoding());
                    subscriptionStrategy = strategy;
                    subscriptionRevPush = revPush;
                    subscriptionUpdateTime = time;
                }
            }
        }

        @Override @JavaOnly
        public List<AbstractPort> getRemoteEdgeDestinations() {
            ArrayList<AbstractPort> result = new ArrayList<AbstractPort>();
            for (int i = 0; i < connections.size(); i++) {
                ProxyPort pp = remotePortRegister.get(connections.get(i).handle);
                if (pp != null) {
                    result.add(pp.getPort());
                }
            }
            return result;
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
            cos = new OutputStreamBuffer(lmBuf, updateTimes);
            cos.writeByte(type);
            //RemoteTypes.serializeLocalDataTypes(cos);
            cos.writeType(CoreNumber.TYPE); // initialize type register
            boolean bulk = type == TCP.TCP_P2P_ID_BULK;
            String typeString = getConnectionTypeString();
            cos.writeBoolean(bulk);
            cos.flush();

            // initialize core streams
            cis = new InputStreamBuffer(socket_.getSource(), updateTimes);
            cis.setTimeout(1000);
            timeBase = cis.readLong(); // Timestamp that remote runtime was created - and relative to which time is encoded in this stream
            //updateTimes.deserialize(cis);
            DataTypeBase dt = cis.readType();
            assert(dt == CoreNumber.TYPE);
            cis.setTimeout(-1);

            @SharedPtr Reader listener = ThreadUtil.getThreadSharedPtr(new Reader("TCP Client " + typeString + "-Listener for " + getName()));
            super.reader = listener;
            @SharedPtr Writer writer = ThreadUtil.getThreadSharedPtr(new Writer("TCP Client " + typeString + "-Writer for " + getName()));
            super.writer = writer;

            if (bulk) {
                boolean newServer = (serverCreationTime < 0) || (serverCreationTime != timeBase);
                log(LogLevel.LL_DEBUG, logDomain, (newServer ? "Connecting" : "Reconnecting") + " to server " + socket_.getRemoteSocketAddress().toString() + "...");
                retrieveRemotePorts(cis, cos, updateTimes, newServer);

                //JavaOnlyBlock
                // connect to admin interface?
                if (adminInterface != null) {
                    FrameworkElement fe = getChildElement(AdminServer.QUALIFIED_PORT_NAME, false);
                    if (fe != null && fe.isPort() && fe.isReady()) {
                        ((AbstractPort)fe).connectToTarget(adminInterface.getWrapped());
                    }
                }

                //JavaOnlyBlock
                // set remote type in RemoteRuntime Annotation
                ((RemoteRuntime)getAnnotation(RemoteRuntime.class)).setRemoteTypes(updateTimes);
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
        public void processRequest(TCP.OpCode opCode) throws Exception {

            int portIndex = 0;
            NetPort p = null;
            //MethodCall mc = null;
            AbstractPort ap = null;

            switch (opCode) {
            case CHANGE_EVENT:

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
                            cis.setFactory(p.getPort());
                            p.receiveDataFromStream(cis, Time.getCoarse(), changedFlag);
                            cis.setFactory(null);
                        }
                    }
                } else {
                    cis.toSkipTarget();
                }
                break;

            case STRUCTURE_UPDATE:

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
         * @param enc Data type to use when writing data to the network
         */
        public void subscribe(int index, short strategy, boolean reversePush, short updateInterval, int localIndex, Serialization.DataEncoding enc) {
            TCPCommand command = TCP.getUnusedTCPCommand();
            command.opCode = TCP.OpCode.SUBSCRIBE;
            command.remoteHandle = index;
            command.strategy = strategy;
            command.reversePush = reversePush;
            command.updateInterval = updateInterval;
            command.localIndex = localIndex;
            command.encoding = enc;
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
            command.opCode = TCP.OpCode.UNSUBSCRIBE;
            command.remoteHandle = index;
            sendCall(command);
            //command.genericRecycle();
        }

        @Override
        public boolean sendData(long startTime) throws Exception {

            // send port data
            return super.sendDataPrototype(startTime, TCP.OpCode.SET);

        }

        @Override
        public void handlePingTimeExceed() {
            log(LogLevel.LL_WARNING, logDomain, "TCPClient warning: critical ping time exceeded");
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

        public ConnectorThread() {
            super(TCPSettings.CONNECTOR_THREAD_LOOP_INTERVAL, false, false);
            setName("TCP Connector Thread for " + RemoteServer.this.getName());
            log(LogLevel.LL_DEBUG_VERBOSE_1, logDomain, "Creating " + RemoteServer.this.getName());
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
                    if (bulk == null) {
                        ctBulk = null;
                    }
                    if (express == null) {
                        ctExpress = null;
                    }
                } catch (Exception e) {
                    log(LogLevel.LL_DEBUG_WARNING, logDomain, e);
                    Thread.sleep(2000);
                    if (bulk == null) {
                        ctBulk = null;
                    }
                    if (express == null) {
                        ctExpress = null;
                    }
                }

            } else if (ctBulk != null && ctExpress != null) {

                try {

                    // check ping times
                    long startTime = System.currentTimeMillis();
                    long mayWait = TCPSettings.getInstance().criticalPingThreshold.getValue();
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
                    log(LogLevel.LL_DEBUG_WARNING, logDomain, e);
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

    @Override
    public void runtimeEdgeChange(byte changeType, AbstractPort source, AbstractPort target) {
        runtimeChange(changeType, source);
        runtimeChange(changeType, target);
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
    public float getConnectionQuality() {
        if (bulk == null || express == null || statusString != null) {
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
    public String getPingString() {
        if (statusString != null) {
            return statusString;
        }

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

    @Override @JavaOnly
    public Integer getRemoteHandle(FrameworkElement element) {
        if (element == this) {
            return RuntimeEnvironment.getInstance().getHandle();
        } else if (element.isPort()) {
            AbstractPort ap = (AbstractPort)element;
            NetPort np = ap.asNetPort();
            if (np != null) {
                return np.getRemoteHandle();
            }
        }
        if (element instanceof ProxyFrameworkElement) {
            return ((ProxyFrameworkElement)element).remoteHandle;
        }
        return null;
    }

    @Override @JavaOnly
    public FrameworkElement getRemoteElement(int handle) {
        if (!isReady()) {
            return null;
        }
        if (handle >= 0) {
            return remotePortRegister.get(handle).getPort();
        } else if (handle == RuntimeEnvironment.getInstance().getHandle()) {
            return this;
        } else {
            return remoteElementRegister.get(-handle);
        }
    }
}
