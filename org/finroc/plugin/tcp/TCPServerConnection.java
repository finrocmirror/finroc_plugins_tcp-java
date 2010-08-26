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

import java.util.ArrayList;
import java.util.List;

import org.finroc.jc.ArrayWrapper;
import org.finroc.jc.AtomicInt;
import org.finroc.jc.MutexLockOrder;
import org.finroc.jc.Time;
import org.finroc.jc.annotation.Friend;
import org.finroc.jc.annotation.JavaOnly;
import org.finroc.jc.annotation.PassByValue;
import org.finroc.jc.annotation.Ptr;
import org.finroc.jc.annotation.SharedPtr;
import org.finroc.jc.container.SafeConcurrentlyIterableList;
import org.finroc.jc.net.NetSocket;
import org.finroc.jc.stream.ChunkedBuffer;
import org.finroc.jc.stream.LargeIntermediateStreamBuffer;
import org.finroc.jc.thread.ThreadUtil;
import org.finroc.log.LogLevel;

import org.finroc.core.ChildIterator;
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
import org.finroc.core.port.net.RemoteTypes;
import org.finroc.core.portdatabase.DataTypeRegister;
import org.finroc.core.thread.CoreLoopThreadBase;

/**
 * @author max
 *
 * Single connection to TCP Server
 *
 * (memory management: Should be created with new - deletes itself:
 *  Port set, as well as reader and writer threads hold shared_ptr to this connection object)
 *
 * Thread-safety: Reader thread is the only one that deletes ports while operating. So it can use them without lock.
 */
public final class TCPServerConnection extends TCPConnection implements RuntimeListener, FrameworkElementTreeFilter.Callback {

    /** List with connections for TCP servers in this runtime */
    static final SafeConcurrentlyIterableList<TCPServerConnection> connections = new SafeConcurrentlyIterableList<TCPServerConnection>(4, 4);

    /** Used for creating connection IDs */
    private static final AtomicInt connectionId = new AtomicInt();

    /** FrameworkElement representation of this Connection (so temporary ports are grouped and can conveniently be deleted) */
    private final PortSet portSet;

    /** Unique ids for ports in this connection */
    //private final AtomicInt portId = new AtomicInt();

    ///** lookup table for proxy ports */
    //private final int[] proxyPortLookup = new int[CoreRegister.MAX_ELEMENTS];

    /** Send information about runtime in this connection? */
    private boolean sendRuntimeInfo = false;

    /**
     * Buffer for storing serialized updated runtime information - ready to be sent -
     * (efficient & completely non-blocking solution :-) )
     */
    @PassByValue private ChunkedBuffer runtimeInfoBuffer = new ChunkedBuffer(false);

    /** For any thread that writes runtime changes - note that when declared in this order, writer will be deleted/closed before runtimeInfoBuffer (that's intended and the correct order) */
    @PassByValue private CoreOutput runtimeInfoWriter = new CoreOutput(runtimeInfoBuffer);

    /** For network writer thread that forwards runtime change information */
    @PassByValue private CoreInput runtimeInfoReader = new CoreInput(runtimeInfoBuffer.getDestructiveSource());

    /** Framework element filter to decide which data is interesting for client */
    private FrameworkElementTreeFilter elementFilter = new FrameworkElementTreeFilter();

    /** Temporary string builder - only used by reader thread */
    private StringBuilder tmp = new StringBuilder();

    /** Number of times disconnect was called, since last connect */
    private final AtomicInt disconnectCalls = new AtomicInt(0);

    /**
     * @param s Socket with new connection
     * @param streamId Stream ID for connection type (see TCP class)
     * @param peer Peer that this server belongs to
     */
    public TCPServerConnection(NetSocket s, byte streamId, TCPServer server, TCPPeer peer) throws Exception {
        super(streamId, streamId == TCP.TCP_P2P_ID_BULK ? peer : null, streamId == TCP.TCP_P2P_ID_BULK);
        socket = s;

        synchronized (this) {

            // initialize core streams (counter part to RemoteServer.Connection constructor)
            @SharedPtr LargeIntermediateStreamBuffer lmBuf = new LargeIntermediateStreamBuffer(s.getSink());
            cos = new CoreOutput(lmBuf);
            //cos = new CoreOutputStream(new BufferedOutputStreamMod(s.getOutputStream()));
            cos.writeLong(RuntimeEnvironment.getInstance().getCreationTime()); // write base timestamp
            RemoteTypes.serializeLocalDataTypes(DataTypeRegister.getInstance(), cos);
            cos.flush();

            // init port set here, since it might be serialized to stream
            portSet = new PortSet(server, this);
            portSet.init();

            cis = new CoreInput(s.getSource());
            cis.setTypeTranslation(updateTimes);
            updateTimes.deserialize(cis);

            String typeString = getConnectionTypeString();

            // send runtime information?
            if (cis.readBoolean()) {
                elementFilter.deserialize(cis);
                sendRuntimeInfo = true;
                synchronized (RuntimeEnvironment.getInstance().getRegistryLock()) { // lock runtime so that we do not miss a change
                    RuntimeEnvironment.getInstance().addListener(this);
                    elementFilter.traverseElementTree(RuntimeEnvironment.getInstance(), this, tmp);
                }
                cos.writeByte(0); // terminator
                cos.flush();
            }

            // start incoming data listener thread
            @SharedPtr Reader listener = ThreadUtil.getThreadSharedPtr(new Reader("TCP Server " + typeString + "-Listener for " + s.getRemoteSocketAddress().toString()));
            super.reader = listener;
            listener.lockObject(portSet.connectionLock);
            listener.start();

            // start writer thread
            @SharedPtr Writer writer = ThreadUtil.getThreadSharedPtr(new Writer("TCP Server " + typeString + "-Writer for " + s.getRemoteSocketAddress().toString()));
            super.writer = writer;
            writer.lockObject(portSet.connectionLock);
            writer.start();

            connections.add(this, false);
            PingTimeMonitor.getInstance(); // start ping time monitor
        }
    }

    @Override
    public void handleDisconnect() {

        // make sure that disconnect is only called once... prevents deadlocks cleaning up all the threads
        int calls = disconnectCalls.incrementAndGet();
        if (calls > 1) {
            return;
        }

        synchronized (portSet) {
            boolean portSetDeleted = portSet.isDeleted();
            synchronized (this) {
                disconnect();
                if (!portSetDeleted) {
                    portSet.managedDelete();
                }
            }
        }
    }

    @Override
    public void handlePingTimeExceed() {
        portSet.notifyPortsOfDisconnect();
    }

    @Override
    public void processRequest(byte opCode) throws Exception {

        int handle = 0;
        ServerPort p = null;

        switch (opCode) {

        case TCP.SET: // Set data command

            handle = cis.readInt();
            cis.readSkipOffset();

            //long timestamp = readTimestamp();
            p = getPort(handle, true);
            log(LogLevel.LL_DEBUG_VERBOSE_2, logDomain, "Incoming Server Command: Set " + (p != null ? p.localPort.getQualifiedName() : handle));
            if (p != null) {
                synchronized (p.getPort()) {
                    if (!p.getPort().isReady()) {
                        cis.toSkipTarget();
                    } else {
                        byte changedFlag = cis.readByte();
                        cis.setBufferSource(p.getPort());
                        p.receiveDataFromStream(cis, System.currentTimeMillis(), changedFlag);
                        cis.setBufferSource(null);
                    }
                }
            } else {
                cis.toSkipTarget();
            }
            break;

        case TCP.UNSUBSCRIBE: // Unsubscribe data

            handle = cis.readInt();
            p = getPort(handle, false);
            log(LogLevel.LL_DEBUG_VERBOSE_2, logDomain, "Incoming Server Command: Unsubscribe " + (p != null ? p.localPort.getQualifiedName() : handle));
            if (p != null && p.getPort().isReady()) { // complete disconnect
                p.managedDelete();
            }
            break;

        default:
            throw new RuntimeException("Unknown OpCode");

        case TCP.SUBSCRIBE: // Subscribe to data

            handle = cis.readInt();
            short strategy = cis.readShort();
            boolean reversePush = cis.readBoolean();
            short updateInterval = cis.readShort();
            int remoteHandle = cis.readInt();
            p = getPort(handle, true);
            log(LogLevel.LL_DEBUG_VERBOSE_2, logDomain, "Incoming Server Command: Subscribe " + (p != null ? p.localPort.getQualifiedName() : handle) + " " + strategy + " " + reversePush + " " + updateInterval + " " + remoteHandle);
            if (p != null) {
                synchronized (p.getPort().getRegistryLock()) {
                    if (p.getPort().isReady()) {
                        p.getPort().setMinNetUpdateInterval(updateInterval);
                        p.updateIntervalPartner = updateInterval;
                        p.setRemoteHandle(remoteHandle);
                        p.getPort().setReversePushStrategy(reversePush);
                        p.propagateStrategyFromTheNet(strategy);
                    }
                }
            }
            break;
        }
    }

    @Override
    public void treeFilterCallback(FrameworkElement fe) {
        if (fe != RuntimeEnvironment.getInstance()) {
            if (!fe.isDeleted()) {
                cos.writeByte(TCP.PORT_UPDATE);
                FrameworkElementInfo.serializeFrameworkElement(fe, RuntimeListener.ADD, cos, elementFilter, tmp);
            }
        }
    }

    @Override
    public void runtimeChange(byte changeType, FrameworkElement element) {
        if (element != RuntimeEnvironment.getInstance() && elementFilter.accept(element, tmp) && changeType != RuntimeListener.PRE_INIT) {
            if (changeType == EDGE_CHANGE && (!elementFilter.isAcceptAllFilter())) {
                return;
            }
            runtimeInfoWriter.writeByte(TCP.PORT_UPDATE);
            FrameworkElementInfo.serializeFrameworkElement(element, changeType, runtimeInfoWriter, elementFilter, tmp);
            if (TCPSettings.DEBUG_TCP) {
                runtimeInfoWriter.writeInt(TCPSettings.DEBUG_TCP_NUMBER);
            }
            runtimeInfoWriter.flush();
            notifyWriter();
        }
    }

    /**
     * Get Port for this connection. Creates Port if not yet existent.
     * (should only be called by reader thread with possiblyCreate=true in order to ensure thread-safety)
     *
     * @param handle Port Handle
     * @param possiblyCreate Possibly create network port if it does not exist
     * @return Port. Null, if it is not existent.
     */
    public ServerPort getPort(int handle, boolean possiblyCreate) {
        AbstractPort orgPort = RuntimeEnvironment.getInstance().getPort(handle);
        if (orgPort == null) {
            return null;
        }
        if (orgPort.isChildOf(portSet)) {
            return (ServerPort)orgPort.asNetPort();
        }
        ServerPort sp = (ServerPort)orgPort.findNetPort(this);
        if (sp == null && possiblyCreate) {
            sp = new ServerPort(orgPort, portSet);
            sp.getPort().init();
        }
        return sp;
    }

    @Override
    public boolean sendData(long startTime) throws Exception {

        // send port data
        boolean requestAcknowledgement = super.sendDataPrototype(startTime, TCP.CHANGE_EVENT);

        // updated runtime information
        while (runtimeInfoReader.moreDataAvailable()) {
            cos.writeAllAvailable(runtimeInfoReader);
        }

        return requestAcknowledgement;
    }

    /**
     * PortSet representation of this Connection (so temporary ports are grouped and can conveniently be deleted)
     */
    @Friend(TCPServerConnection.class)
    public class PortSet extends FrameworkElement {

        /** For iterating over portSet's ports */
        private final ChildIterator portIterator;

        /** Ensures that connection object exists as long as port set does */
        private final @SharedPtr TCPServerConnection connectionLock;

        public PortSet(TCPServer server, @SharedPtr TCPServerConnection connectionLock) {
            super("connection" + connectionId.getAndIncrement(), server, CoreFlags.ALLOWS_CHILDREN | CoreFlags.NETWORK_ELEMENT, LockOrderLevels.PORT - 1); // manages ports itself
            portIterator = new ChildIterator(this);
            this.connectionLock = connectionLock;
        }

        @Override
        protected void prepareDelete() {
            handleDisconnect();
            if (sendRuntimeInfo) {
                RuntimeEnvironment.getInstance().removeListener(TCPServerConnection.this);
            }
            notifyPortsOfDisconnect();
            connections.remove(TCPServerConnection.this);
            super.prepareDelete();
        }

        /**
         * Notifies ports that connection is bad/disconnected so that
         * they can apply default values if needed.
         */
        private void notifyPortsOfDisconnect() {
            portIterator.reset();
            for (FrameworkElement port = portIterator.next(); port != null; port = portIterator.next()) {
                ((AbstractPort)port).notifyDisconnect();
            }
        }

//      @Override
//      public void handleCallReturn(AbstractCall pc) {
//          pc.setRemotePortHandle(pc.popCaller());
//          TCPServerConnection.this.sendCall(pc);
//      }
    }

    /**
     * Local Port that is created for subscriptions and incoming connections.
     */
    @Friend(TCPServerConnection.class)
    public class ServerPort extends TCPPort {

        /** Local partner port */
        private final AbstractPort localPort;

        /** Edge to connect server port with local port
         * @param portSet */
        //Edge edge;

        public ServerPort(AbstractPort counterPart, PortSet portSet) {
            super(initPci(counterPart), TCPServerConnection.this.portSet.connectionLock);
            localPort = counterPart;
        }

        @Override
        protected void postChildInit() {

            super.postChildInit();

            // add edge
            if (getPort().isOutputPort()) {
                getPort().connectToTarget(localPort);
            } else {
                getPort().connectToSource(localPort);
            }
        }

        // notify any connected input ports about disconnect
        @Override
        public void notifyDisconnect() {
            if (localPort.isInputPort()) {
                localPort.notifyDisconnect();
            }
        }

//      /**
//       * Set Push strategy
//       *
//       * @param b on or off?
//       */
//      public void setPush(boolean b) {
//          if (getPort().isOutputPort()) {
//              getPort().setReversePushStrategy(b);
//          } else {
//              getPort().setPushStrategy(b);
//          }
//      }

//      /**
//       * @param queueLength maximum Queue Length
//       */
//      public void setQueueLength(int queueLength) {
//          if (getPort().getFlag(PortFlags.OUTPUT_PORT | PortFlags.HAS_QUEUE)) {
//              getPort().setMaxQueueLength(queueLength);
//          }
//      }

//      /**
//       * @return Current Minimum network update interval for this port (takes local and remote settings into account)
//       */
//      public short getMinNetUpdateInterval() {
//          short t = 0;
//          if ((t = updateIntervalPartner) >= 0) {
//              return t;
//          } else if ((t = localPort.getMinNetUpdateInterval()) >= 0) {
//              return t;
//          } else if ((t = updateTimes.getTime(getPort().getDataType())) >= 0) {
//              return t;
//          } else if ((t = getPort().getDataType().getUpdateTime()) >= 0) {
//              return t;
//          }
//          return updateTimes.getGlobalDefault();
//      }
//
//      @Override
//      protected void portChanged() {
//          notifyWriter();
//      }

        @Override
        protected void propagateStrategyOverTheNet() {
            // data is propagated automatically with port strategy changed in framework element class
        }

        @Override @JavaOnly
        public List<AbstractPort> getRemoteEdgeDestinations() {
            log(LogLevel.LL_DEBUG_WARNING, logDomain, "remote server ports have no info on remote edges");
            return new ArrayList<AbstractPort>();
        }
    }

    /**
     * (belongs to ServerPort)
     * Create matching port creation info to access specified port.
     *
     * @param counterPart Port that will be accessed
     * @return Port Creation Info
     */
    private PortCreationInfo initPci(AbstractPort counterPart) {
        PortCreationInfo pci = new PortCreationInfo(0);
        pci.maxQueueSize = 0;
        pci.parent = portSet;
        int flags = 0;
        if (counterPart.isOutputPort()) {
            // create input port
            flags |= PortFlags.HAS_QUEUE | PortFlags.ACCEPTS_DATA | PortFlags.USES_QUEUE;

        } else {
            // create output io port
            flags |= PortFlags.IS_OUTPUT_PORT /*| PortFlags.ACCEPTS_REVERSE_DATA*/ | PortFlags.MAY_ACCEPT_REVERSE_DATA | PortFlags.EMITS_DATA;
        }
        pci.flags = flags;
        pci.dataType = counterPart.getDataType();
        pci.lockOrder = LockOrderLevels.REMOTE_PORT;
        return pci;
    }

    /**
     * Monitors connections for critical ping time exceed
     */
    @Friend(TCPServerConnection.class) @Ptr
    public static class PingTimeMonitor extends CoreLoopThreadBase {

        @SharedPtr private static PingTimeMonitor instance;

        /** Locked before thread list (in C++) */
        @SuppressWarnings("unused")
        private static final MutexLockOrder staticClassMutex = new MutexLockOrder(LockOrderLevels.INNER_MOST - 20);

        private PingTimeMonitor() {
            super(TCPSettings.CONNECTOR_THREAD_LOOP_INTERVAL, false, false);
            setName("TCP Server Ping Time Monitor");
        }

        private synchronized static PingTimeMonitor getInstance() {
            if (instance == null) {
                instance = ThreadUtil.getThreadSharedPtr(new PingTimeMonitor());
                instance.start();
            }
            return instance;
        }


        @Override
        public void mainLoopCallback() throws Exception {

            long startTime = Time.getCoarse();
            long mayWait = TCPSettings.criticalPingThreshold.get();

            @Ptr ArrayWrapper<TCPServerConnection> it = connections.getIterable();
            for (int i = 0, n = connections.size(); i < n; i++) {
                TCPServerConnection tsc = it.get(i);
                if (tsc != null) {
                    mayWait = Math.min(mayWait, tsc.checkPingForDisconnect()); // safe, since connection is deleted deferred and call time is minimal
                }
            }

            // wait remaining uncritical time
            long waitFor = mayWait - (Time.getCoarse() - startTime);
            if (waitFor > 0) {
                Thread.sleep(waitFor);
            }
        }
    }

    @Override
    protected TCPPort lookupPortForCallHandling(int portHandle) {
        return getPort(portHandle, true);
    }
}
