/**
 * You received this file as part of Finroc
 * A Framework for intelligent robot control
 *
 * Copyright (C) Finroc GbR (finroc.org)
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.finroc.core.FrameworkElement;
import org.finroc.core.FrameworkElementFlags;
import org.finroc.core.FrameworkElementTags;
import org.finroc.core.LockOrderLevels;
import org.finroc.core.RuntimeEnvironment;
import org.finroc.core.admin.AdminClient;
import org.finroc.core.admin.AdministrationService;
import org.finroc.core.datatype.FrameworkElementInfo;
import org.finroc.core.port.AbstractPort;
import org.finroc.core.port.PortCreationInfo;
import org.finroc.core.port.cc.CCPortBase;
import org.finroc.core.port.cc.CCPortDataManager;
import org.finroc.core.port.cc.CCPortDataManagerTL;
import org.finroc.core.port.cc.CCPullRequestHandler;
import org.finroc.core.port.net.NetPort;
import org.finroc.core.port.rpc.RPCInterfaceType;
import org.finroc.core.port.rpc.internal.AbstractCall;
import org.finroc.core.port.rpc.internal.RPCMessage;
import org.finroc.core.port.rpc.internal.RPCPort;
import org.finroc.core.port.rpc.internal.RPCRequest;
import org.finroc.core.port.rpc.internal.RPCResponse;
import org.finroc.core.port.rpc.internal.AbstractCall.CallType;
import org.finroc.core.port.std.PortBase;
import org.finroc.core.port.std.PortDataManager;
import org.finroc.core.port.std.PullRequestHandler;
import org.finroc.core.portdatabase.CCType;
import org.finroc.core.portdatabase.FinrocTypeInfo;
import org.finroc.core.remote.ModelNode;
import org.finroc.core.remote.RemoteFrameworkElement;
import org.finroc.core.remote.RemotePort;
import org.finroc.core.remote.RemoteRuntime;
import org.finroc.core.remote.RemoteTypes;
import org.finroc.plugins.tcp.internal.TCP.OpCode;
import org.rrlib.finroc_core_utils.jc.AtomicInt;
import org.rrlib.finroc_core_utils.log.LogLevel;
import org.rrlib.finroc_core_utils.rtti.DataTypeBase;
import org.rrlib.finroc_core_utils.serialization.InputStreamBuffer;
import org.rrlib.finroc_core_utils.serialization.MemoryBuffer;
import org.rrlib.finroc_core_utils.serialization.Serialization;

/**
 * @author Max Reichardt
 *
 * Class that represent a remote runtime environment.
 * It creates a proxy port for each shared port in the remote runtime.
 */
public class RemotePart extends FrameworkElement implements PullRequestHandler, CCPullRequestHandler {

    /** Peer info that this part belongs to */
    final PeerInfo peerInfo;

    /** Peer implementation that this remote part belongs to */
    final TCPPeer peerImplementation;

    /** Connection to transfer "express" ports data */
    TCPConnection expressConnection;

    /** Connection to transfer "bulk" ports data */
    TCPConnection bulkConnection;

    /** Connection to transfer all other "management" data */
    TCPConnection managementConnection;

    /** Structure information to send to remote part */
    private FrameworkElementInfo.StructureExchange sendStructureInfo = FrameworkElementInfo.StructureExchange.NONE;

    /** Framework element that contains all global links - possibly NULL */
    private FrameworkElement globalLinks;

    /** Framework element that contains all server ports - possibly NULL */
    private FrameworkElement serverPorts;

    /**
     * Lookup for remote framework elements (currently not ports) - similar to remote CoreRegister
     * (should only be accessed by reader thread of management connection)
     */
    private HashMap<Integer, ProxyPort> remotePortRegister = new HashMap<Integer, ProxyPort>();

    /** Temporary buffer for match checks (only used by bulk reader or connector thread) */
    private final StringBuilder tmpMatchBuffer = new StringBuilder();

    /** Administration interface client port */
    private final AdminClient adminInterface;

    /** Remote part's current model node */
    private RemoteRuntime currentModelNode;

    /**
     * Remote part's new model node. Is created on new connection.
     * CurrentModelNode is replaced with this, as soon as connection is fully initialized.
     * As this is not published yet, we can operate on this with any thread.
     */
    private RemoteRuntime newModelNode;

    /** List with remote ports that have not been initialized yet */
    private ArrayList<ProxyPort> uninitializedRemotePorts = new ArrayList<ProxyPort>();

    /** Next call id to assign to sent call */
    AtomicLong nextCallId = new AtomicLong();

    /** Pull calls that wait for a response */
    private ArrayList<PullCall> pullCallsAwaitingResponse = new ArrayList<PullCall>();

    /** Number of times disconnect was called, since last connect */
    private final AtomicInt disconnectCalls = new AtomicInt(0);


    /** Peer info this part is associated with */
    RemotePart(PeerInfo peerInfo, FrameworkElement parent, TCPPeer peerImplementation) {
        super(parent, peerInfo.toString());
        this.peerInfo = peerInfo;
        this.peerImplementation = peerImplementation;
        adminInterface = (peerImplementation.structureExchange == FrameworkElementInfo.StructureExchange.FINSTRUCT) ?
                         new AdminClient("AdminClient " + getName(), peerImplementation.connectionElement) : null;
    }

    /**
     * Add connection for this remote part
     *
     * @param connection Connection to add (with flags set)
     * @return Did this succeed? (fails if there already is a connection for specified types of data; may happen if two parts try to connect at the same time - only one connection is kept)
     */
    boolean addConnection(TCPConnection connection) {
        if (connection.bulk) {
            if (bulkConnection != null) {
                return false;
            }
            bulkConnection = connection;
        } else {
            if (expressConnection != null || managementConnection != null) {
                return false;
            }
            expressConnection = connection;
            managementConnection = connection;
        }

        if (bulkConnection != null && expressConnection != null && managementConnection != null) {
            peerInfo.connected = true;
            log(LogLevel.LL_DEBUG, logDomain, "Connected to " + peerInfo.toString());
        }

        return true;
    }

    /**
     * Called during initial structure exchange
     *
     * @param info Info on another remote framework element
     * @param initalStructure Is this call originating from initial structure exchange?
     * @param remoteRuntime Remote runtime object to add structure to
     */
    void addRemoteStructure(FrameworkElementInfo info, boolean initalStructureExchange) {
        RemoteRuntime remoteRuntime = initalStructureExchange ? newModelNode : currentModelNode;
        logDomain.log(LogLevel.LL_DEBUG_VERBOSE_1, getLogDescription(), "Adding element: " + info.toString());
        if (info.isPort()) {
            ProxyPort port = new ProxyPort(info);
            for (int i = 0; i < info.getLinkCount(); i++) {
                RemoteFrameworkElement remoteElement = new RemotePort(info.getHandle(), info.getLink(i).name, port.getPort(), i);
                if (i == 0) {
                    remoteRuntime.elementLookup.put(info.getHandle(), remoteElement);
                }
                remoteElement.setUserObject(info.getLink(i).name);
                remoteElement.setTags(info.getTags());
                remoteElement.setFlags(info.getFlags());
                ModelNode parent = getFrameworkElement(info.getLink(i).parent, remoteRuntime);
                if (initalStructureExchange) {
                    parent.add(remoteElement);
                } else {
                    peerImplementation.connectionElement.getModelHandler().addNode(parent, remoteElement);
                    if (peerImplementation.structureExchange == FrameworkElementInfo.StructureExchange.SHARED_PORTS) {
                        port.getPort().init();
                    } else {
                        uninitializedRemotePorts.add(port);
                    }
                }
            }
        } else {
            RemoteFrameworkElement remoteElement = getFrameworkElement(info.getHandle(), remoteRuntime);
            remoteElement.setUserObject(info.getLink(0).name);
            remoteElement.setTags(info.getTags());
            remoteElement.setFlags(info.getFlags());
            ModelNode parent = getFrameworkElement(info.getLink(0).parent, remoteRuntime);
            if (initalStructureExchange) {
                parent.add(remoteElement);
            } else {
                peerImplementation.connectionElement.getModelHandler().addNode(parent, remoteElement);
            }
        }
    }

    /**
     * Creates new model of remote part
     */
    void createNewModel() {
        newModelNode = new RemoteRuntime(peerInfo.toString(), adminInterface, managementConnection.remoteTypes);
        newModelNode.setFlags(RuntimeEnvironment.getInstance().getAllFlags());
    }

    /**
     * Creates qualified link for element of remote framework element model
     *
     * @param remoteElement Element to create link for
     * @return Created Link
     */
    private String createPortName(RemoteFrameworkElement remoteElement) {
        return remoteElement.getQualifiedLink();
    }

    /**
     * Deletes all child elements of remote part
     */
    void deleteAllChildren() {
        ChildIterator ci = new ChildIterator(this);
        FrameworkElement child;
        while ((child = ci.next()) != null) {
            child.managedDelete();
        }
        remotePortRegister = new HashMap<Integer, ProxyPort>();
    }

    /**
     * Disconnects remote part
     */
    public void disconnect() {
        // make sure that disconnect is only called once... prevents deadlocks cleaning up all the threads
        int calls = disconnectCalls.incrementAndGet();
        if (calls > 1) {
            return;
        }

        synchronized (peerImplementation.connectTo) {
            if (managementConnection != null) {
                managementConnection.disconnect();
            }
            if (expressConnection != null) {
                expressConnection.disconnect();
            }
            if (bulkConnection != null) {
                bulkConnection.disconnect();
            }
            disconnectCalls.set(0);
        }
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
     * @return Connection quality (see ExternalConnection)
     */
    public float getConnectionQuality() {
        if (bulkConnection == null || expressConnection == null || managementConnection == null || (!peerInfo.connected)) {
            return 0;
        }
        float pingTime = 0;
        for (int i = 0; i < 3; i++) {
            TCPConnection c = (i == 0) ? managementConnection : (i == 1 ? bulkConnection : expressConnection);
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
     * Returns framework element with specified handle.
     * Creates one if it doesn't exist.
     *
     * @param handle Remote handle of framework element
     * @param remoteRuntime Remote runtime model to use for lookup
     * @return Framework element.
     */
    public RemoteFrameworkElement getFrameworkElement(int handle, RemoteRuntime remoteRuntime) {
        RemoteFrameworkElement remoteElement = remoteRuntime.elementLookup.get(handle);
        if (remoteElement == null) {
            remoteElement = new RemoteFrameworkElement(handle, "(unknown)");
            remoteRuntime.elementLookup.put(handle, remoteElement);
        }
        return remoteElement;
    }

    /**
     * @return Framework element that contains all global links (possibly created by call to this)
     */
    public FrameworkElement getGlobalLinkElement() {
        if (globalLinks == null) {
            globalLinks = new FrameworkElement(this, "global", Flag.NETWORK_ELEMENT | Flag.GLOBALLY_UNIQUE_LINK | Flag.ALTERNATIVE_LINK_ROOT, -1);
        }
        return globalLinks;
    }

    /**
     * @return String containing ping times
     */
    public String getPingString() {
        if (!peerInfo.connected) {
            return "disconnected";
        }

        int pingAvg = 0;
        int pingMax = 0;
        int dataRate = 0;
        String s = "ping (avg/max/Rx): ";
        if (bulkConnection == null || expressConnection == null) { // should be disconnected... but maybe this is even safer
            return s + "- ";
        }
        for (int i = 0; i < 3; i++) {
            TCPConnection c = (i == 0) ? managementConnection : (i == 1 ? bulkConnection : expressConnection);
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
     * Initializes part and checks for admin port to connect to
     *
     * @param obsoleteNode Model node that is now obsolete
     */
    public void initAndCheckForAdminPort(final ModelNode obsoleteNode) {

        if (peerImplementation.structureExchange != FrameworkElementInfo.StructureExchange.SHARED_PORTS) {

            // expand port names
            synchronized (getRegistryLock()) {
                for (ProxyPort pp : remotePortRegister.values()) {
                    RemotePort[] remotePorts = RemotePort.get(pp.getPort());
                    for (int i = 0; i < remotePorts.length; i++) {
                        pp.getPort().setName(createPortName(remotePorts[i]), i);
                    }
                }
            }
        }

        this.init();

        // connect to admin interface?
        if (adminInterface != null) {
            FrameworkElement fe = getChildElement(AdministrationService.QUALIFIED_PORT_NAME, false);
            if (fe != null && fe.isPort() && fe.isReady()) {
                ((AbstractPort)fe).connectTo(adminInterface.getWrapped());
            } else {
                log(LogLevel.LL_ERROR, logDomain, "Could not find administration port to connect to.");
            }
        }

        // set remote type in RemoteRuntime Annotation
        //((RemoteRuntime)getAnnotation(RemoteRuntime.class)).setRemoteTypes(managementConnection.updateTimes);
        final RemoteRuntime oldModel = currentModelNode;
        final RemoteRuntime newModel = newModelNode;
        currentModelNode = newModelNode;
        newModelNode = null;

        if (oldModel != null) {
            peerImplementation.connectionElement.getModelHandler().removeNode(obsoleteNode);
            peerImplementation.connectionElement.getModelHandler().replaceNode(oldModel, newModel);
        } else {
            peerImplementation.connectionElement.getModelHandler().replaceNode(obsoleteNode, newModel);
        }
    }

    /**
     * Process message with specified opcode in provided stream
     *
     * @param opCode Opcode of message
     * @param stream Stream to read message from
     * @param remoteTypes Info on remote types
     * @param connection Connection this was called from
     * @param elementInfoBuffer Framework element info buffer to use
     */
    public void processMessage(OpCode opCode, InputStreamBuffer stream, RemoteTypes remoteTypes, TCPConnection connection) throws Exception {
        log(LogLevel.LL_DEBUG_VERBOSE_1, logDomain, "Processing message " + opCode.toString());

        switch (opCode) {
        case PORT_VALUE_CHANGE:
        case SMALL_PORT_VALUE_CHANGE:
        case SMALL_PORT_VALUE_CHANGE_WITHOUT_TIMESTAMP:
            int handle = stream.readInt();
            Serialization.DataEncoding encoding = stream.readEnum(Serialization.DataEncoding.class);
            AbstractPort port = RuntimeEnvironment.getInstance().getPort(handle);
            if (port != null && port.isReady() && (!FinrocTypeInfo.isMethodType(port.getDataType(), true))) {
                NetPort netPort = port.asNetPort();
                if (netPort != null) {
                    netPort.receiveDataFromStream(stream, encoding, opCode != OpCode.SMALL_PORT_VALUE_CHANGE_WITHOUT_TIMESTAMP);
                }
            }
            break;
        case RPC_CALL:
            handle = stream.readInt();
            CallType callType = stream.readEnum(CallType.class);
            DataTypeBase type = stream.readType();
            byte methodId = stream.readByte();
            if (!(type instanceof RPCInterfaceType)) {
                log(LogLevel.LL_WARNING, logDomain, "Type " + type.getName() + " is no RPC type. Ignoring call.");
                return;
            }
            RPCInterfaceType rpcInterfaceType = (RPCInterfaceType)type;

            if (callType == CallType.RPC_MESSAGE || callType == CallType.RPC_REQUEST) {
                port = RuntimeEnvironment.getInstance().getPort(handle);
                if (port != null && rpcInterfaceType == port.getDataType()) {
                    //RPCDeserializationScope deserializationScope(message.Get<0>(), connection.rpcCallBufferPools); // TODO?
                    if (callType == CallType.RPC_MESSAGE) {
                        RPCMessage.deserializeAndExecuteCallImplementation(stream, (RPCPort)port, methodId);
                    } else {
                        RPCRequest.deserializeAndExecuteCallImplementation(stream, (RPCPort)port, methodId, connection);
                    }
                }
            } else { // type is RPC response
                long callId = stream.readLong();

                AbstractCall callAwaitingThisResponse = null;
                synchronized (connection.callsAwaitingResponse) {
                    for (TCPConnection.CallAndTimeout call : connection.callsAwaitingResponse) {
                        if (call.call.getCallId() == callId) {
                            callAwaitingThisResponse = call.call;
                            connection.callsAwaitingResponse.remove(call);
                            break;
                        }
                    }
                }
                if (callAwaitingThisResponse == null) { // look in other connection; TODO: think about rules which connection to use for RPCs
                    TCPConnection otherConnection = (connection != expressConnection) ? expressConnection : bulkConnection;
                    synchronized (otherConnection.callsAwaitingResponse) {
                        for (TCPConnection.CallAndTimeout call : otherConnection.callsAwaitingResponse) {
                            if (call.call.getCallId() == callId) {
                                callAwaitingThisResponse = call.call;
                                otherConnection.callsAwaitingResponse.remove(call);
                                break;
                            }
                        }
                    }
                }
                if (callAwaitingThisResponse != null) {
                    port = RuntimeEnvironment.getInstance().getPort(callAwaitingThisResponse.getLocalPortHandle());
                    if (port != null) {
                        //RPCDeserializationScope deserializationScope(callAwaitingThisResponse.getLocalPortHandle(), connection.rpcCallBufferPools); // TODO?
                        RPCResponse.deserializeAndExecuteCallImplementation(stream, rpcInterfaceType.getMethod(methodId), connection, callAwaitingThisResponse);
                        return;
                    }
                }
                RPCResponse.deserializeAndExecuteCallImplementation(stream, rpcInterfaceType.getMethod(methodId), connection, null);
            }
            break;
        case PULLCALL:
            handle = stream.readInt();
            long callUid = stream.readLong();
            encoding = stream.readEnum(Serialization.DataEncoding.class);

            SerializedTCPCommand pullCallReturn = new SerializedTCPCommand(TCP.OpCode.PULLCALL_RETURN, 8192);
            pullCallReturn.getWriteStream().writeLong(callUid);
            port = RuntimeEnvironment.getInstance().getPort(handle);
            if (port != null && port.isReady() && (!FinrocTypeInfo.isMethodType(port.getDataType(), true))) {
                pullCallReturn.getWriteStream().writeBoolean(false);
                if (FinrocTypeInfo.isStdType(port.getDataType())) {
                    CCPortBase pb = (CCPortBase)port;
                    CCPortDataManager manager = pb.getPullInInterthreadContainerRaw(true, true);
                    pullCallReturn.getWriteStream().writeBoolean(true);
                    pullCallReturn.getWriteStream().writeType(manager.getObject().getType());
                    manager.getTimestamp().serialize(pullCallReturn.getWriteStream());
                    manager.getObject().serialize(pullCallReturn.getWriteStream(), encoding);
                    manager.recycle2();
                } else {
                    PortBase pb = (PortBase)port;
                    PortDataManager manager = pb.getPullLockedUnsafe(true, true);
                    pullCallReturn.getWriteStream().writeBoolean(true);
                    pullCallReturn.getWriteStream().writeType(manager.getType());
                    manager.getTimestamp().serialize(pullCallReturn.getWriteStream());
                    manager.getObject().serialize(pullCallReturn.getWriteStream(), encoding);
                    manager.releaseLock();
                }
            } else {
                pullCallReturn.getWriteStream().writeBoolean(false);
            }
            connection.sendCall(pullCallReturn);
            break;
        case PULLCALL_RETURN:
            callUid = stream.readLong();
            boolean failed = stream.readBoolean();

            synchronized (pullCallsAwaitingResponse) {
                PullCall pullCall = null;
                for (PullCall pullCallWaiting : pullCallsAwaitingResponse) {
                    if (pullCallWaiting.callId == callUid) {
                        pullCall = pullCallWaiting;
                        break;
                    }
                }
                if (pullCall != null) {
                    synchronized (pullCall) {
                        if (!failed) {
                            type = stream.readType();
                            if (pullCall.origin != null) {
                                if (pullCall.origin.getDataType() == type) {
                                    PortDataManager manager = pullCall.origin.getUnusedBufferRaw();
                                    manager.getTimestamp().deserialize(stream);
                                    manager.getObject().deserialize(stream, pullCall.encoding);
                                }
                            } else if (pullCall.ccResultBuffer != null) {
                                if (pullCall.ccResultBuffer.getObject().getType() == type) {
                                    pullCall.ccResultBuffer.getTimestamp().deserialize(stream);
                                    pullCall.ccResultBuffer.getObject().deserialize(stream, pullCall.encoding);
                                }
                            }
                        }
                        pullCall.notify();
                    }
                }
            }
            break;
//        case SUBSCRIBE:
//            // TODO
//            SubscribeMessage message;
//            message.deserialize(stream);
//
//            // Get or create server port
//            auto it = serverPortMap.find(message.Get<0>());
//            if (it != serverPortMap.end()) {
//                dataPorts::GenericPort port = it.second;
//                NetworkPortInfo* networkPortInfo = port.GetAnnotation<NetworkPortInfo>();
//                networkPortInfo.setServerSideSubscriptionData(message.Get<1>(), message.Get<2>(), message.Get<3>(), message.Get<5>());
//
//                boolean pushStrategy = message.Get<1>() > 0;
//                boolean reversePushStrategy = message.Get<2>();
//                if (port.getWrapped().pushStrategy() != pushStrategy || port.getWrapped().reversePushStrategy() != reversePushStrategy) {
//                    // flags need to be changed
//                    thread::Lock lock(getStructureMutex(), false);
//                    if (lock.tryLock()) {
//                        if (port.getWrapped().pushStrategy() != pushStrategy) {
//                            port.getWrapped().setPushStrategy(pushStrategy);
//                        }
//                        if (port.getWrapped().reversePushStrategy() != reversePushStrategy) {
//                            port.getWrapped().setReversePushStrategy(reversePushStrategy);
//                        }
//                    }
//                    else {
//                        return true; // We could not obtain lock - try again later
//                    }
//                }
//            }
//            else {
//                thread::Lock lock(getStructureMutex(), false);
//                if (lock.tryLock()) {
//                    // Create server port
//                    AbstractPort* port = RuntimeEnvironment::getInstance().getPort(message.Get<0>());
//                    if ((!port) || (!port.isReady())) {
//                        fINROC_LOG_PRINT(DEBUG_WARNING, "Port for subscription not available");
//                        return false;
//                    }
//
//                    Flags flags = Flag::NETWORK_ELEMENT | Flag::VOLATILE;
//                    if (port.isOutputPort()) {
//                        flags |= Flag::ACCEPTS_DATA; // create input port
//                    }
//                    else {
//                        flags |= Flag::OUTPUT_PORT | Flag::EMITS_DATA; // create output io port
//                    }
//                    if (sendStructureInfo != StructureExchange::SHARED_PORTS) {
//                        flags |= Flag::TOOL_PORT;
//                    }
//                    if (message.Get<1>() > 0) {
//                        flags |= Flag::PUSH_STRATEGY;
//                    }
//                    if (message.Get<2>()) {
//                        flags |= Flag::PUSH_STRATEGY_REVERSE;
//                    }
//
//                    dataPorts::GenericPort createdPort(port.getQualifiedName().substr(1), getServerPortsElement(), port.getDataType(), flags, message.Get<3>());
//                    NetworkPortInfo* networkPortInfo = new NetworkPortInfo(*this, message.Get<4>(), message.Get<1>(), true, *createdPort.getWrapped());
//                    networkPortInfo.setServerSideSubscriptionData(message.Get<1>(), message.Get<2>(), message.Get<3>(), message.Get<5>());
//                    createdPort.addPortListenerForPointer(*networkPortInfo);
//                    createdPort.init();
//                    createdPort.connectTo(port);
//                    serverPortMap.insert(pair<FrameworkElementHandle, dataPorts::GenericPort>(message.Get<0>(), createdPort));
//                    fINROC_LOG_PRINT(DEBUG, "Created server port ", createdPort.getWrapped().getQualifiedName());
//                }
//                else {
//                    return true; // We could not obtain lock - try again later
//                }
//            }
//            break;
//        case UNSUBSCRIBE:
//            // TODO
//            UnsubscribeMessage message;
//            message.deserialize(stream);
//            auto it = serverPortMap.find(message.Get<0>());
//            if (it != serverPortMap.end()) {
//                thread::Lock lock(getStructureMutex(), false);
//                if (lock.tryLock()) {
//                    it.second.getWrapped().managedDelete();
//                    serverPortMap.erase(message.Get<0>());
//                }
//                else {
//                    return true; // We could not obtain lock - try again later
//                }
//            }
//            else {
//                fINROC_LOG_PRINT(DEBUG_WARNING, "Port for unsubscribing not available");
//                return false;
//            }
//            break;
        default:
            throw new Exception("Opcode " + opCode.toString() + " not implemented yet.");
        }
    }

    /**
     * Processes buffer containing change events regarding remote runtime.
     * Must be executed from model thread.
     *
     * @param structureBufferToProcess Buffer containing serialized structure changes
     * @param remoteTypes Info on remote types
     */
    public void processStructurePacket(MemoryBuffer structureBufferToProcess, RemoteTypes remoteTypes) {
        FrameworkElementInfo info = new FrameworkElementInfo();
        InputStreamBuffer stream = new InputStreamBuffer(structureBufferToProcess, remoteTypes);
        while (stream.moreDataAvailable()) {
            TCP.OpCode opcode = stream.readEnum(TCP.OpCode.class);
            if (opcode == TCP.OpCode.STRUCTURE_CREATE) {
                info.deserialize(stream, peerImplementation.structureExchange);
                addRemoteStructure(info, false);
            } else if (opcode == TCP.OpCode.STRUCTURE_CHANGE) {
                int handle = stream.readInt();
                int flags = stream.readInt();
                short strategy = stream.readShort();
                short updateInterval = stream.readShort();
                ProxyPort port = remotePortRegister.get(handle);
                if (port != null) {
                    boolean connections = peerImplementation.structureExchange == FrameworkElementInfo.StructureExchange.FINSTRUCT;
                    if (connections) {
                        info.deserializeConnections(stream);
                    }
                    port.update(flags, strategy, updateInterval, connections ? info.getConnections() : null);

                    RemotePort[] modelElements = RemotePort.get(port.getPort());
                    for (RemotePort modelElement : modelElements) {
                        modelElement.setFlags(flags);
                    }
                } else {
                    RemoteFrameworkElement element = currentModelNode.getRemoteElement(handle);
                    if (element != null) {
                        element.setFlags(flags);
                    }
                }
            } else if (opcode == TCP.OpCode.STRUCTURE_DELETE) {
                int handle = stream.readInt();
                ProxyPort port = remotePortRegister.get(handle);
                if (port != null) {
                    RemotePort[] modelElements = RemotePort.get(port.getPort());
                    port.managedDelete();
                    for (RemotePort modelElement : modelElements) {
                        peerImplementation.connectionElement.getModelHandler().removeNode(modelElement);
                    }
                    currentModelNode.elementLookup.remove(handle);
                    uninitializedRemotePorts.remove(port);
                } else {
                    RemoteFrameworkElement element = currentModelNode.getRemoteElement(handle);
                    if (element != null) {
                        peerImplementation.connectionElement.getModelHandler().removeNode(element);
                        currentModelNode.elementLookup.remove(handle);
                    }
                }
            } else {
                log(LogLevel.LL_WARNING, logDomain, "Received corrupted structure info. Skipping packet");
                return;
            }
            TCPConnection.checkCommandEnd(stream);
        }

        // Initialize ports whose links are now complete
        synchronized (getRegistryLock()) {
            for (int i = 0; i < uninitializedRemotePorts.size(); i++) {
                ProxyPort port = uninitializedRemotePorts.get(i);
                RemotePort[] remotePorts = RemotePort.get(port.getPort());
                boolean complete = true;
                for (RemotePort remotePort : remotePorts) {
                    complete |= remotePort.isNodeAncestor(currentModelNode);
                }
                if (complete) {
                    for (int j = 0; j < remotePorts.length; j++) {
                        port.getPort().setName(createPortName(remotePorts[j]), j);
                    }
                    port.getPort().init();
                    uninitializedRemotePorts.remove(i);
                    i--;
                }
            }
        }
    }

    @Override
    public boolean pullRequest(CCPortBase origin, CCPortDataManagerTL resultBuffer, boolean intermediateAssign) {
        NetPort netport = origin.asNetPort();
        if (netport != null && expressConnection != null) {
            PullCall pullCall = new PullCall(netport);
            pullCall.ccResultBuffer = resultBuffer;
            synchronized (pullCallsAwaitingResponse) {
                pullCallsAwaitingResponse.add(pullCall);
            }
            synchronized (pullCall) {
                expressConnection.sendCall(pullCall);
                try {
                    pullCall.wait(1000);
                } catch (InterruptedException e) {}
            }
            synchronized (pullCallsAwaitingResponse) {
                pullCallsAwaitingResponse.remove(pullCall);
            }
            if (pullCall.ccPullSuccess) {
                return true;
            }
        }
        origin.getRaw(resultBuffer.getObject(), true);
        return true;
    }

    @Override
    public PortDataManager pullRequest(PortBase origin, byte addLocks, boolean intermediateAssign) {
        NetPort netport = origin.asNetPort();
        if (netport != null && expressConnection != null) {
            PullCall pullCall = new PullCall(netport);
            pullCall.origin = origin;
            synchronized (pullCallsAwaitingResponse) {
                pullCallsAwaitingResponse.add(pullCall);
            }
            synchronized (pullCall) {
                expressConnection.sendCall(pullCall);
                try {
                    pullCall.wait(1000);
                } catch (InterruptedException e) {}
            }
            synchronized (pullCallsAwaitingResponse) {
                pullCallsAwaitingResponse.remove(pullCall);
            }
            if (pullCall.resultBuffer != null) {
                pullCall.resultBuffer.getCurrentRefCounter().setLocks((byte)(addLocks));
                return pullCall.resultBuffer;
            }
        }
        PortDataManager pd = origin.lockCurrentValueForRead();
        pd.getCurrentRefCounter().addLocks((byte)(addLocks - 1)); // we already have one lock
        return pd;
    }

    /**
     * Removes connection for this remote part
     *
     * @param connection Connection to remove
     */
    void removeConnection(TCPConnection connection) {
        if (connection == managementConnection) {
            managementConnection = null;
            deleteAllChildren();
            globalLinks = null;
            serverPorts = null;
            uninitializedRemotePorts.clear();
            pullCallsAwaitingResponse.clear();
            if (currentModelNode != null) {
                peerImplementation.connectionElement.getModelHandler().removeNode(currentModelNode);
            }
            currentModelNode = null;
        }
        if (connection == expressConnection) {
            expressConnection = null;
        }
        if (connection == bulkConnection) {
            bulkConnection = null;
        }
        if (peerInfo.connected) {
            peerInfo.lastConnection = System.currentTimeMillis();
        }
        peerInfo.connected = false;
    }

    /**
     * @param sendStructureInfo Structure information to send to remote part
     */
    void setDesiredStructureInfo(FrameworkElementInfo.StructureExchange sendStructureInfo) {
        if (sendStructureInfo == FrameworkElementInfo.StructureExchange.NONE) {
            return;
        }
        if (this.sendStructureInfo != FrameworkElementInfo.StructureExchange.NONE && this.sendStructureInfo != sendStructureInfo) {
            log(LogLevel.LL_WARNING, logDomain, "Desired structure info already set to " + this.sendStructureInfo.toString() + ". This is likely to cause trouble.");
        }
        this.sendStructureInfo = sendStructureInfo;
    }

    /**
     * Local port that acts as proxy for ports on remote machines
     */
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
        protected ArrayList<FrameworkElementInfo.ConnectionInfo> connections = new ArrayList<FrameworkElementInfo.ConnectionInfo>();


        /**
         * @param portInfo Port information
         */
        public ProxyPort(FrameworkElementInfo portInfo) {
            super(createPCI(portInfo), null /*(portInfo.getFlags() & Flag.EXPRESS_PORT) > 0 ? expressConnection : bulkConnection // bulkConnection might be null*/);
            remoteHandle = portInfo.getHandle();
            remotePortRegister.put(remoteHandle, this);

            super.updateFlags(portInfo.getFlags());
            getPort().setMinNetUpdateInterval(portInfo.getMinNetUpdateInterval());
            updateIntervalPartner = portInfo.getMinNetUpdateInterval(); // TODO redundant?
            propagateStrategyFromTheNet(portInfo.getStrategy());
            connections.addAll(portInfo.getConnections());

            log(LogLevel.LL_DEBUG_VERBOSE_2, logDomain, "Updating port info: " + portInfo.toString());
            for (int i = 1, n = portInfo.getLinkCount(); i < n; i++) {
                FrameworkElement parent = portInfo.getLink(i).unique ? getGlobalLinkElement() : (FrameworkElement)RemotePart.this;
                getPort().link(parent, portInfo.getLink(i).name);
            }
            FrameworkElement parent = portInfo.getLink(0).unique ? getGlobalLinkElement() : (FrameworkElement)RemotePart.this;
            getPort().setName(portInfo.getLink(0).name);
            parent.addChild(getPort());
            FrameworkElementTags.addTags(getPort(), portInfo.getTags());

            if (getPort() instanceof CCPortBase) {
                ((CCPortBase)getPort()).setPullRequestHandler(RemotePart.this);
            } else if (getPort() instanceof PortBase) {
                ((PortBase)getPort()).setPullRequestHandler(RemotePart.this);
            }
        }

        public void update(int flags, short strategy, short minNetUpdateInterval, List<FrameworkElementInfo.ConnectionInfo> newConnections) {
            updateFlags(flags);
            getPort().setMinNetUpdateInterval(minNetUpdateInterval);
            updateIntervalPartner = minNetUpdateInterval; // TODO redundant?
            propagateStrategyFromTheNet(strategy);
            connections.clear();
            if (newConnections != null) {
                connections.addAll(newConnections);
            }
        }

//        /**
//         * Is port the one that is described by this information?
//         *
//         * @param info Port information
//         * @return Answer
//         */
//        public boolean matches(FrameworkElementInfo info) {
//            synchronized (getPort()) {
//                if (remoteHandle != info.getHandle() || info.getLinkCount() != getPort().getLinkCount()) {
//                    return false;
//                }
//                if ((getPort().getAllFlags() & Flag.CONSTANT_FLAGS) != (info.getFlags() & Flag.CONSTANT_FLAGS)) {
//                    return false;
//                }
//                for (int i = 0; i < info.getLinkCount(); i++) {
//                    if (peerImplementation.structureExchange == FrameworkElementInfo.StructureExchange.SHARED_PORTS) {
//                        getPort().getQualifiedLink(tmpMatchBuffer, i);
//                    } else {
//                        tmpMatchBuffer.delete(0, tmpMatchBuffer.length());
//                        tmpMatchBuffer.append(getPort().getLink(i).getName());
//                    }
//                    if (!tmpMatchBuffer.equals(info.getLink(i).name)) {
//                        return false;
//                    }
//                    // parents are negligible if everything else, matches
//                }
//                return true;
//            }
//        }

//        public void reset() {
//            connection = null; // set connection to null
//            monitored = false; // reset monitored flag
//            refound = false; // reset refound flag
//            propagateStrategyFromTheNet((short)0);
//            subscriptionRevPush = false;
//            subscriptionUpdateTime = -1;
//            subscriptionStrategy = -1;
//        }

//        /**
//         * Update port properties/information from received port information
//         *
//         * @param portInfo Port info
//         */
//        private void updateFromPortInfo(FrameworkElementInfo portInfo, TCP.OpCode opCode) {
//            synchronized (getPort().getRegistryLock()) {
//                updateFlags(portInfo.getFlags());
//                getPort().setMinNetUpdateInterval(portInfo.getMinNetUpdateInterval());
//                updateIntervalPartner = portInfo.getMinNetUpdateInterval(); // TODO redundant?
//                propagateStrategyFromTheNet(portInfo.getStrategy());
//                portInfo.getConnections(connections);
//
//                log(LogLevel.LL_DEBUG_VERBOSE_2, logDomain, "Updating port info: " + portInfo.toString());
//                if (opCode == TCP.OpCode.STRUCTURE_CREATE) {
//                    assert(!getPort().isReady());
//                    for (int i = 1, n = portInfo.getLinkCount(); i < n; i++) {
//                        FrameworkElement parent = portInfo.getLink(i).unique ? getGlobalLinkElement() : (FrameworkElement)RemotePart.this;
//                        getPort().link(parent, portInfo.getLink(i).name);
//                    }
//                    FrameworkElement parent = portInfo.getLink(0).unique ? getGlobalLinkElement() : (FrameworkElement)RemotePart.this;
//                    getPort().setName(portInfo.getLink(0).name);
//                    parent.addChild(getPort());
//                }
//                FrameworkElementTags.addTags(getPort(), portInfo.getTags());
//
//                checkSubscription();
//            }
//        }

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
        protected void connectionAdded() {
            checkSubscription();
        }

        @Override
        protected void propagateStrategyOverTheNet() {
            checkSubscription();
        }

        @Override
        protected void checkSubscription() {
            if (FinrocTypeInfo.isMethodType(getPort().getDataType(), true)) {
                return;
            }

            synchronized (getPort().getRegistryLock()) {
                AbstractPort p = getPort();
                boolean revPush = p.isInputPort() && p.isConnectedToReversePushSources();
                short time = getUpdateIntervalForNet();
                short strategy = p.isInputPort() ? 0 : p.getStrategy();
                if (!p.isConnected()) {
                    strategy = -1;
                }

                TCPConnection c = connection;

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
                setMonitored(publishPortDataOverTheNet() && getPort().isConnected());
            }
        }

        @Override
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

        @Override
        protected void postChildInit() {
            this.connection = (getPort().getFlag(FrameworkElementFlags.EXPRESS_PORT) ||
                               (getPort().getDataType().getJavaClass() != null && CCType.class.isAssignableFrom(getPort().getDataType().getJavaClass()))) ? expressConnection : bulkConnection;
            super.postChildInit();
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
    private static PortCreationInfo createPCI(FrameworkElementInfo portInfo) {
        PortCreationInfo pci = new PortCreationInfo(portInfo.getFlags());
        pci.flags = portInfo.getFlags();

        // set queue size
        pci.maxQueueSize = portInfo.getStrategy();

        pci.dataType = portInfo.getDataType();
        pci.lockOrder = LockOrderLevels.REMOTE_PORT;

        return pci;
    }

    /**
     * Pull call storage and management
     */
    class PullCall extends SerializedTCPCommand {

        PullCall(NetPort netport) {
            super(TCP.OpCode.PULLCALL, 16);
            timeSent = System.currentTimeMillis();
            callId = nextCallId.incrementAndGet();
            encoding = netport.getEncoding();
            getWriteStream().writeInt(netport.getRemoteHandle());
            getWriteStream().writeLong(callId);
            getWriteStream().writeEnum(encoding);
        }

        /** Time when call was created/sent */
        final long timeSent;

        /** Call id of pull call */
        final long callId;

        /** Port call originates from - in case of a standard port */
        PortBase origin;

        /** Buffer with result */
        PortDataManager resultBuffer;

        /** Result buffer for CC port */
        CCPortDataManagerTL ccResultBuffer;

        /** Was CC Pull successful? */
        boolean ccPullSuccess;

        /** Data encoding to use */
        Serialization.DataEncoding encoding;
    }
}
