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
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;

import org.rrlib.finroc_core_utils.jc.ArrayWrapper;
import org.rrlib.finroc_core_utils.jc.AtomicDoubleInt;
import org.rrlib.finroc_core_utils.jc.HasDestructor;
import org.rrlib.finroc_core_utils.jc.MutexLockOrder;
import org.rrlib.finroc_core_utils.jc.Time;
import org.rrlib.finroc_core_utils.jc.container.SafeConcurrentlyIterableList;
import org.rrlib.finroc_core_utils.jc.container.WonderQueue;
import org.rrlib.logging.Log;
import org.rrlib.logging.LogLevel;
import org.rrlib.serialization.BinaryInputStream;
import org.rrlib.serialization.BinaryOutputStream;
import org.rrlib.serialization.InputStreamSource;
import org.rrlib.serialization.MemoryBuffer;
import org.rrlib.serialization.Serialization;
import org.rrlib.serialization.rtti.DataTypeBase;

import org.finroc.core.LockOrderLevels;
import org.finroc.core.RuntimeEnvironment;
import org.finroc.core.RuntimeSettings;
import org.finroc.core.datatype.CoreString;
import org.finroc.core.datatype.FrameworkElementInfo;
import org.finroc.core.parameter.ParameterNumeric;
import org.finroc.core.port.AbstractPort;
import org.finroc.core.port.ThreadLocalCache;
import org.finroc.core.port.net.UpdateTimeChangeListener;
import org.finroc.core.port.rpc.FutureStatus;
import org.finroc.core.port.rpc.internal.AbstractCall;
import org.finroc.core.port.rpc.internal.ResponseSender;
import org.finroc.core.remote.ModelNode;
import org.finroc.core.remote.RemoteTypes;
import org.finroc.core.thread.CoreLoopThreadBase;
import org.finroc.plugins.tcp.TCPSettings;
import org.finroc.plugins.tcp.internal.TCP.OpCode;

/**
 * @author Max Reichardt
 *
 * Common parts of client and server TCP Connections
 *
 * (writer and listener members need to be initialized by subclass)
 */
class TCPConnection implements UpdateTimeChangeListener, ResponseSender {

    /** Network Socket used for accessing remote Server */
    private final Socket socket;

    /** default connection times of connection partner */
    final RemoteTypes remoteTypes = new RemoteTypes();

    /** Socket's output stream */
    private final OutputStream socketOutputStream;

    /** Buffer for writing data to stream */
    private final MemoryBuffer writeBuffer = new MemoryBuffer(MemoryBuffer.DEFAULT_SIZE, MemoryBuffer.DEFAULT_RESIZE_FACTOR, false);

    /** Output Stream for sending data to remote Server */
    private final BinaryOutputStream writeBufferStream = new BinaryOutputStream(writeBuffer, remoteTypes);

    /** Input Stream for receiving data ro remote Server */
    private final BinaryInputStream readBufferStream;

    /** Reference to remote part this connection belongs to */
    private final RemotePart remotePart;

    /** Writer Thread */
    private Writer writer;

    /** Reader Thread */
    private Reader reader;

    /** List with calls that wait for a response */
    final ArrayList<CallAndTimeout> callsAwaitingResponse = new ArrayList<CallAndTimeout>();

    /** References to Connection parameters */
    private ParameterNumeric<Integer> minUpdateInterval;
    private ParameterNumeric<Integer> maxNotAcknowledgedPackets;

    /** Index of last acknowledged sent packet */
    private volatile int lastAcknowledgedPacket = 0;

    /** Index of last acknowledgement request that was received */
    private volatile int lastAckRequestIndex = 0;

    /** Timestamp of when packet n was sent (Index is n % MAX_NOT_ACKNOWLEDGED_PACKETS => efficient and safe implementation (ring queue)) */
    private final long[] sentPacketTime = new long[TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS + 1];

    /** Ping time for last packages (Index is n % AVG_PING_PACKETS => efficient and safe implementation (ring queue)) */
    private final int[] pingTimes = new int[TCPSettings.AVG_PING_PACKETS + 1];

    /** Ping time statistics */
    private volatile int avgPingTime, maxPingTime;

    /** Signal for disconnecting */
    private volatile boolean disconnectSignal = false;

    /** Connection type - BULK or EXPRESS */
    protected final boolean bulk;

    /** Ports that are monitored for changes by this connection and should be checked for modifications */
    protected SafeConcurrentlyIterableList<TCPPort> monitoredPorts = new SafeConcurrentlyIterableList<TCPPort>(50, 4);

    /** TCPPeer that this connection belongs to (null if it does not belong to a peer) */
    protected final TCPPeer peer;

    /** Last version of peer information that was sent to connection partner */
    protected int lastPeerInfoSentRevision = -1;

    /** Rx related: last time RX was retrieved */
    protected long lastRxTimestamp = 0;

    /** Rx related: last time RX was retrieved: how much have we received in total? */
    protected long lastRxPosition = 0;

    /** Needs to be locked after framework elements, but before runtime registry */
    public final MutexLockOrder objMutex = new MutexLockOrder(LockOrderLevels.REMOTE + 1);

    /** Log description for connection */
    protected String description;

    /** Temporary buffer with port information */
    private final FrameworkElementInfo tempFrameworkElementInfo = new FrameworkElementInfo();


    /**
     * Initializes connection
     * If this works, adds this connection to the respective RemotePart
     *
     * @param peer Peer implementation that this connection belongs to
     * @param socket Connected network socket
     * @param connectionFlags Flags for this connection (will be merged with flags that connection partner sends)
     * @param neverForget Is this a connection to a peer that should never be forgotten?
     * @param modelNode Model node that shows status
     */
    public TCPConnection(TCPPeer peer, Socket socket, int connectionFlags, boolean neverForget, ModelNode modelNode) throws Exception {
        this.peer = peer;
        this.socket = socket;
        this.socketOutputStream = socket.getOutputStream();
        this.readBufferStream = new BinaryInputStream(new InputStreamSource(socket.getInputStream()), remoteTypes);

        // Initialize connection
        // Write...
        writeBufferStream.writeString(TCP.GREET_MESSAGE);
        writeBufferStream.writeShort(TCP.PROTOCOL_VERSION);
        writeBufferStream.writeSkipOffsetPlaceholder();
        // ConnectionInitMessage
        peer.thisPeer.uuid.serialize(writeBufferStream);
        writeBufferStream.writeEnum(peer.thisPeer.peerType);
        writeBufferStream.writeString(peer.thisPeer.name);
        writeBufferStream.writeEnum(peer.structureExchange);
        writeBufferStream.writeInt((connectionFlags & TCP.BULK_DATA) != 0 ? TCP.BULK_DATA : (TCP.EXPRESS_DATA | TCP.MANAGEMENT_DATA));
        TCP.serializeInetAddress(writeBufferStream, socket.getInetAddress());
        if (TCPSettings.DEBUG_TCP) {
            writeBufferStream.writeByte(TCP.DEBUG_TCP_NUMBER);
        }
        writeBufferStream.skipTargetHere();
        writeBufferToNetwork();

        // Read...
        byte[] greetBuffer = new byte[TCP.GREET_MESSAGE.length()]; // We read to raw byte array here - to avoid reading a huge string with some unknown protocol
        readBufferStream.readFully(greetBuffer);
        readBufferStream.readByte(); // String terminator
        if (!new String(greetBuffer).equals(TCP.GREET_MESSAGE)) {
            Log.log(LogLevel.WARNING, this, "Connection partner does not speak Finroc protocol");
            throw new ConnectException("Partner does not speak Finroc protocol");
        }
        if (readBufferStream.readShort() != TCP.PROTOCOL_VERSION) {
            Log.log(LogLevel.WARNING, this, "Connection partner has wrong protocol version");
            throw new ConnectException("Partner has wrong protocol version");
        }
        readBufferStream.readInt(); // skip offset
        UUID uuid = new UUID();
        uuid.deserialize(readBufferStream);
        TCP.PeerType peerType = readBufferStream.readEnum(TCP.PeerType.class);
        String peerName = readBufferStream.readString();
        FrameworkElementInfo.StructureExchange structureExchange = readBufferStream.readEnum(FrameworkElementInfo.StructureExchange.class); // TODO: send structure
        connectionFlags |= readBufferStream.readInt();
        this.bulk = (connectionFlags & TCP.BULK_DATA) != 0;
        InetAddress myAddress = TCP.deserializeInetAddress(readBufferStream);
        checkCommandEnd(readBufferStream);

        // Adjust peer management information
        synchronized (peer.connectTo) {
            peer.addOwnAddress(myAddress);
            remotePart = peer.getRemotePart(uuid, peerType, peerName, socket.getInetAddress(), neverForget);
            remotePart.peerInfo.connecting = true;
            if (!remotePart.addConnection(this)) {
                throw new ConnectException("Connection already established. Closing.");
            }
            if (peer.thisPeer.peerType != TCP.PeerType.CLIENT_ONLY) {
                remotePart.setDesiredStructureInfo(structureExchange);
            }
        }

        // set params
        minUpdateInterval = bulk ? TCPSettings.getInstance().minUpdateIntervalBulk : TCPSettings.getInstance().minUpdateIntervalExpress;
        maxNotAcknowledgedPackets = bulk ? TCPSettings.getInstance().maxNotAcknowledgedPacketsBulk : TCPSettings.getInstance().maxNotAcknowledgedPacketsExpress;

        // Init connection
        readBufferStream.setTimeout(-1);
        String typeString = bulk ? "Bulk" : "Express";

        // Initialize them here, so that subscribe calls from structure creation do not get lost
        reader = new Reader("TCP Client " + typeString + "-Listener for " + uuid.toString());
        writer = new Writer("TCP Client " + typeString + "-Writer for " + uuid.toString());

        // Get framework elements from connection partner
        if ((connectionFlags & TCP.MANAGEMENT_DATA) != 0) {
            peer.connectionElement.getModelHandler().changeNodeName(modelNode, "Connecting to " + remotePart.peerInfo.toString() + "...");
            //ModelNode statusNode = new ModelNode("Obtaining structure information...");
            //peer.connectionElement.getModelHandler().addNode(modelNode, statusNode);

            // retrieveRemotePorts(cis, cos, updateTimes, newServer);
            boolean newServer = true; /*(serverCreationTime < 0) || (serverCreationTime != timeBase);*/
            //log(LogLevel.LL_DEBUG, this, (newServer ? "Connecting" : "Reconnecting") + " to server " + uuid.toString() + "...");

            // Delete any elements from previous connections
            remotePart.deleteAllChildren();
            remotePart.createNewModel();

            /*portIterator.reset();
            for (ProxyPort pp = portIterator.next(); pp != null; pp = portIterator.next()) {
                pp.managedDelete(); // Delete all ports if we are talking to a new server
            }
            elemIterator.reset();
            for (ProxyFrameworkElement pp = elemIterator.next(); pp != null; pp = elemIterator.next()) {
                pp.managedDelete();
            }*/
            ThreadLocalCache.get(); // we need thread local cache for structure creation (port queue containers)

            try {
                // Process structure packets
                int structurePacketSize = readBufferStream.readInt();
                boolean readType = peer.structureExchange == FrameworkElementInfo.StructureExchange.SHARED_PORTS;
                MemoryBuffer structurePacketBuffer = new MemoryBuffer(structurePacketSize);
                BinaryInputStream structurePacketReadStream = new BinaryInputStream(structurePacketBuffer, remoteTypes);
                while (structurePacketSize != 0) {
                    structurePacketBuffer.setSize(structurePacketSize);
                    readBufferStream.readFully(structurePacketBuffer.getBuffer(), 0, structurePacketSize);
                    structurePacketReadStream.reset();
                    if (readType) {
                        DataTypeBase type = structurePacketReadStream.readType();
                        if (type == null || type != CoreString.TYPE) {
                            Log.log(LogLevel.WARNING, this, "Type encoding does not seem to work");
                            throw new ConnectException("Type encoding does not seem to work");
                        }
                        readType = false;
                    }
                    while (structurePacketReadStream.moreDataAvailable()) {
                        tempFrameworkElementInfo.deserialize(structurePacketReadStream, peer.structureExchange);
                        remotePart.addRemoteStructure(tempFrameworkElementInfo, true);
                    }
                    structurePacketSize = readBufferStream.readInt();
                }
                // remotePart.initAndCheckForAdminPort(modelNode); do this later: after bulk connection has been initialized
            } catch (Exception e) {
                Log.log(LogLevel.DEBUG_WARNING, this, uuid.toString(), e);
                //peer.connectionElement.getModelHandler().removeNode(statusNode);
                remotePart.removeConnection(this);
                throw e;
            }

            // TODO: we currently do not send any structure to connection partner
            writeBufferStream.writeInt(0);
            writeBufferToNetwork();
        }
        //RuntimeSettings.getInstance().addUpdateTimeChangeListener(this);

        // start threads
        reader.start();
        writer.start();
    }

    /**
     * @return Is TCP connection disconnecting?
     */
    public boolean disconnecting() {
        return disconnectSignal;
    }

    /**
     * @return Type of connection ("Bulk" oder "Express")
     */
    public String getConnectionTypeString() {
        return bulk ? "Bulk" : "Express";
    }

    /**
     * Close connection
     */
    public synchronized void disconnect() {
        disconnectSignal = true;
        RuntimeSettings.getInstance().removeUpdateTimeChangeListener(this);
        synchronized (peer.connectTo) {
            remotePart.removeConnection(this);
        }
        notifyWriter(); // stops writer
        //cos = null;
        try {
            socket.close(); // stops reader
        } catch (Exception e) {}

        // join threads for thread safety
        Writer lockedWriter = writer;
        if (lockedWriter != null && Thread.currentThread() != lockedWriter) {
            try {
                lockedWriter.join();
                writer = null;
            } catch (InterruptedException e) {
                Log.log(LogLevel.WARNING, this, "warning: TCPConnection::disconnect() - Interrupted waiting for writer thread.");
            }
        }
        Reader lockedReader = reader;
        if (lockedReader != null && Thread.currentThread() != lockedReader) {
            try {
                lockedReader.join();
                reader = null;
            } catch (InterruptedException e) {
                Log.log(LogLevel.WARNING, this, "warning: TCPConnection::disconnect() - Interrupted waiting for reader thread.");
            }
        }
    }

    /**
     * Check that command is terminated correctly when TCPSettings.DEBUG_TCP is activated
     */
    static void checkCommandEnd(BinaryInputStream stream) {
        if (TCPSettings.DEBUG_TCP) {
            int i = stream.readByte();
            if (i != TCP.DEBUG_TCP_NUMBER) {
                throw new RuntimeException("TCP Stream seems corrupt");
            }
        }
    }

    /**
     * Listens at socket for incoming data
     */
    public class Reader extends CoreLoopThreadBase {

        public Reader(String description) {
            super(-1, false, false);
            setName(description);
            lockObject(socket);
        }

        @Override
        public void run() {
            initThreadLocalCache();
            BinaryInputStream stream = TCPConnection.this.readBufferStream;
            MemoryBuffer structureBuffer = new MemoryBuffer(); // structure changes are copied to this buffer and processed by model (thread)
            BinaryOutputStream structureBufferWriter = new BinaryOutputStream(structureBuffer);
            byte[] tempBuffer = new byte[2048];

            try {
                while (!disconnectSignal) {
                    long packetSize = stream.readInt();
                    long nextPacketStart = stream.getAbsoluteReadPosition() + packetSize;

                    // Process message batch

                    // packet acknowledgement request
                    short ackRequestIndex = stream.readShort();
                    if (ackRequestIndex >= 0) {
                        lastAckRequestIndex = ackRequestIndex;
                        notifyWriter();
                    }

                    // packet acknowledgements
                    short acknowledgement = stream.readShort();
                    if (acknowledgement >= 0) {
                        long curTime = System.currentTimeMillis();
                        while (lastAcknowledgedPacket != acknowledgement) {
                            lastAcknowledgedPacket = (lastAcknowledgedPacket + 1) & 0x7FFF;
                            pingTimes[lastAcknowledgedPacket & TCPSettings.AVG_PING_PACKETS] =
                                (int)(curTime - sentPacketTime[lastAcknowledgedPacket & TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS]);
                        }
                        updatePingStatistics();
                    }

                    while (stream.getAbsoluteReadPosition() < nextPacketStart) {
                        TCP.OpCode opCode = stream.readEnum(TCP.OpCode.class);
                        if (opCode.ordinal() >= TCP.OpCode.OTHER.ordinal()) {
                            Log.log(LogLevel.WARNING, this, "Received corrupted TCP message batch. Invalid opcode. Skipping.");
                            stream.skip((int)(nextPacketStart - stream.getAbsoluteReadPosition()));
                            break;
                        }
                        TCP.MessageSize messageSizeEncoding = TCP.MESSAGE_SIZES[opCode.ordinal()];
                        long messageSize = messageSizeEncoding == TCP.MessageSize.FIXED ? -1 :
                                           (messageSizeEncoding == TCP.MessageSize.VARIABLE_UP_TO_255_BYTE ? (stream.readByte() & 0xFF) : stream.readInt());
                        //long messageEncodingSize = messageSizeEncoding.ordinal() * messageSizeEncoding.ordinal(); // :-)
                        long commandStartPosition = stream.getAbsoluteReadPosition();
                        if (messageSize == 0 || (stream.getAbsoluteReadPosition() + messageSize > nextPacketStart)) {
                            Log.log(LogLevel.WARNING, this, "Received corrupted TCP message batch. Invalid message size: " + messageSize + ". Skipping.");
                            stream.skip((int)(nextPacketStart - stream.getAbsoluteReadPosition()));
                            break;
                        }
                        long nextCommandStartPosition = commandStartPosition + messageSize;

                        // Copy to structure buffer?
                        if (opCode == OpCode.STRUCTURE_CREATE || opCode == OpCode.STRUCTURE_CHANGE || opCode == OpCode.STRUCTURE_DELETE) {
                            structureBufferWriter.writeEnum(opCode);
                            long remaining = (opCode == OpCode.STRUCTURE_DELETE) ? (TCPSettings.DEBUG_TCP ? 5 : 4) : messageSize;
                            while (remaining > 0) {
                                long copy = Math.min(tempBuffer.length, remaining);
                                stream.readFully(tempBuffer, 0, (int)copy);
                                structureBufferWriter.write(tempBuffer, 0, (int)copy);
                                remaining -= copy;
                            }
                        } else {
                            try {
                                remotePart.processMessage(opCode, stream, remoteTypes, TCPConnection.this);
                                checkCommandEnd(stream);
                            } catch (Exception e) {
                                Log.log(LogLevel.WARNING, this, "Failed to deserialize message of type " + opCode.toString() + ". Skipping. Reason: ", e);
                                long skip = nextCommandStartPosition - stream.getAbsoluteReadPosition();
                                if (skip >= 0) {
                                    stream.skip((int)skip);
                                } else {
                                    skip = nextPacketStart - stream.getAbsoluteReadPosition();
                                    if (skip >= 0) {
                                        Log.log(LogLevel.WARNING, this, "Too much stream content was read. This is not handled yet. Moving to next message batch."); // TODO
                                    } else {
                                        Log.log(LogLevel.WARNING, this, "Too much stream content was read - exceeding message batch. This is not handled yet. Disconnecting."); // TODO
                                    }
                                }
                            }
                        }
                    }

                    structureBufferWriter.flush();
                    if (structureBuffer.getSize() > 0) {
                        final MemoryBuffer structureBufferToProcess = structureBuffer;
                        structureBuffer = new MemoryBuffer(16384);
                        structureBufferWriter.reset(structureBuffer);

                        peer.connectionElement.getModelHandler().updateModel(new Runnable() {
                            @Override
                            public void run() {
                                remotePart.processStructurePacket(structureBufferToProcess, remoteTypes);
                            }
                        });
                    }
                }
            } catch (Exception e) {
                if (e instanceof RuntimeException && e.getCause() != null) {
                    e = (Exception)e.getCause();
                }
                if (!(e instanceof SocketException || e instanceof IOException)) {
                    Log.log(LogLevel.DEBUG_WARNING, this, e);
                }
            }
            try {
                remotePart.disconnect();
            } catch (Exception e) {
                Log.log(LogLevel.WARNING, this, e);
            }

            try {
                stream.close();
            } catch (Exception e) {}
        }

        @Override
        public void mainLoopCallback() throws Exception {}

        public void stopThread() {
            synchronized (TCPConnection.this) {
                disconnectSignal = true;
                try {
                    socket.shutdownInput();
                } catch (SocketException e) {
                    // do nothing... can happen if socket is close twice
                } catch (Exception e) {
                    Log.log(LogLevel.ERROR, "NetSocket", e);
                }
            }
        }
    }

//    /**
//     * Needs to be called after a command has been serialized to the output stream
//     */
//    public void terminateCommand() {
//        if (TCPSettings.DEBUG_TCP) {
//            cos.writeInt(TCPSettings.DEBUG_TCP_NUMBER);
//        }
//    }

    /**
     * Notify (possibly wake-up) writer thread. Should be called whenever new tasks for the writer arrive.
     */
    public void notifyWriter() {
        Writer lockedWriter = writer;
        if (lockedWriter != null) {
            lockedWriter.notifyWriter();
        }
    }

    /** Class to store a pair - call and timeout - in a list */
    static class CallAndTimeout {

        long timeoutTime;
        AbstractCall call;

        public CallAndTimeout(long timeoutTime, AbstractCall call) {
            this.timeoutTime = timeoutTime;
            this.call = call;
        }
    }

    /**
     * Writes outgoing data to socket
     */
    public class Writer extends CoreLoopThreadBase implements HasDestructor {

        /** Index of last packet that was acknowledged */
        private int lastAckIndex = 0; // TODO: congestion control

        /** Index of currently sent packet */
        private volatile int curPacketIndex = 0;

        /** State at this change counter state are/were handled */
        private int handledChangedCounter = -1;

        /** Last time acknowledgement was requested */
        private long lastAcknowledgementRequestTime;

        /**
         * Double int to efficiently handle when to lay writer thread to sleep
         * [1 bit sleeping?][30 bit change counter]
         */
        private AtomicDoubleInt writerSynch = new AtomicDoubleInt(1, 30, 0, 0);

        /** Queue with RPC calls waiting to be sent */
        private final WonderQueue<AbstractCall> callsToSend = new WonderQueue<AbstractCall>();

        /** Queue with TCP commands waiting to be sent */
        private final WonderQueue<SerializedTCPCommand> tcpCallsToSend = new WonderQueue<SerializedTCPCommand>();

        /** List with calls that were not ready for sending yet */
        private final ArrayList<AbstractCall> notReadyCallsToSend = new ArrayList<AbstractCall>();


        public Writer(String description) {
            super(-1, true, false);
            setName(description);
            lockObject(socket);
        }

        @Override
        public void delete() {

            // recycle calls
            AbstractCall call = null;
            while ((call = callsToSend.dequeue()) != null) {
                call.setException(FutureStatus.NO_CONNECTION);
            }

            for (AbstractCall c : notReadyCallsToSend) {
                c.setException(FutureStatus.NO_CONNECTION);
            }
            notReadyCallsToSend.clear();

            synchronized (callsAwaitingResponse) {
                for (CallAndTimeout callAndTimeout : callsAwaitingResponse) {
                    callAndTimeout.call.setException(FutureStatus.NO_CONNECTION);
                }
                callsAwaitingResponse.clear();
            }
        }

        public boolean canSend() {
            int maxNotAck = maxNotAcknowledgedPackets.getValue();
            return curPacketIndex < lastAcknowledgedPacket + maxNotAck || (maxNotAck <= 0 && curPacketIndex < lastAcknowledgedPacket + TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS);
        }

        @Override
        public void run() {
            initThreadLocalCache();
            BinaryOutputStream stream = TCPConnection.this.writeBufferStream;

            try {

                while (true) {

                    // this is "trap" for writer thread... stays in here as long as nothing has changed
                    //do {
                    if (disconnectSignal) {
                        try {
                            remotePart.disconnect();
                        } catch (Exception e) {
                            Log.log(LogLevel.WARNING, this, e);
                        }
                        //cleanShutdown();
                        return;
                    }

                    // has something changed since last iteration? => continue
                    int raw = writerSynch.getRaw();
                    assert(writerSynch.getVal1(raw) == 0);
                    int changeCount = writerSynch.getVal2(raw);
                    if (changeCount != handledChangedCounter) {
                        handledChangedCounter = changeCount;
                    } else {

                        // okay... seems nothing has changed... set synch variable to sleeping
                        synchronized (this) {
                            try {
                                if (writerSynch.compareAndSet(raw, 1, changeCount)) {
                                    doWait(1000);
                                }
                                // if compare and set failed... there was a change => continue
                            } catch (InterruptedException e) {
                                // continue, because something changed
                            }

                            // reset changed flag
                            while (!disconnectSignal) {
                                int raw2 = writerSynch.getRaw();
                                if (writerSynch.compareAndSet(raw2, 0, writerSynch.getVal2(raw2))) {
                                    break;
                                }
                            }
                        }
                    }
                    //} while (true);

                    long startTime = System.currentTimeMillis();
                    stream.reset();

                    writeBufferStream.writeLong(0); // placeholder for skip offset and acknowledgement data

                    // send commands
                    sendCommands(startTime);

                    // send data
                    tc.releaseAllLocks();
//                    boolean requestAcknowledgement = sendData(startTime);

                    boolean requestAcknowledgement = startTime >= (lastAcknowledgementRequestTime + 1000);

                    // send port data
                    ArrayWrapper<TCPPort> it = monitoredPorts.getIterable();
                    byte changedFlag = 0;
                    for (int i = 0, n = it.size(); i < n; i++) {
                        TCPPort pp = it.get(i);
                        if (pp != null && pp.getPort().isReady()) {
                            if (pp.getLastUpdate() + pp.getUpdateIntervalForNet() > startTime) {

                                // value cannot be written in this iteration due to minimal update rate
                                notifyWriter();

                            } else if ((changedFlag = pp.getPort().getChanged()) > AbstractPort.NO_CHANGE) {
                                pp.getPort().resetChanged();
                                requestAcknowledgement = true;
                                pp.setLastUpdate(startTime);

                                // execute/write set command to stream
                                pp.writeDataToNetwork(stream, changedFlag);

//
//                                cos.writeEnum(opCode);
//                                cos.writeInt(pp.getRemoteHandle());
//                                cos.writeSkipOffsetPlaceholder();
//                                cos.writeByte(changedFlag);
//                                pp.writeDataToNetwork(cos, startTime);
//                                cos.skipTargetHere();
//                                terminateCommand();
                            }
                        }
                    }
                    ThreadLocalCache.getFast().releaseAllLocks();

//                    if (!requestAcknowledgement && lastAcknowledgedPacket == curPacketIndex) {
//                        requestAcknowledgement = Time.getCoarse() > sentPacketTime[lastAcknowledgedPacket & TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS] + 1000;
//                        /*if (requestAcknowledgement) {
//                            System.out.println("requesting ack - because we haven't done that for a long time " + Time.getCoarse());
//                        }*/
//                    }
//
//                    if (requestAcknowledgement) {
//                        cos.writeEnum(TCP.OpCode.PING);
//                        curPacketIndex++;
//                        cos.writeInt(curPacketIndex);
//                        sentPacketTime[curPacketIndex & TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS] = System.currentTimeMillis();
//                        terminateCommand();
//                    }

                    // send commands - again
                    sendCommands(startTime);

                    writeBufferStream.flush();
                    writeBuffer.getBuffer().putInt(0, writeBuffer.getSize() - 4);
                    writeBuffer.getBuffer().putShort(4, -1); // TODO
                    writeBuffer.getBuffer().putShort(6, lastAckRequestIndex);

                    if (requestAcknowledgement) {
                        curPacketIndex++;
                        writeBuffer.getBuffer().putShort(4, curPacketIndex & 0x7FFF); // TODO
                        lastAcknowledgementRequestTime = System.currentTimeMillis();
                        sentPacketTime[curPacketIndex & TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS] = lastAcknowledgementRequestTime;
                    }

                    writeBufferToNetwork();

                    tc.releaseAllLocks();

                    // wait for minimum update time
                    long waitFor = ((long)minUpdateInterval.getValue()) - (System.currentTimeMillis() - startTime);
                    assert(waitFor <= minUpdateInterval.getValue());
                    if ((waitFor > 0) && (!disconnectSignal)) {
                        Thread.sleep(waitFor);
                    }
                }
            } catch (Exception e) {

                if (e instanceof RuntimeException && e.getCause() != null) {
                    e = (Exception)e.getCause();
                }
                if (!(e instanceof SocketException)) {
                    Log.log(LogLevel.WARNING, this, e);
                }

                try {
                    remotePart.disconnect();
                } catch (Exception e2) {
                    Log.log(LogLevel.WARNING, this, e2);
                }
            }

            try {
                stream.close();
            } catch (Exception e) {}
        }

        /**
         * Put thread to sleep until something happens
         *
         * @param maxSleepTime Maximum time to sleep
         */
        private void doWait(long maxSleepTime) throws InterruptedException {
            if (notReadyCallsToSend.size() == 0 && callsAwaitingResponse.size() == 0) {
                wait(maxSleepTime);
            } else if (notReadyCallsToSend.size() > 0) {
                long maxIterations = maxSleepTime / TCPSettings.CHECK_NOT_READY_CALLS_INTERVAL;
                for (int i = 0; i < maxIterations; i++) {
                    wait(TCPSettings.CHECK_NOT_READY_CALLS_INTERVAL);
                    for (AbstractCall call : notReadyCallsToSend) {
                        if (call.readyForSending()) {
                            return;
                        }
                    }
                    checkWaitingCallsForTimeout(System.currentTimeMillis());
                }
            } else {
                long maxIterations = maxSleepTime / TCPSettings.CHECK_WAITING_CALLS_FOR_TIMEOUT_INTERVAL;
                for (int i = 0; i < maxIterations; i++) {
                    wait(TCPSettings.CHECK_WAITING_CALLS_FOR_TIMEOUT_INTERVAL);
                    boolean wait = checkWaitingCallsForTimeout(System.currentTimeMillis());
                    if (!wait) {
                        long waitFor = maxSleepTime - ((i + 1) * TCPSettings.CHECK_WAITING_CALLS_FOR_TIMEOUT_INTERVAL);
                        if (waitFor > 0) {
                            wait(waitFor);
                        }
                        return;
                    }
                }
            }
        }

        public synchronized void stopThread() {
            disconnectSignal = true;
            if (writerSynch.getVal1() != 0) { // if thread is waiting... wake up
                notify();
            }
        }

        /**
         * Send answers for any pending acknowledgement requests and commands like subscriptions
         */
        public void sendCommands(long startTime) throws Exception {
            BinaryOutputStream stream = TCPConnection.this.writeBufferStream;

//            // send receive notification
//            if (lastAckIndex < lastAckRequestIndex) {
//                cos.writeEnum(TCP.OpCode.PONG);
//                lastAckIndex = lastAckRequestIndex;
//                cos.writeInt(lastAckIndex);
//                terminateCommand();
//            }

            for (int i = 0; i < notReadyCallsToSend.size(); i++) {
                if (notReadyCallsToSend.get(i).readyForSending()) {
                    sendCallImplementation(notReadyCallsToSend.get(i), startTime);
                    notReadyCallsToSend.remove(i);
                    i--;
                }
            }

            // send waiting method calls
            AbstractCall call = null;
            while ((call = callsToSend.dequeue()) != null) {
                if (!call.readyForSending()) {
                    notReadyCallsToSend.add(call);
                } else {
                    sendCallImplementation(call, startTime);
                }
            }

            SerializedTCPCommand tcpCall = null;
            while ((tcpCall = tcpCallsToSend.dequeue()) != null) {
                tcpCall.serialize(stream);
            }

//            // send updates on connected peers
//            if (sendPeerInfoToPartner && peer != null) {
//                int peerInfoRevision = peer.getPeerList().getRevision();
//                if (lastPeerInfoSentRevision < peerInfoRevision) {
//                    lastPeerInfoSentRevision = peerInfoRevision;
//                    cos.writeEnum(TCP.OpCode.PEER_INFO);
//                    cos.writeSkipOffsetPlaceholder();
//                    socket.getRemoteIPSocketAddress().getAddress().serialize(cos);
//                    peer.getPeerList().serializeAddresses(cos);
//                    cos.skipTargetHere();
//                    terminateCommand();
//                }
//            }
        }

        /**
         * Sends specified RPC call to connection partner
         *
         * @param call Call to send (already checked whether it is ready)
         */
        private void sendCallImplementation(AbstractCall call, long startTime) {
            boolean expectsResponse = call.expectsResponse();
            if (expectsResponse) {
                call.setCallId(remotePart.nextCallId.incrementAndGet());
            }

            writeBufferStream.writeEnum(TCP.OpCode.RPC_CALL);
            writeBufferStream.writeSkipOffsetPlaceholder();
            writeBufferStream.writeInt(call.getRemotePortHandle());
            writeBufferStream.writeEnum(call.getCallType());
            call.serialize(writeBufferStream);
            if (TCPSettings.DEBUG_TCP) {
                writeBufferStream.writeByte(TCP.DEBUG_TCP_NUMBER);
            }
            writeBufferStream.skipTargetHere();

            if (expectsResponse) {
                CallAndTimeout callAndTimeout = new CallAndTimeout(startTime + call.getResponseTimeout(), call);
                synchronized (callsAwaitingResponse) {
                    callsAwaitingResponse.add(callAndTimeout);
                }
            }
        }

        @Override
        public void mainLoopCallback() throws Exception {}

        /**
         * Notify (possibly wake-up) writer thread. Should be called whenever new tasks for the writer arrive.
         */
        public void notifyWriter() {
            while (true) {
                int raw = writerSynch.getRaw();
                int sleeping = writerSynch.getVal1(raw);
                int counter = writerSynch.getVal2(raw) + 1;
                if (sleeping == 0) { // typical case: increment changed counter by one
                    if (writerSynch.compareAndSet(raw, 0, counter)) {
                        return;
                    }
                } else { // writer thread is sleeping...
                    if (canSend()) {
                        synchronized (this) {
                            if (writerSynch.compareAndSet(raw, 0, counter)) {
                                notify();
                                return;
                            }
                        }
                    } else {
                        if (writerSynch.compareAndSet(raw, 1, counter)) { // thread is still sleeping... but increment counter
                            return;
                        }
                    }
                }
            }
        }

        /**
         * Send call to connection partner
         *
         * @param call Call object
         */
        public void sendCall(AbstractCall call) {
            //call.responsibleThread = -1;
            callsToSend.enqueue(call);
            notifyWriter();
        }

        /**
         * Send serialized TCP call to connection partner
         *
         * @param call Call object
         */
        public void sendCall(SerializedTCPCommand call) {
            //call.responsibleThread = -1;
            tcpCallsToSend.enqueue(call);
            notifyWriter();
        }
    }

    /**
     * Checks whether any waiting calls have timed out.
     * Removes any timed out calls from list.
     *
     * @return Are still calls waiting?
     */
    public boolean checkWaitingCallsForTimeout(long timeNow) {
        synchronized (callsAwaitingResponse) {
            for (int i = 0; i < callsAwaitingResponse.size(); i++) {
                if (timeNow > callsAwaitingResponse.get(i).timeoutTime) {
                    callsAwaitingResponse.get(i).call.setException(FutureStatus.TIMEOUT);
                    callsAwaitingResponse.remove(i);
                    i--;
                }
            }
            return callsAwaitingResponse.size() > 0;
        }
    }

    /**
     * Should be called regularly by monitoring thread to check whether critical
     * ping time threshold is exceeded.
     *
     * @return Time the calling thread may wait before calling again (it futile to call this method before)
     */
    public long checkPingForDisconnect() {
        Writer lockedWriter = writer;
        if (lockedWriter == null) {
            return TCPSettings.getInstance().criticalPingThreshold.getValue();
        }
        if (lastAcknowledgedPacket != lockedWriter.curPacketIndex) {
            long criticalPacketTime = sentPacketTime[(lastAcknowledgedPacket + 1) & TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS];
            long timeLeft = criticalPacketTime + TCPSettings.getInstance().criticalPingThreshold.getValue() - System.currentTimeMillis();
            if (timeLeft < 0) {
                handlePingTimeExceed();
                return TCPSettings.getInstance().criticalPingThreshold.getValue();
            }
            return timeLeft;
        } else {
            return TCPSettings.getInstance().criticalPingThreshold.getValue();
        }
    }

    /**
     * Updates ping statistic variables
     */
    private void updatePingStatistics() {
        int result = 0;
        int resultAvg = 0;
        for (int i = 0; i < pingTimes.length; i++) {
            result = Math.max(result, pingTimes[i]);
            resultAvg += pingTimes[i];
        }
        maxPingTime = result;
        avgPingTime = resultAvg / pingTimes.length;
    }

    /**
     * @return Maximum ping time among last TCPSettings.AVG_PING_PACKETS packets
     */
    public int getMaxPingTime() {
        return maxPingTime;
    }

    /**
     * @return Average ping time among last TCPSettings.AVG_PING_PACKETS packets
     */
    public int getAvgPingTime() {
        return avgPingTime;
    }

    /**
     * @return Is critical ping time currently exceeded (possibly temporary disconnect)
     */
    public boolean pingTimeExceeed() {
        Writer lockedWriter = writer;
        if (lockedWriter == null) {
            return false;
        }
        if (lastAcknowledgedPacket != lockedWriter.curPacketIndex) {
            long criticalPacketTime = sentPacketTime[(lastAcknowledgedPacket + 1) & TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS];
            long timeLeft = criticalPacketTime + TCPSettings.getInstance().criticalPingThreshold.getValue() - System.currentTimeMillis();
            return timeLeft < 0;
        } else {
            return false;
        }
    }

    /**
     * Called when critical ping time threshold was exceeded
     */
    public void handlePingTimeExceed() {} // TODO

//    /**
//     * Send data chunk. Is called regularly in writer loop whenever changed flag is set.
//     *
//     * @param startTime Timestamp when send operation was started
//     * @return Is this an packet that needs acknowledgement ?
//     */
//    public abstract boolean sendData(long startTime) throws Exception;

//    /**
//     * Common/standard implementation of above
//     *
//     * @param startTime Timestamp when send operation was started
//     * @param opCode OpCode to use for send operations
//     * @return Is this an packet that needs acknowledgement ?
//     */
//    public boolean sendDataPrototype(long startTime, TCP.OpCode opCode) throws Exception {
//        boolean requestAcknowledgement = false;
//
//        // send port data
//        ArrayWrapper<TCPPort> it = monitoredPorts.getIterable();
//        byte changedFlag = 0;
//        for (int i = 0, n = it.size(); i < n; i++) {
//            TCPPort pp = it.get(i);
//            if (pp != null && pp.getPort().isReady()) {
//                if (pp.getLastUpdate() + pp.getUpdateIntervalForNet() > startTime) {
//
//                    // value cannot be written in this iteration due to minimal update rate
//                    notifyWriter();
//
//                } else if ((changedFlag = pp.getPort().getChanged()) > AbstractPort.NO_CHANGE) {
//                    pp.getPort().resetChanged();
//                    requestAcknowledgement = true;
//                    pp.setLastUpdate(startTime);
//
//                    // execute/write set command to stream
//                    cos.writeEnum(opCode);
//                    cos.writeInt(pp.getRemoteHandle());
//                    cos.writeSkipOffsetPlaceholder();
//                    cos.writeByte(changedFlag);
//                    pp.writeDataToNetwork(cos, startTime);
//                    cos.skipTargetHere();
//                    terminateCommand();
//                }
//            }
//        }
//        ThreadLocalCache.getFast().releaseAllLocks();
//        return requestAcknowledgement;
//    }

    /**
     * Send call to connection partner
     *
     * @param call Call object
     */
    public void sendCall(AbstractCall call) {
        Writer lockedWriter = writer;
        if (lockedWriter != null && (!disconnectSignal)) {
            lockedWriter.sendCall(call);
        } else {
            call.setException(FutureStatus.NO_CONNECTION);
        }
    }

    /**
     * Send serialized TCP call to connection partner
     *
     * @param call Call object
     */
    public void sendCall(SerializedTCPCommand call) {
        Writer lockedWriter = writer;
        if (lockedWriter != null && (!disconnectSignal)) {
            lockedWriter.sendCall(call);
        }
    }

    @Override
    public void updateTimeChanged(DataTypeBase dt, short newUpdateTime) {
        // forward update time change to connection partner
        SerializedTCPCommand command = new SerializedTCPCommand(TCP.OpCode.TYPE_UPDATE, 8);
        command.getWriteStream().writeShort(dt == null ? -1 : dt.getUid());
        command.getWriteStream().writeShort(newUpdateTime);
        sendCall(command);
    }

//    /**
//     * Handles method calls on server and client side
//     */
//    protected void handleMethodCall() {
//
//        // read port index and retrieve proxy port
//        int handle = readBufferStream.readInt();
//        int remoteHandle = readBufferStream.readInt();
//        DataTypeBase methodType = readBufferStream.readType();
//        readBufferStream.readSkipOffset();
//        TCPPort port = lookupPortForCallHandling(handle);
//
//        if ((port == null || methodType == null || (!FinrocTypeInfo.isMethodType(methodType)))) {
//
//            // create/decode call
//            MethodCall mc = ThreadLocalCache.getFast().getUnusedMethodCall();
//            try {
//                mc.deserializeCall(readBufferStream, methodType, true);
//            } catch (Exception e) {
//                mc.recycle();
//                return;
//            }
//
//            if (mc.getStatus() == AbstractCall.Status.SYNCH_CALL) {
//                mc.setExceptionStatus(MethodCallException.Type.NO_CONNECTION);
//                mc.setRemotePortHandle(remoteHandle);
//                mc.setLocalPortHandle(handle);
//                sendCall(mc);
//            }
//            readBufferStream.toSkipTarget();
//        }
//
//        // make sure, "our" port is not deleted while we use it
//        synchronized (port.getPort()) {
//
//            boolean skipCall = (!port.getPort().isReady());
//            MethodCall mc = ThreadLocalCache.getFast().getUnusedMethodCall();
//            readBufferStream.setFactory(skipCall ? null : port.getPort());
//            try {
//                mc.deserializeCall(readBufferStream, methodType, skipCall);
//            } catch (Exception e) {
//                readBufferStream.setFactory(null);
//                mc.recycle();
//                return;
//            }
//            readBufferStream.setFactory(null);
//
//            // process call
//            log(LogLevel.LL_DEBUG_VERBOSE_2, this, "Incoming Server Command: Method call " + (port != null ? port.getPort().getQualifiedName() : handle) + " " + mc.getMethod().getName());
//            if (skipCall) {
//                if (mc.getStatus() == AbstractCall.Status.SYNCH_CALL) {
//                    mc.setExceptionStatus(MethodCallException.Type.NO_CONNECTION);
//                    mc.setRemotePortHandle(remoteHandle);
//                    mc.setLocalPortHandle(handle);
//                    sendCall(mc);
//                }
//                readBufferStream.toSkipTarget();
//            } else {
//                NetPort.InterfaceNetPortImpl inp = (NetPort.InterfaceNetPortImpl)port.getPort();
//                inp.processCallFromNet(mc);
//            }
//        }
//    }
//
//    /**
//     * Handles returning method calls on server and client side
//     */
//    protected void handleMethodCallReturn() {
//
//        // read port index and retrieve proxy port
//        int handle = readBufferStream.readInt();
//        /*int remoteHandle =*/
//        readBufferStream.readInt();
//        DataTypeBase methodType = readBufferStream.readType();
//        readBufferStream.readSkipOffset();
//        TCPPort port = lookupPortForCallHandling(handle);
//
//        // skip call?
//        if (port == null) {
//            readBufferStream.toSkipTarget();
//            return;
//        }
//
//        // make sure, "our" port is not deleted while we use it
//        synchronized (port.getPort()) {
//
//            if (!port.getPort().isReady()) {
//                readBufferStream.toSkipTarget();
//                return;
//            }
//
//            // create/decode call
//            MethodCall mc = ThreadLocalCache.getFast().getUnusedMethodCall();
//            //boolean skipCall = (methodType == null || (!methodType.isMethodType()));
//            readBufferStream.setFactory(port.getPort());
//            try {
//                mc.deserializeCall(readBufferStream, methodType, false);
//            } catch (Exception e) {
//                readBufferStream.toSkipTarget();
//                readBufferStream.setFactory(null);
//                mc.recycle();
//                return;
//            }
//            readBufferStream.setFactory(null);
//
//            // process call
//            log(LogLevel.LL_DEBUG_VERBOSE_2, this, "Incoming Server Command: Method call return " + (port != null ? port.getPort().getQualifiedName() : handle));
//
//            // process call
//            port.handleCallReturnFromNet(mc);
//        }
//    }

    /**
     * @return Data rate of bytes read from network (in bytes/s)
     */
    public int getRx() {
        long lastTime = lastRxTimestamp;
        long lastPos = lastRxPosition;
        lastRxTimestamp = Time.getCoarse();
        lastRxPosition = readBufferStream.getAbsoluteReadPosition();
        if (lastTime == 0) {
            return 0;
        }
        if (lastRxTimestamp == lastTime) {
            return 0;
        }

        double data = lastRxPosition - lastPos;
        double interval = (lastRxTimestamp - lastTime) / 1000;
        return (int)(data / interval);
    }

    public String toString() {
        return description;
    }

    /**
     * Writes all contents of write buffer to network and empties and resets it
     */
    protected void writeBufferToNetwork() throws IOException {
        writeBufferStream.flush();
        socketOutputStream.write(writeBuffer.getBuffer().getBuffer().array(), 0, writeBuffer.getSize());
        writeBufferStream.reset();
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
        SerializedTCPCommand command = new SerializedTCPCommand(TCP.OpCode.SUBSCRIBE, 16);
        command.getWriteStream().writeInt(index);
        command.getWriteStream().writeShort(strategy);
        command.getWriteStream().writeBoolean(reversePush);
        command.getWriteStream().writeShort(updateInterval);
        command.getWriteStream().writeInt(localIndex);
        command.getWriteStream().writeEnum(enc);
        sendCall(command);
    }

    /**
     * Unsubscribe from port changes on remote server
     *
     * @param index Port index in remote runtime
     */
    public void unsubscribe(int index) {
        SerializedTCPCommand command = new SerializedTCPCommand(TCP.OpCode.UNSUBSCRIBE, 8);
        command.getWriteStream().writeInt(index);
        sendCall(command);
    }

    /**
     * @param portIndex Index of port
     * @return TCPPort for specified index
     */
    protected TCPPort lookupPortForCallHandling(int portIndex) {
        AbstractPort ap = RuntimeEnvironment.getInstance().getPort(portIndex);
        TCPPort p = null;
        if (ap != null) {
            p = (TCPPort)ap.asNetPort();
            assert(p != null);
        }
        return p;
    }

    @Override
    public void sendResponse(AbstractCall responseToSend) {
        sendCall(responseToSend);
    }
}
