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

import org.finroc.jc.net.EOFException;
import org.finroc.jc.net.IPAddress;

import java.net.SocketException;

import org.finroc.jc.ArrayWrapper;
import org.finroc.jc.AtomicDoubleInt;
import org.finroc.jc.HasDestructor;
import org.finroc.jc.MutexLockOrder;
import org.finroc.jc.Time;
import org.finroc.jc.annotation.AtFront;
import org.finroc.jc.annotation.Const;
import org.finroc.jc.annotation.Friend;
import org.finroc.jc.annotation.InCpp;
import org.finroc.jc.annotation.InCppFile;
import org.finroc.jc.annotation.PassByValue;
import org.finroc.jc.annotation.Ptr;
import org.finroc.jc.annotation.Ref;
import org.finroc.jc.annotation.SharedPtr;
import org.finroc.jc.annotation.SizeT;
import org.finroc.jc.annotation.WeakPtr;
import org.finroc.jc.container.Reusable;
import org.finroc.jc.container.SafeConcurrentlyIterableList;
import org.finroc.jc.container.WonderQueue;
import org.finroc.jc.net.NetSocket;

import org.finroc.core.LockOrderLevels;
import org.finroc.core.RuntimeSettings;
import org.finroc.core.buffer.CoreOutput;
import org.finroc.core.buffer.CoreInput;
import org.finroc.core.port.AbstractPort;
import org.finroc.core.port.ThreadLocalCache;
import org.finroc.core.port.net.NetPort;
import org.finroc.core.port.net.RemoteTypes;
import org.finroc.core.port.net.UpdateTimeChangeListener;
import org.finroc.core.port.rpc.MethodCall;
import org.finroc.core.port.rpc.MethodCallException;
import org.finroc.core.port.rpc.PullCall;
import org.finroc.core.port.rpc.RPCThreadPool;
import org.finroc.core.portdatabase.DataType;
import org.finroc.core.portdatabase.SerializableReusable;
import org.finroc.core.setting.IntSetting;
import org.finroc.core.thread.CoreLoopThreadBase;

/**
 * @author max
 *
 * Common parts of client and server TCP Connections
 *
 * (writer and listener members need to be initialized by subclass)
 */
@Friend(TCPPort.class)
public abstract class TCPConnection implements UpdateTimeChangeListener {

    /** Network Socket used for accessing remote Server */
    protected NetSocket socket;

    /** Output Stream for sending data to remote Server */
    protected @SharedPtr CoreOutput cos;

    /** Input Stream for receiving data ro remote Server */
    protected @SharedPtr CoreInput cis;

    /** Listener Thread */
    //protected @SharedPtr Reader listener;

    /** Writer Thread */
    protected @WeakPtr Writer writer;

    /** Reader Thread */
    protected @WeakPtr Reader reader;

    /** Timestamp relative to which time is encoded in this stream */
    protected long timeBase;

    /** References to Connection parameters */
    @Const @Ref private IntSetting minUpdateInterval;
    @Const @Ref private IntSetting maxNotAcknowledgedPackets;

    /** Index of last acknowledged sent packet */
    private volatile int lastAcknowledgedPacket = 0;

    /** Index of last acknowledgement request that was received */
    private volatile int lastAckRequestIndex = 0;

    /** Timestamp of when packet n was sent (Index is n % MAX_NOT_ACKNOWLEDGED_PACKETS => efficient and safe implementation (ring queue)) */
    private final long[] sentPacketTime = new long[TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS + 1];

    /** Ping time for last packages (Index is n % AVG_PING_PACKETS => efficient and safe implementation (ring queue)) */
    private final int[] pingTimes = new int[TCPSettings.AVG_PING_PACKETS + 1];

    /** Signal for disconnecting */
    private volatile boolean disconnectSignal = false;

    /** default connection times of connection partner */
    @PassByValue protected final RemoteTypes updateTimes = new RemoteTypes();

    /** Connection type - BULK or EXPRESS */
    protected final byte type;

    /** Ports that are monitored for changes by this connection and should be checked for modifications */
    protected SafeConcurrentlyIterableList<TCPPort> monitoredPorts = new SafeConcurrentlyIterableList<TCPPort>(50, 4);

    /** TCPPeer that this connection belongs to (null if it does not belong to a peer) */
    protected final TCPPeer peer;

    /** Send information about peers to partner port? */
    protected final boolean sendPeerInfoToPartner;

    /** Last version of peer information that was sent to connection partner */
    protected int lastPeerInfoSentRevision = -1;

    /** Rx related: last time RX was retrieved */
    protected long lastRxTimestamp = 0;

    /** Rx related: last time RX was retrieved: how much have we received in total? */
    protected long lastRxPosition = 0;

    /** Needs to be locked after framework elements, but before runtime registry */
    public final MutexLockOrder objMutex = new MutexLockOrder(LockOrderLevels.REMOTE + 1);

    /**
     * @param type Connection type
     * @param peer TCPPeer that this connection belongs to (null if it does not belong to a peer)
     * @param sendPeerInfoToPartner Send information about peers to partner port?
     */
    public TCPConnection(byte type, TCPPeer peer, boolean sendPeerInfoToPartner) {
        this.type = type;
        this.peer = peer;
        this.sendPeerInfoToPartner = sendPeerInfoToPartner;

        // set params
        minUpdateInterval = (type == TCP.TCP_P2P_ID_BULK) ? TCPSettings.minUpdateIntervalBulk : TCPSettings.minUpdateIntervalExpress;
        maxNotAcknowledgedPackets = (type == TCP.TCP_P2P_ID_BULK) ? TCPSettings.maxNotAcknowledgedPacketsBulk : TCPSettings.maxNotAcknowledgedPacketsExpress;

        RuntimeSettings.getInstance().addUpdateTimeChangeListener(this);
        if (peer != null) {
            peer.addConnection(this);
        }
    }

    /*Cpp
    virtual ~TCPConnection() {
        printf("TCPConnection deleted (%s)\n", getConnectionTypeString().getCString());
    }
     */

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
        return (type == TCP.TCP_P2P_ID_BULK ? "Bulk" : "Express");
    }

    /**
     * Close connection
     */
    public synchronized void disconnect() {
        disconnectSignal = true;
        RuntimeSettings.getInstance().removeUpdateTimeChangeListener(this);
        if (peer != null) {
            peer.removeConnection(this);
        }
        notifyWriter(); // stops writer
        //cos = null;
        try {
            socket.close(); // stops reader
        } catch (Exception e) {}

        // join threads for thread safety
        @InCpp("::std::tr1::shared_ptr<Writer> lockedWriter = writer._lock();")
        @SharedPtr Writer lockedWriter = writer;
        if (lockedWriter != null && Thread.currentThread() != lockedWriter) {
            try {
                lockedWriter.join();
                writer = null;
            } catch (InterruptedException e) {
                //JavaOnlyBlock
                e.printStackTrace();

                //Cpp _printf("warning: TCPConnection::disconnect() - Interrupted waiting for writer thread.\n");
            }
        }
        @InCpp("::std::tr1::shared_ptr<Reader> lockedReader = reader._lock();")
        @SharedPtr Reader lockedReader = reader;
        if (lockedReader != null && Thread.currentThread() != lockedReader) {
            try {
                lockedReader.join();
                reader = null;
            } catch (InterruptedException e) {
                //JavaOnlyBlock
                e.printStackTrace();

                //Cpp _printf("warning: TCPConnection::disconnect() - Interrupted waiting for reader thread.\n");
            }
        }
    }

    /**
     * Listens at socket for incoming data
     */
    @AtFront
    public class Reader extends CoreLoopThreadBase {

        public Reader(String description) {
            super(-1, false, false);
            setName(description);
            lockObject(socket);
        }

        @Override
        public void run() {
            initThreadLocalCache();
            // only for c++ automatic deallocation
            @SharedPtr CoreInput cis = TCPConnection.this.cis;

            try {
                while (!disconnectSignal) {

                    // we are waiting for change events and acknowledgement related stuff
                    byte opCode = cis.readByte();
                    //System.out.println("Incoming command - opcode " + opCode);

                    // create vars before switch-statement (because of C++)
                    int ackReqIndex = 0;
                    int index = 0;
                    long curTime = 0;
                    PeerList pl = null;
                    boolean notifyWriters = false;

                    // process acknowledgement stuff and other commands common for server and client
                    switch (opCode) {
                    case TCP.PING:
                        ackReqIndex = cis.readInt();
                        if (ackReqIndex != lastAckRequestIndex + 1) {
                            throw new Exception("Invalid acknowledgement request");
                        }
                        lastAckRequestIndex++; // not atomic, but doesn't matter here - since this is the only thread that writes
                        notifyWriter();
                        break;

                    case TCP.PONG:
                        index = cis.readInt();

                        // set ping times
                        curTime = System.currentTimeMillis();
                        for (int i = lastAcknowledgedPacket + 1; i <= index; i++) {
                            pingTimes[i & TCPSettings.AVG_PING_PACKETS] = (int)(curTime - sentPacketTime[i & TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS]);
                        }

                        lastAcknowledgedPacket = index;
                        notifyWriter();
                        break;

                    case TCP.UPDATETIME:
                        updateTimes.setTime(cis.readShort(), cis.readShort());
                        break;

                    case TCP.PULLCALL:
                        handlePullCall();
                        break;

                    case TCP.PULLCALL_RETURN:
                        handleReturningPullCall();
                        break;

                    case TCP.METHODCALL:
                        handleMethodCall();
                        break;

                    case TCP.METHODCALL_RETURN:
                        handleMethodCallReturn();
                        break;

                    case TCP.PEER_INFO:
                        cis.readSkipOffset();
                        if (peer == null) {
                            cis.toSkipTarget();
                            break;
                        }
                        assert(peer != null);
                        pl = peer.getPeerList();
                        notifyWriters = false;
                        synchronized (pl) {
                            index = pl.getRevision();
                            IPAddress ia = IPAddress.deserialize(cis);
                            peer.getPeerList().deserializeAddresses(cis, ia, socket.getRemoteIPSocketAddress().getAddress());
                            notifyWriters = index != pl.getRevision();
                        }
                        if (notifyWriters) {
                            peer.notifyAllWriters();
                        }
                        break;

                    default:
                        processRequest(opCode);
                        break;
                    }
                    checkCommandEnd();
                }
            } catch (Exception e) {
                //JavaOnlyBlock
                if (e instanceof RuntimeException && e.getCause() != null) {
                    e = (Exception)e.getCause();
                }
                if (!(e instanceof EOFException || e instanceof SocketException || e instanceof java.io.IOException)) {
                    e.printStackTrace();
                }

                /*Cpp
                if (typeid(e) != typeid(finroc::util::EOFException) && typeid(e) != typeid(finroc::util::IOException)) {
                    e.printStackTrace();
                }
                 */
            }
            try {
                handleDisconnect();
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                cis.close();
            } catch (Exception e) {}
        }

        /**
         * Check that command is terminated correctly when TCPSettings.DEBUG_TCP is activated
         */
        private void checkCommandEnd() {
            if (TCPSettings.DEBUG_TCP) {
                int i = cis.readInt();
                if (i != TCPSettings.DEBUG_TCP_NUMBER) {
                    throw new RuntimeException("TCP Stream seems corrupt");
                }
            }
        }

        @Override
        public void mainLoopCallback() throws Exception {}

        public void stopThread() {
            synchronized (TCPConnection.this) {
                disconnectSignal = true;
                socket.shutdownReceive();
            }
        }
    }

    /**
     * Needs to be called after a command has been serialized to the output stream
     */
    public void terminateCommand() {
        if (TCPSettings.DEBUG_TCP) {
            cos.writeInt(TCPSettings.DEBUG_TCP_NUMBER);
        }
    }

    /**
     * Notify (possibly wake-up) writer thread. Should be called whenever new tasks for the writer arrive.
     */
    public void notifyWriter() {
        @InCpp("::std::tr1::shared_ptr<Writer> lockedWriter = writer._lock();")
        @SharedPtr Writer lockedWriter = writer;
        if (lockedWriter != null) {
            lockedWriter.notifyWriter();
        }
    }

    /**
     * Writes outgoing data to socket
     */
    @AtFront @Friend(TCPConnection.class)
    public class Writer extends CoreLoopThreadBase implements HasDestructor {

        ///** Signal that something relevant changed */
        //public volatile boolean changedSignal;

        ///** True, when thread is asleep, because nothing is happening */
        //private volatile boolean waiting;

        /** Index of last packet that was acknowledged */
        private int lastAckIndex = 0;

        /** Index of currently sent packet */
        private volatile int curPacketIndex = 0;

        /** State at this change counter state are/were handled */
        private int handledChangedCounter = -1;

        /**
         * Double int to efficiently handle when to lay writer thread to sleep
         * [1 bit sleeping?][30 bit change counter]
         */
        private AtomicDoubleInt writerSynch = new AtomicDoubleInt(1, 30, 0, 0);

        /** Queue with calls/commands waiting to be sent */
        private final WonderQueue<SerializableReusable> callsToSend = new WonderQueue<SerializableReusable>();

        public Writer(String description) {
            super(-1, true, false);
            setName(description);
            lockObject(socket);
        }

        @Override
        public void delete() {

            // recycle calls
            SerializableReusable call = null;
            while ((call = callsToSend.dequeue()) != null) {
                assert(call.stateChange(Reusable.ENQUEUED, Reusable.POST_QUEUED, TCPConnection.this));
                //call.responsibleThread = ThreadUtil.getCurrentThreadId();
                call.genericRecycle(); // recycle complete call
            }
        }

        @InCppFile
        public boolean canSend() {
            int maxNotAck = maxNotAcknowledgedPackets.get();
            return curPacketIndex < lastAcknowledgedPacket + maxNotAck || (maxNotAck <= 0 && curPacketIndex < lastAcknowledgedPacket + TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS);
        }

        @Override
        public void run() {
            initThreadLocalCache();
            @SharedPtr CoreOutput cos = TCPConnection.this.cos;

            try {

                while (true) {

                    // this is "trap" for writer thread... stays in here as long as nothing has changed
                    do {
                        if (disconnectSignal) {
                            try {
                                handleDisconnect();
                            } catch (Exception e) {
                                e.printStackTrace();
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
                            break;
                        }

                        // okay... seems nothing has changed... set synch variable to sleeping
                        synchronized (this) {
                            try {
                                if (writerSynch.compareAndSet(raw, 1, changeCount)) {
                                    wait(10000); // 10000 to avoid unlucky dead locks while disconnecting
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
                    } while (true);

                    long startTime = System.currentTimeMillis();

                    // send acknowledgements
                    sendAcknowledgementsAndCommands();

                    // send data
                    tc.releaseAllLocks();
                    boolean requestAcknowledgement = sendData(startTime);
                    if (!requestAcknowledgement && lastAcknowledgedPacket == curPacketIndex) {
                        requestAcknowledgement = Time.getCoarse() > sentPacketTime[lastAcknowledgedPacket & TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS] + 1000;
                        /*if (requestAcknowledgement) {
                            System.out.println("requesting ack - because we haven't done that for a long time " + Time.getCoarse());
                        }*/
                    }

                    if (requestAcknowledgement) {
                        cos.writeByte(TCP.PING);
                        curPacketIndex++;
                        cos.writeInt(curPacketIndex);
                        sentPacketTime[curPacketIndex & TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS] = System.currentTimeMillis();
                        terminateCommand();
                    }

                    // send acknowledgements - again
                    sendAcknowledgementsAndCommands();

                    cos.flush();
                    tc.releaseAllLocks();

                    // wait for minimum update time
                    long waitFor = ((long)minUpdateInterval.get()) - (System.currentTimeMillis() - startTime);
                    assert(waitFor <= minUpdateInterval.get());
                    if ((waitFor > 0) && (!disconnectSignal)) {
                        Thread.sleep(waitFor);
                    }
                }
            } catch (Exception e) {

                //JavaOnlyBlock
                if (e instanceof RuntimeException && e.getCause() != null) {
                    e = (Exception)e.getCause();
                }
                if (!(e instanceof SocketException)) {
                    e.printStackTrace();
                }

                //Cpp e.printStackTrace();

                try {
                    handleDisconnect();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }

            try {
                cos.close();
            } catch (Exception e) {}
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
        public void sendAcknowledgementsAndCommands() throws Exception {

            // send receive notification
            if (lastAckIndex < lastAckRequestIndex) {
                cos.writeByte(TCP.PONG);
                lastAckIndex = lastAckRequestIndex;
                cos.writeInt(lastAckIndex);
                terminateCommand();
            }

            // send waiting method calls
            SerializableReusable call = null;
            while ((call = callsToSend.dequeue()) != null) {
                assert(call.stateChange(Reusable.ENQUEUED, Reusable.POST_QUEUED, TCPConnection.this));
                if (call instanceof PullCall) {
                    PullCall pc = (PullCall)call;
                    cos.writeByte(pc.isReturning(true) ? TCP.PULLCALL_RETURN : TCP.PULLCALL);
                    cos.writeInt(pc.getRemotePortHandle());
                    cos.writeInt(pc.getLocalPortHandle());
                    cos.writeSkipOffsetPlaceholder();
                } else if (call instanceof MethodCall) {
                    MethodCall mc = (MethodCall)call;
                    //assert(mc.getMethod() != null); can be null - if type is not known and we want to return call
                    cos.writeByte(mc.isReturning(true) ? TCP.METHODCALL_RETURN : TCP.METHODCALL);
                    cos.writeInt(mc.getRemotePortHandle());
                    cos.writeInt(mc.getLocalPortHandle());
                    cos.writeType(mc.getPortInterfaceType());
                    cos.writeSkipOffsetPlaceholder();
                }
                call.serialize(cos);
                if (call instanceof PullCall || call instanceof MethodCall) {
                    cos.skipTargetHere();
                }
                terminateCommand();
                call.genericRecycle(); // call is sent... we do not need any of its objects anymore
            }

            // send updates on connected peers
            if (sendPeerInfoToPartner && peer != null) {
                int peerInfoRevision = peer.getPeerList().getRevision();
                if (lastPeerInfoSentRevision < peerInfoRevision) {
                    lastPeerInfoSentRevision = peerInfoRevision;
                    cos.writeByte(TCP.PEER_INFO);
                    cos.writeSkipOffsetPlaceholder();
                    socket.getRemoteIPSocketAddress().getAddress().serialize(cos);
                    peer.getPeerList().serializeAddresses(cos);
                    cos.skipTargetHere();
                    terminateCommand();
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
        public void sendCall(SerializableReusable call) {
            //call.responsibleThread = -1;
            assert(call.stateChange((byte)(Reusable.UNKNOWN | Reusable.USED), Reusable.ENQUEUED, TCPConnection.this));
            callsToSend.enqueue(call);
            notifyWriter();
        }
    }

    /**
     * Should be called regularly by monitoring thread to check whether critical
     * ping time threshold is exceeded.
     *
     * @return Time the calling thread may wait before calling again (it futile to call this method before)
     */
    public long checkPingForDisconnect() {
        @InCpp("::std::tr1::shared_ptr<Writer> lockedWriter = writer._lock();")
        @SharedPtr Writer lockedWriter = writer;
        if (lockedWriter == null) {
            return TCPSettings.criticalPingThreshold.get();
        }
        if (lastAcknowledgedPacket != lockedWriter.curPacketIndex) {
            long criticalPacketTime = sentPacketTime[(lastAcknowledgedPacket + 1) & TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS];
            long timeLeft = criticalPacketTime + TCPSettings.criticalPingThreshold.get() - System.currentTimeMillis();
            if (timeLeft < 0) {
                handlePingTimeExceed();
                return TCPSettings.criticalPingThreshold.get();
            }
            return timeLeft;
        } else {
            return TCPSettings.criticalPingThreshold.get();
        }
    }

    /**
     * @return Maximum ping time among last TCPSettings.AVG_PING_PACKETS packets
     */
    public int getMaxPingTime() {
        int result = 0;
        for (@SizeT int i = 0; i < pingTimes.length; i++) {
            result = Math.max(result, pingTimes[i]);
        }
        return result;
    }

    /**
     * @return Average ping time among last TCPSettings.AVG_PING_PACKETS packets
     */
    public int getAvgPingTime() {
        int result = 0;
        for (@SizeT int i = 0; i < pingTimes.length; i++) {
            result += pingTimes[i];
        }
        return result / pingTimes.length;
    }

    /**
     * @return Is critical ping time currently exceeded (possibly temporary disconnect)
     */
    public boolean pingTimeExceeed() {
        @InCpp("::std::tr1::shared_ptr<Writer> lockedWriter = writer._lock();")
        @SharedPtr Writer lockedWriter = writer;
        if (lockedWriter == null) {
            return false;
        }
        if (lastAcknowledgedPacket != lockedWriter.curPacketIndex) {
            long criticalPacketTime = sentPacketTime[(lastAcknowledgedPacket + 1) & TCPSettings.MAX_NOT_ACKNOWLEDGED_PACKETS];
            long timeLeft = criticalPacketTime + TCPSettings.criticalPingThreshold.get() - System.currentTimeMillis();
            return timeLeft < 0;
        } else {
            return false;
        }
    }

    public void writeTimestamp(long timeStamp) throws Exception {
        cos.writeInt((int)(timeStamp - timeBase));
    }

    public long readTimestamp() throws Exception {
        return cis.readInt() + timeBase;
    }


    /**
     * Called when cricical ping time threshold was exceeded
     */
    public abstract void handlePingTimeExceed();

    /**
     * Send data chunk. Is called regularly in writer loop whenever changed flag is set.
     *
     * @param startTime Timestamp when send operation was started
     * @return Is this an packet that needs acknowledgement ?
     */
    public abstract boolean sendData(long startTime) throws Exception;

    /**
     * Common/standard implementation of above
     *
     * @param startTime Timestamp when send operation was started
     * @param opCode OpCode to use for send operations
     * @return Is this an packet that needs acknowledgement ?
     */
    public boolean sendDataPrototype(long startTime, byte opCode) throws Exception {
        boolean requestAcknowledgement = false;

        // send port data
        @Ptr ArrayWrapper<TCPPort> it = monitoredPorts.getIterable();
        byte changedFlag = 0;
        for (@SizeT int i = 0, n = monitoredPorts.size(); i < n; i++) {
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
                    cos.writeByte(opCode);
                    cos.writeInt(pp.getRemoteHandle());
                    cos.writeSkipOffsetPlaceholder();
                    cos.writeByte(changedFlag);
                    pp.writeDataToNetwork(cos, startTime);
                    cos.skipTargetHere();
                    terminateCommand();
                }
            }
        }
        ThreadLocalCache.getFast().releaseAllLocks();
        return requestAcknowledgement;
    }

    /**
     * Send call to connection partner
     *
     * @param call Call object
     */
    public void sendCall(SerializableReusable call) {
        @InCpp("::std::tr1::shared_ptr<Writer> lockedWriter = writer._lock();")
        @SharedPtr Writer lockedWriter = writer;
        if (lockedWriter != null && (!disconnectSignal)) {
            lockedWriter.sendCall(call);
        }
    }

    /**
     * Called when listener or writer is disconnected
     */
    public abstract void handleDisconnect();

    /**
     * Called when listener receives request
     */
    public abstract void processRequest(byte opCode) throws Exception;

    @Override
    public void updateTimeChanged(DataType dt, short newUpdateTime) {
        // forward update time change to connection partner
        TCPCommand tc = TCP.getUnusedTCPCommand();
        tc.opCode = TCP.UPDATETIME;
        tc.datatypeuid = dt == null ? -1 : dt.getUid();
        tc.updateInterval = newUpdateTime;
        sendCall(tc);
    }

    /**
     * Handles returning pull call on server and client side
     */
    protected void handleReturningPullCall() {
        int handle = cis.readInt();
        /*int remoteHandle =*/
        cis.readInt();
        cis.readSkipOffset();
        TCPPort port = lookupPortForCallHandling(handle);

        if (port == null || (!port.getPort().isReady())) {
            // port does not exist anymore - discard call
            cis.toSkipTarget();
            return;
        }

        // make sure, "our" port is not deleted while we use it
        synchronized (port.getPort()) {

            // check ready again...
            if (!port.getPort().isReady()) {
                // port is deleted - discard call
                cis.toSkipTarget();
                return;
            }

            // deserialize pull call
            PullCall pc = ThreadLocalCache.getFast().getUnusedPullCall();
            try {
                cis.setBufferSource(port.getPort());
                pc.deserialize(cis);
                cis.setBufferSource(null);

                // debug output
                if (TCPSettings.DISPLAY_INCOMING_TCP_SERVER_COMMANDS.get()) {
                    System.out.println("Incoming Server Command: Pull return call " + (port != null ? port.getPort().getQualifiedName() : handle) + " status: " + pc.getStatusString());
                }

                // Returning call
                pc.deserializeParamaters();
            } catch (Exception e) {
                e.printStackTrace();
                pc.recycle();
                pc = null;
            }

            if (pc != null) {
                port.handleCallReturnFromNet(pc);
            }
        }
    }

    /**
     * Handles pull calls on server and client side
     */
    protected void handlePullCall() {

        // read port index and retrieve proxy port
//      int remoteHandle = cis.readInt();
        int handle = cis.readInt();
        int remoteHandle = cis.readInt();
        cis.readSkipOffset();
        TCPPort port = lookupPortForCallHandling(handle);

        // create/decode call
        PullCall pc = ThreadLocalCache.getFast().getUnusedPullCall();
        try {
            pc.deserialize(cis);
        } catch (Exception e) {
            pc.recycle();
            return;
        }

        // process call
        if (TCPSettings.DISPLAY_INCOMING_TCP_SERVER_COMMANDS.get()) {
            System.out.println("Incoming Server Command to port '" + (port != null ? port.getPort().getQualifiedName() : handle) + "': " + pc.toString());
        }

        if (port == null || (!port.getPort().isReady())) {
            pc.setExceptionStatus(MethodCallException.Type.NO_CONNECTION);
            pc.setRemotePortHandle(remoteHandle);
            pc.setLocalPortHandle(handle);
            sendCall(pc);
        } else {
            // Execute pull in extra thread, since it can block
//          pc.setRemotePortHandle(remoteHandle);
            pc.prepareForExecution(port);
            RPCThreadPool.getInstance().executeTask(pc);
        }
    }

    /**
     * Handles method calls on server and client side
     */
    protected void handleMethodCall() {

        // read port index and retrieve proxy port
        int handle = cis.readInt();
        int remoteHandle = cis.readInt();
        DataType methodType = cis.readType();
        cis.readSkipOffset();
        TCPPort port = lookupPortForCallHandling(handle);

        if ((port == null || methodType == null || (!methodType.isMethodType()))) {

            // create/decode call
            MethodCall mc = ThreadLocalCache.getFast().getUnusedMethodCall();
            try {
                mc.deserializeCall(cis, methodType, true);
            } catch (Exception e) {
                mc.recycle();
                return;
            }

            mc.setExceptionStatus(MethodCallException.Type.NO_CONNECTION);
            mc.setRemotePortHandle(remoteHandle);
            mc.setLocalPortHandle(handle);
            sendCall(mc);
            cis.toSkipTarget();
        }

        // make sure, "our" port is not deleted while we use it
        synchronized (port.getPort()) {

            boolean skipCall = (!port.getPort().isReady());
            MethodCall mc = ThreadLocalCache.getFast().getUnusedMethodCall();
            cis.setBufferSource(skipCall ? null : port.getPort());
            try {
                mc.deserializeCall(cis, methodType, skipCall);
            } catch (Exception e) {
                cis.setBufferSource(null);
                mc.recycle();
                return;
            }
            cis.setBufferSource(null);

            // process call
            if (TCPSettings.DISPLAY_INCOMING_TCP_SERVER_COMMANDS.get()) {
                System.out.println("Incoming Server Command: Method call " + (port != null ? port.getPort().getQualifiedName() : handle));
            }
            if (skipCall) {
                mc.setExceptionStatus(MethodCallException.Type.NO_CONNECTION);
                mc.setRemotePortHandle(remoteHandle);
                mc.setLocalPortHandle(handle);
                sendCall(mc);
                cis.toSkipTarget();
            } else {
                NetPort.InterfaceNetPortImpl inp = (NetPort.InterfaceNetPortImpl)port.getPort();
                inp.processCallFromNet(mc);
            }
        }
    }

    /**
     * Handles returning method calls on server and client side
     */
    protected void handleMethodCallReturn() {

        // read port index and retrieve proxy port
        int handle = cis.readInt();
        /*int remoteHandle =*/
        cis.readInt();
        DataType methodType = cis.readType();
        cis.readSkipOffset();
        TCPPort port = lookupPortForCallHandling(handle);

        // skip call?
        if (port == null) {
            cis.toSkipTarget();
            return;
        }

        // make sure, "our" port is not deleted while we use it
        synchronized (port.getPort()) {

            if (!port.getPort().isReady()) {
                cis.toSkipTarget();
                return;
            }

            // create/decode call
            MethodCall mc = ThreadLocalCache.getFast().getUnusedMethodCall();
            //boolean skipCall = (methodType == null || (!methodType.isMethodType()));
            cis.setBufferSource(port.getPort());
            try {
                mc.deserializeCall(cis, methodType, false);
            } catch (Exception e) {
                cis.toSkipTarget();
                cis.setBufferSource(null);
                mc.recycle();
                return;
            }
            cis.setBufferSource(null);

            // process call
            if (TCPSettings.DISPLAY_INCOMING_TCP_SERVER_COMMANDS.get()) {
                System.out.println("Incoming Server Command: Method call return " + (port != null ? port.getPort().getQualifiedName() : handle));
            }

            // process call
            port.handleCallReturnFromNet(mc);
        }
    }

    /**
     * When calls are received: Lookup port responsible for handling that call
     *
     * @param portIndex Port index as received from network stream
     */
    protected abstract TCPPort lookupPortForCallHandling(int portIndex);

    /**
     * @return Data rate of bytes read from network (in bytes/s)
     */
    public int getRx() {
        long lastTime = lastRxTimestamp;
        long lastPos = lastRxPosition;
        lastRxTimestamp = Time.getCoarse();
        lastRxPosition = cis.getAbsoluteReadPosition();
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
}
