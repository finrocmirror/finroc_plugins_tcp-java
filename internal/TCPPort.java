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

import org.rrlib.finroc_core_utils.jc.GarbageCollector;
import org.rrlib.finroc_core_utils.serialization.OutputStreamBuffer;
import org.finroc.core.FrameworkElementFlags;
import org.finroc.core.RuntimeSettings;
import org.finroc.core.datatype.Timestamp;
import org.finroc.core.port.PortCreationInfo;
import org.finroc.core.port.ThreadLocalCache;
import org.finroc.core.port.cc.CCPortDataManager;
import org.finroc.core.port.cc.CCQueueFragmentRaw;
import org.finroc.core.port.net.NetPort;
import org.finroc.core.port.rpc.FutureStatus;
import org.finroc.core.port.rpc.internal.AbstractCall;
import org.finroc.core.port.std.PortDataManager;
import org.finroc.core.port.std.PortDataReference;
import org.finroc.core.port.std.PortQueueFragmentRaw;
import org.finroc.core.portdatabase.FinrocTypeInfo;
import org.finroc.plugins.tcp.TCPSettings;

/**
 * @author Max Reichardt
 *
 * NetPort for TCP Connections
 */
abstract class TCPPort extends NetPort {

    /**
     * Connection that TCP Port belongs to - has to be checked for null, before used -
     * is deleted deferred, so using it after checking (without waiting) is safe
     */
    protected TCPConnection connection;

    /** Is port currently monitored? */
    protected boolean monitored = false;

    /** Update interval as requested by connection partner - -1 or smaller means no request */
    protected short updateIntervalPartner = -1;

    /**
     * @param pci Port Creation Info
     * @param connection Connection that TCP Port belongs to
     */

    public TCPPort(PortCreationInfo pci, TCPConnection connection) {
        super(pci, connection);
        this.connection = connection;
    }

    @Override
    protected void sendCall(AbstractCall mc) {
        TCPConnection c = connection;
        if (c != null) {

            // we received a method/pull call that we will forward over the net
            //mc.pushCaller(getPort());
            mc.setRemotePortHandle(remoteHandle);
            //mc.setLocalPortHandle(getPort().getHandle());
            c.sendCall(mc);
        } else {
            mc.setException(FutureStatus.NO_CONNECTION);
            //SynchMethodCallLogic.handleMethodReturn(mc);
            // no connection - throw exception
            //mc.setStatus(AbstractCall.CONNECTION_EXCEPTION);
            //mc.returnToCaller();
        }
    }


//    @Override
//    public void sendCallReturn(AbstractCall mc) {
//        TCPConnection c = connection;
//        if (c != null) {
//
//            // we received a method/pull call that we will forward over the net
//            //mc.pushCaller(getPort());
//            mc.setRemotePortHandle(remoteHandle);
//            mc.setLocalPortHandle(getPort().getHandle());
//            c.sendCall(mc);
//        } else {
//            mc.setExceptionStatus(MethodCallException.Type.NO_CONNECTION);
//            SynchMethodCallLogic.handleMethodReturn(mc);
//            // no connection - throw exception
//            //mc.setStatus(AbstractCall.CONNECTION_EXCEPTION);
//            //mc.returnToCaller();
//        }
//    }

    /**
     * @return Publish data of this port over the network when it changes? (regardless of forward or reverse direction)
     */
    public boolean publishPortDataOverTheNet() {
        return publishPortDataOverTheNetForward() || publishPortDataOverTheNetReverse();
    }

    /**
     * @return Publish data of this port over the network in forward direction when it changes?
     */
    private boolean publishPortDataOverTheNetForward() {
        return getPort().isInputPort() && getPort().getStrategy() > 0;
    }

    /**
     * @return Publish data of this port over the network in forward direction when it changes?
     */
    private boolean publishPortDataOverTheNetReverse() {
        return getPort().isOutputPort() && getPort().getFlag(FrameworkElementFlags.PUSH_STRATEGY_REVERSE);
    }

    /**
     * Set whether port is monitored for changes
     *
     * @param monitored2 desired state
     */
    protected void setMonitored(boolean monitored2) {
        TCPConnection c = connection;
        if (c != null) {
            if (monitored2 && !monitored) {
                c.monitoredPorts.add(this, false);
                monitored = true;
                c.notifyWriter();
            } else if (!monitored2 && monitored) {
                c.monitoredPorts.remove(this);
                monitored = false;
            }
        } else {
            monitored = false;
        }
    }

    @Override
    protected void portChanged() {
        TCPConnection c = connection;
        if (monitored && c != null) {
            c.notifyWriter();
        }
    }

    public short getUpdateIntervalForNet() {
        short t = 0;
        TCPConnection c = connection;

        // 1. does destination have any wishes/requirements?
        if ((t = updateIntervalPartner) >= 0) {
            return t;

            // 2. any local suggestions?
        } else if ((t = getPort().getMinNetworkUpdateIntervalForSubscription()) >= 0) {
            return t;

            // 3. data type default
        } else if ((t = FinrocTypeInfo.get(getDataType()).getUpdateTime()) >= 0) {
            return t;

            // 4. server data type default
        } else if (c != null /*&& connection.updateTimes != null*/ && (t = c.remoteTypes.getTime(getDataType())) >= 0) {
            return t;
        }

        // 5. runtime default
        int res = RuntimeSettings.DEFAULT_MINIMUM_NETWORK_UPDATE_TIME.getValue();
        return (short)res;
    }

    @Override
    protected void prepareDelete() {
        setMonitored(false);
        super.prepareDelete();
        connection = null;
        GarbageCollector.deleteDeferred(this);
    }

    @Override
    public void propagateStrategyFromTheNet(short strategy) {
        super.propagateStrategyFromTheNet(strategy);
        setMonitored(publishPortDataOverTheNet() && getPort().isConnected());
    }

    /**
     * Relevant for client ports - called whenever something changes that could have an impact on a server subscription
     */
    protected void checkSubscription() {

    }

    /**
     * Write data to stream
     *
     * @param stream Stream
     * @param changedFlag Current changed flag of port
     */
    public void writeDataToNetwork(OutputStreamBuffer stream, byte changedFlag) {

        boolean useQ = getPort().getFlag(FrameworkElementFlags.USES_QUEUE);
        boolean first = true;

        if (isStdType() || isTransactionType() || isUnknownAdaptableType()) {
            stream.writeEnum(TCP.OpCode.PORT_VALUE_CHANGE);
            stream.writeSkipOffsetPlaceholder();
            stream.writeInt(getRemoteHandle());
            stream.writeEnum(getEncoding());

            StdNetPort pb = (StdNetPort)getPort();
            if (!useQ) {
                PortDataManager pd = pb.getLockedUnsafeRaw(true);
                stream.writeByte(changedFlag);
                pd.getTimestamp().serialize(stream);
                pd.getObject().serialize(stream, getInternalEncoding());
                pd.releaseLock();
            } else {
                PortQueueFragmentRaw fragment = ThreadLocalCache.getFast().tempFragment;
                pb.dequeueAllRaw(fragment);
                PortDataReference pd = null;
                while ((pd = (PortDataReference)fragment.dequeue()) != null) {
                    if (!first) {
                        stream.writeBoolean(true);
                    }
                    first = false;
                    stream.writeByte(changedFlag);
                    pd.getManager().getTimestamp().serialize(stream);
                    pd.getData().serialize(stream, getInternalEncoding());
                    pd.getManager().releaseLock();
                }
            }
        } else if (isCCType()) {
            CCNetPort pb = (CCNetPort)getPort();
            if (!useQ) {
                CCPortDataManager ccitc = pb.getInInterThreadContainer(true);

                boolean writeTime = ccitc.getTimestamp().equals(Timestamp.ZERO);
                stream.writeEnum(writeTime ? TCP.OpCode.SMALL_PORT_VALUE_CHANGE : TCP.OpCode.SMALL_PORT_VALUE_CHANGE_WITHOUT_TIMESTAMP);
                stream.writeSkipOffsetPlaceholder(true);
                stream.writeInt(getRemoteHandle());
                stream.writeEnum(getEncoding());
                stream.writeByte(changedFlag);
                if (writeTime) {
                    ccitc.getTimestamp().serialize(stream);
                }
                ccitc.getObject().serialize(stream, getInternalEncoding());

                ccitc.recycle2();
            } else {

                stream.writeEnum(TCP.OpCode.PORT_VALUE_CHANGE);
                stream.writeSkipOffsetPlaceholder();
                stream.writeInt(getRemoteHandle());
                stream.writeEnum(getEncoding());

                CCQueueFragmentRaw fragment = ThreadLocalCache.getFast().tempCCFragment;
                pb.dequeueAllRaw(fragment);
                CCPortDataManager pd = null;
                while ((pd = fragment.dequeueUnsafe()) != null) {
                    if (!first) {
                        stream.writeBoolean(true);
                    }
                    first = false;
                    stream.writeByte(changedFlag);
                    pd.getTimestamp().serialize(stream);
                    pd.getObject().serialize(stream, getInternalEncoding());
                    pd.recycle2();
                }
            }
        } else { // interface port
            throw new RuntimeException("Method calls are not handled using this mechanism");
        }
        stream.writeBoolean(false); // No more element in queue
        if (TCPSettings.DEBUG_TCP) {
            stream.writeByte(TCP.DEBUG_TCP_NUMBER);
        }

        stream.skipTargetHere();
    }

    @Override
    protected void postChildInit() {
        super.postChildInit();
        assert(connection != null);
    }
}
