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

import org.finroc.jc.GarbageCollector;
import org.finroc.jc.annotation.Ptr;
import org.finroc.core.RuntimeSettings;
import org.finroc.core.port.PortCreationInfo;
import org.finroc.core.port.PortFlags;
import org.finroc.core.port.net.NetPort;
import org.finroc.core.port.rpc.AbstractCall;
import org.finroc.core.port.rpc.MethodCallException;
import org.finroc.core.port.rpc.SynchMethodCallLogic;
import org.finroc.core.portdatabase.FinrocTypeInfo;

/**
 * @author max
 *
 * NetPort for TCP Connections
 */
public abstract class TCPPort extends NetPort {

    /**
     * Connection that TCP Port belongs to - has to be checked for null, before used -
     * is deleted deferred, so using it after checking (without waiting) is safe
     */
    @Ptr protected TCPConnection connection;

    /** Is port currently monitored? */
    protected boolean monitored = false;

    /** Update interval as requested by connection partner - -1 or smaller means no request */
    protected short updateIntervalPartner = -1;

    /**
     * @param pci Port Creation Info
     * @param connection Connection that TCP Port belongs to
     */

    public TCPPort(PortCreationInfo pci, @Ptr TCPConnection connection) {
        super(pci, connection);
        this.connection = connection;
        assert(connection != null);
    }

    @Override
    protected void sendCall(AbstractCall mc) {
        TCPConnection c = connection;
        if (c != null) {

            // we received a method/pull call that we will forward over the net
            //mc.pushCaller(getPort());
            mc.setRemotePortHandle(remoteHandle);
            mc.setLocalPortHandle(getPort().getHandle());
            c.sendCall(mc);
        } else {
            mc.setExceptionStatus(MethodCallException.Type.NO_CONNECTION);
            SynchMethodCallLogic.handleMethodReturn(mc);
            // no connection - throw exception
            //mc.setStatus(AbstractCall.CONNECTION_EXCEPTION);
            //mc.returnToCaller();
        }
    }


    @Override
    public void sendCallReturn(AbstractCall mc) {
        TCPConnection c = connection;
        if (c != null) {

            // we received a method/pull call that we will forward over the net
            //mc.pushCaller(getPort());
            mc.setRemotePortHandle(remoteHandle);
            mc.setLocalPortHandle(getPort().getHandle());
            c.sendCall(mc);
        } else {
            mc.setExceptionStatus(MethodCallException.Type.NO_CONNECTION);
            SynchMethodCallLogic.handleMethodReturn(mc);
            // no connection - throw exception
            //mc.setStatus(AbstractCall.CONNECTION_EXCEPTION);
            //mc.returnToCaller();
        }
    }

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
        return getPort().isOutputPort() && getPort().getFlag(PortFlags.PUSH_STRATEGY_REVERSE);
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
        } else if ((t = FinrocTypeInfo.get(getPort().getDataType()).getUpdateTime()) >= 0) {
            return t;

            // 4. server data type default
        } else if (c != null /*&& connection.updateTimes != null*/ && (t = c.updateTimes.getTime(getPort().getDataType())) >= 0) {
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
        setMonitored(publishPortDataOverTheNet());
    }

    /**
     * Relevant for client ports - called whenever something changes that could have an impact on a server subscription
     */
    protected void checkSubscription() {

    }
}
