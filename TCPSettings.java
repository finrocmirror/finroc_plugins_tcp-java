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

import org.finroc.core.FrameworkElement;
import org.finroc.core.RuntimeSettings;
import org.finroc.core.datatype.Bounds;
import org.finroc.core.datatype.Constant;
import org.finroc.core.datatype.Unit;
import org.finroc.core.parameter.ParameterNumeric;

/**
 * @author Max Reichardt
 *
 * TCP Settings
 */
public class TCPSettings extends FrameworkElement {

    /** Singleton Instance */
    private static TCPSettings inst = null;

    /** Loop Interval for Connector Thread... currently only 1ms since waiting is done depending on critical ping times instead */
    public static final int CONNECTOR_THREAD_LOOP_INTERVAL = 1;

    /** How often will Connector Thread update subscriptions? (in ms) */
    public static final int CONNECTOR_THREAD_SUBSCRIPTION_UPDATE_INTERVAL = 2000;

    /** Minimum Update Time for remote Ports */
    public static final int MIN_PORTS_UPDATE_INTERVAL = 200;

    /** Size of dequeue queue in TCP Port */
    public static final int DEQUEUE_QUEUE_SIZE = 50;

    /** Maximum not acknowledged Packet */
    public static final int MAX_NOT_ACKNOWLEDGED_PACKETS = 0x1F; // 32 (2^x for fast modulo)

    /** Packets considered when calculating avergage ping time */
    public static final int AVG_PING_PACKETS = 0x7; // 8 (2^x for fast modulo)

    /** Help for debugging: insert checks in data stream => more bandwidth */
    public static final boolean DEBUG_TCP = true;

    /** Rate to check calls that are not ready to send yet - in ms*/
    public static final long CHECK_NOT_READY_CALLS_INTERVAL = 10;

    /** Rate to check calls waiting for response for timeout - in ms*/
    public static final long CHECK_WAITING_CALLS_FOR_TIMEOUT_INTERVAL = 50;


    // Port settings
    public final ParameterNumeric<Integer> maxNotAcknowledgedPacketsExpress =
        new ParameterNumeric<Integer>("Maximum not acknowledged express packets", this, 4, new Bounds<Integer>(1, 40, true));

    public final ParameterNumeric<Integer> maxNotAcknowledgedPacketsBulk =
        new ParameterNumeric<Integer>("Maximum not acknowledged bulk packets", this, 2, new Bounds<Integer>(1, 40, true));

    public final ParameterNumeric<Integer> minUpdateIntervalExpress =
        new ParameterNumeric<Integer>("Minimum Express Update Interval", this, 25, new Bounds<Integer>(1, 2000, Constant.NO_MIN_TIME_LIMIT.getValue()), Unit.ms);

    public final ParameterNumeric<Integer> minUpdateIntervalBulk =
        new ParameterNumeric<Integer>("Minimum Bulk Update Interval", this, 50, new Bounds<Integer>(1, 2000, Constant.NO_MIN_TIME_LIMIT.getValue()), Unit.ms);

    public final ParameterNumeric<Integer> criticalPingThreshold =
        new ParameterNumeric<Integer>("Critical Ping Threshold", this, 1500, new Bounds<Integer>(50, 20000, Constant.NO_MAX_TIME_LIMIT.getValue()), Unit.ms);

    /** Debug Settings */
    //static final BoolSetting DISPLAY_INCOMING_TCP_SERVER_COMMANDS = inst.add("DISPLAY_INCOMING_TCP_SERVER_COMMANDS", true, true);
    //static final BoolSetting DISPLAY_INCOMING_PORT_UPDATES = inst.add("DISPLAY_INCOMING_TCP_SERVER_COMMANDS", false, true);

    private TCPSettings() {
        super(RuntimeSettings.getInstance().getParent(), "TCP");
    }

    public static void initInstance() {
        getInstance().init();
    }

    public static TCPSettings getInstance() {
        if (inst == null) {
            inst = new TCPSettings();
        }
        return inst;
    }
}
