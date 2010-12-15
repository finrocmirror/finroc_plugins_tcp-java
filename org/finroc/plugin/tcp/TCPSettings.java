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

import org.finroc.jc.annotation.Const;
import org.finroc.jc.annotation.Ptr;
import org.finroc.core.FrameworkElement;
import org.finroc.core.RuntimeSettings;
import org.finroc.core.datatype.Bounds;
import org.finroc.core.datatype.Constant;
import org.finroc.core.datatype.Unit;
import org.finroc.core.parameter.ParameterNumeric;

/**
 * @author max
 *
 * TCP Settings
 */
public class TCPSettings extends FrameworkElement {

    /** Singleton Instance */
    private static TCPSettings inst = null;

    /** Loop Interval for Connector Thread... currently only 1ms since waiting is done depending on critical ping times instead */
    static final int CONNECTOR_THREAD_LOOP_INTERVAL = 1;

    /** How often will Connector Thread update subscriptions? (in ms) */
    static final int CONNECTOR_THREAD_SUBSCRIPTION_UPDATE_INTERVAL = 2000;

    /** Minimum Update Time for remote Ports */
    static final int MIN_PORTS_UPDATE_INTERVAL = 200;

    /** Size of dequeue queue in TCP Port */
    static final int DEQUEUE_QUEUE_SIZE = 50;

    /** Maximum not acknowledged Packet */
    static final int MAX_NOT_ACKNOWLEDGED_PACKETS = 0x1F; // 32 (2^x for fast modulo)

    /** Packets considered when calculating avergage ping time */
    static final int AVG_PING_PACKETS = 0x7; // 8 (2^x for fast modulo)

    /** Help for debugging: insert checks in data stream => more bandwidth */
    static final boolean DEBUG_TCP = false;

    /** Help for debugging: this number will be inserted after every command when DEBUG_TCP is activated */
    @Const static final int DEBUG_TCP_NUMBER = 0xCAFEBABE;

    // Port settings
    final ParameterNumeric<Integer> maxNotAcknowledgedPacketsExpress =
        new ParameterNumeric<Integer>("Maximum not acknowledged express packets", this, Unit.NO_UNIT, 4, new Bounds(1, 40, true));

    final ParameterNumeric<Integer> maxNotAcknowledgedPacketsBulk =
        new ParameterNumeric<Integer>("Maximum not acknowledged bulk packets", this, Unit.NO_UNIT, 2, new Bounds(1, 40, true));

    final ParameterNumeric<Integer> minUpdateIntervalExpress =
        new ParameterNumeric<Integer>("Minimum Express Update Interval", this, Unit.ms, 25, new Bounds(1, 2000, Constant.NO_MIN_TIME_LIMIT));

    final ParameterNumeric<Integer> minUpdateIntervalBulk =
        new ParameterNumeric<Integer>("Minimum Bulk Update Interval", this, Unit.ms, 50, new Bounds(1, 2000, Constant.NO_MIN_TIME_LIMIT));

    final ParameterNumeric<Integer> criticalPingThreshold =
        new ParameterNumeric<Integer>("Critical Ping Threshold", this, Unit.ms, 1500, new Bounds(50, 20000, Constant.NO_MAX_TIME_LIMIT));

    /** Debug Settings */
    //static final BoolSetting DISPLAY_INCOMING_TCP_SERVER_COMMANDS = inst.add("DISPLAY_INCOMING_TCP_SERVER_COMMANDS", true, true);
    //static final BoolSetting DISPLAY_INCOMING_PORT_UPDATES = inst.add("DISPLAY_INCOMING_TCP_SERVER_COMMANDS", false, true);

    private TCPSettings() {
        super(RuntimeSettings.getInstance(), "TCP Settings");
    }

    static void initInstance() {
        getInstance().init();
    }

    public static @Ptr TCPSettings getInstance() {
        if (inst == null) {
            inst = new TCPSettings();
        }
        return inst;
    }
}
