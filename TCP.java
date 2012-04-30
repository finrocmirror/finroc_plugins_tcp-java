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
import org.finroc.core.FrameworkElementTreeFilter;
import org.finroc.core.parameter.ConstructorParameters;
import org.finroc.core.parameter.StaticParameterList;
import org.finroc.core.plugin.CreateExternalConnectionAction;
import org.finroc.core.plugin.ExternalConnection;
import org.finroc.core.plugin.Plugin;
import org.finroc.core.plugin.Plugins;
import org.rrlib.finroc_core_utils.jc.AutoDeleter;
import org.rrlib.finroc_core_utils.jc.HasDestructor;
import org.rrlib.finroc_core_utils.jc.annotation.AtFront;
import org.rrlib.finroc_core_utils.jc.annotation.ForwardDecl;
import org.rrlib.finroc_core_utils.jc.annotation.InCppFile;
import org.rrlib.finroc_core_utils.jc.annotation.PassByValue;
import org.rrlib.finroc_core_utils.jc.annotation.Ptr;
import org.rrlib.finroc_core_utils.jc.container.ReusablesPoolCR;

/**
 * @author max
 *
 * Plugin for P2P TCP connections
 */
@ForwardDecl(TCPPeer.class)
public class TCP implements Plugin, HasDestructor {

    /** Singleton instance of TCP plugin */
    static TCP instance;

    public TCP() {
        instance = this;
    }

    /** Stream IDs for different connection types */
    public static final byte TCP_P2P_ID_EXPRESS = 9, TCP_P2P_ID_BULK = 10;

    /**
     * Protocol OpCodes
     *
     * TODO: protocol could be optimized/simplified
     *
     * payload is   [int8: data encoding]
     *            n*[bool8 another buffer? == true][int16: data type uid][int8: encoding][binary blob or null-terminated string depending on type encoding]
     *              [bool8 another buffer? == false]
     *
     * parameter is [int8: parameter type? == NULL_PARAM]
     *              [int8: parameter type? == NUMBER][serialized tCoreNumber]
     *              [int8: parameter_type? == OBJECT][int16: data type][serialized data][bool8: write lockid? if TRUE, followed by int32 lock id]
     */
    public enum OpCode {
        SET,               // Port data set operation             [int32: remote port handle][int32: skip offset][int8: changed flag][payload](skip target)
        SUBSCRIBE,         // Subscribe to data port              [int32: remote port handle][int16: strategy][bool: reverse push][int16: update interval][int32: local port handle][int8: encoding]
        UNSUBSCRIBE,       // Unsubscribe from data port          [int32: remote port handle]
        CHANGE_EVENT,      // Change event from subscription      [int32: remote port handle][int32: skip offset][int8: changed flag][payload](skip target)
        PING,              // Ping to determine round-trip time   [int32: packet index]
        PONG,              // Pong to determine round-trip time   [int32: acknowledged packet index]
        PULLCALL,          // Pull call                           [int32: remote port handle][int32: local port handle][int32: skip offset][int8: status][int8: exception type][int8: syncher ID][int32: thread uid][int16: method call index][bool8 intermediateAssign][int8: desired encoding](skip target)
        PULLCALL_RETURN,   // Returning pull call                 [int32: remote port handle][int32: local port handle][int32: skip offset][int8: status][int8: exception type][int8: syncher ID][int32: thread uid][int16: method call index][bool8 intermediateAssign][int8: desired encoding][int16: data type][serialized data](skip target)
        METHODCALL,        // Method call                         [int32: remote port handle][int32: local port handle][int16: interface type][int32: skip offset][int8: method id][int32: timeout in ms][int8: status][int8: exception type][int8: syncher ID][int32: thread uid][int16: method call index][parameter0][parameter1][parameter2][parameter3](skip target)
        METHODCALL_RETURN, // Returning Method call               [int32: remote port handle][int32: local port handle][int16: interface type][int32: skip offset][int8: method id][int32: timeout in ms][int8: status][int8: exception type][int8: syncher ID][int32: thread uid][int16: method call index][parameter0][parameter1][parameter2][parameter3](skip target)
        UPDATE_TIME,       // Update on desired update times      [int16: data type][int16: new update time]
        STRUCTURE_UPDATE,  // Update on remote framework elements [int8: opcode (e.g. add)][int32: remote handle][int32: flags][bool8: only ports?]... (see serializeFrameworkElement() in FrameworkElementInfo)
        PEER_INFO          // Information about other peers       [int32+int16: IP and port of this runtime in partner's network interface][int32: known peer count n] n*[int32+int16: address of peer[i]]
    }

    /** Return Status */
    public final static byte SUCCESS = 100, FAIL = 101;

    /** Default network name */
    public static final String DEFAULT_CONNECTION_NAME = "localhost:4444";

    /** Pool with Reusable TCP Commands (SUBSCRIBE & UNSUBSCRIBE) */
    @Ptr private static final ReusablesPoolCR<TCPCommand> tcpCommands = AutoDeleter.addStatic(new ReusablesPoolCR<TCPCommand>());

    /** Name of ports only connection */
    public static final String TCP_PORTS_ONLY_NAME = "TCP ports only";

    /** Standard TCP connection creator */
    public static final CreateAction creator1 = new CreateAction(TCPPeer.GUI_FILTER, "TCP", 0);

    /** Alternative TCP connection creator */
    public static final CreateAction creator2 = new CreateAction(TCPPeer.DEFAULT_FILTER, TCP_PORTS_ONLY_NAME, 0);

    /** Complete TCP connection creator */
    public static final CreateAction creator3 = new CreateAction(TCPPeer.ALL_AND_EDGE_FILTER, "TCP admin", CreateExternalConnectionAction.REMOTE_EDGE_INFO);

    /**
     * @return Unused TCP Command
     */
    public static TCPCommand getUnusedTCPCommand() {
        TCPCommand tc = tcpCommands.getUnused();
        if (tc == null) {
            tc = new TCPCommand();
            tcpCommands.attach(tc, false);
        }
        //tc.responsibleThread = ThreadUtil.getCurrentThreadId();
        return tc;
    }

    @Override
    public void init(/*PluginManager mgr*/) {
//        Plugins.getInstance().registerExternalConnection(creator1);
//        Plugins.getInstance().registerExternalConnection(creator2);
//        Plugins.getInstance().registerExternalConnection(creator3);
    }

    @Override
    public void delete() {
        if (tcpCommands != null) {
            tcpCommands.controlledDelete();
        }
    }

    /**
     * Class for TCP create-Actions
     */
    @AtFront @PassByValue
    private static class CreateAction implements CreateExternalConnectionAction {

        /** Filter to used for this connection type */
        private final FrameworkElementTreeFilter filter;

        /** Name of connection type */
        private final String name;

        /** Flags to use */
        private final int flags;

        /** Name of module type */
        private final String group;

        public CreateAction(FrameworkElementTreeFilter filter, String name, int flags) {
            this.filter = filter;
            this.name = name;
            this.flags = flags;
            Plugins.getInstance().registerExternalConnection(this);

            //Cpp group = getBinary((void*)_M_dummy);

            //JavaOnlyBlock
            this.group = Plugins.getInstance().getContainingJarFile(TCP.class);
        }

        /*Cpp
        static void dummy() {}
         */

        @Override
        public FrameworkElement createModule(FrameworkElement parent, String name, ConstructorParameters params) throws Exception {
            FrameworkElement result = createExternalConnection();
            parent.addChild(result);
            return result;
        }

        @Override
        public StaticParameterList getParameterTypes() {
            return null;
        }

        @Override
        public String getModuleGroup() {
            return group;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override @InCppFile
        public ExternalConnection createExternalConnection() throws Exception {
            return new TCPPeer(DEFAULT_CONNECTION_NAME, filter);
        }

        @Override
        public int getFlags() {
            return flags;
        }
    }
}
