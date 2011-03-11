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

import org.finroc.core.FrameworkElement;
import org.finroc.core.FrameworkElementTreeFilter;
import org.finroc.core.parameter.ConstructorParameters;
import org.finroc.core.parameter.StructureParameterList;
import org.finroc.core.plugin.CreateExternalConnectionAction;
import org.finroc.core.plugin.ExternalConnection;
import org.finroc.core.plugin.Plugin;
import org.finroc.core.plugin.Plugins;
import org.finroc.jc.AutoDeleter;
import org.finroc.jc.HasDestructor;
import org.finroc.jc.annotation.AtFront;
import org.finroc.jc.annotation.ForwardDecl;
import org.finroc.jc.annotation.InCppFile;
import org.finroc.jc.annotation.PassByValue;
import org.finroc.jc.annotation.Ptr;
import org.finroc.jc.container.ReusablesPoolCR;

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

    /** Protocol OpCodes */
    public final static byte SET = 1, SUBSCRIBE = 2, UNSUBSCRIBE = 3, CHANGE_EVENT = 4, PING = 5, PONG = 6, PULLCALL = 7,
                                   METHODCALL = 8, UPDATETIME = 9, REQUEST_PORT_UPDATE = 10, PORT_UPDATE = 11,
                                                                PULLCALL_RETURN = 12, METHODCALL_RETURN = 13, PEER_INFO = 14;

    /** Return Status */
    public final static byte SUCCESS = 100, FAIL = 101;

    /** Default network name */
    public static final String DEFAULT_CONNECTION_NAME = "localhost:4444";

    /** Pool with Reusable TCP Commands (SUBSCRIBE & UNSUBSCRIBE) */
    @Ptr private static final ReusablesPoolCR<TCPCommand> tcpCommands = AutoDeleter.addStatic(new ReusablesPoolCR<TCPCommand>());

    /** Standard TCP connection creator */
    public static final CreateAction creator1 = new CreateAction(TCPPeer.GUI_FILTER, "TCP", 0);

    /** Alternative TCP connection creator */
    public static final CreateAction creator2 = new CreateAction(TCPPeer.DEFAULT_FILTER, "TCP ports only", 0);

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
        public StructureParameterList getParameterTypes() {
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
