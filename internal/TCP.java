//
// You received this file as part of Finroc
// A Framework for intelligent robot control
//
// Copyright (C) Finroc GbR (finroc.org)
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
//
//----------------------------------------------------------------------
package org.finroc.plugins.tcp.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.finroc.core.FrameworkElement;
import org.finroc.core.RuntimeEnvironment;
import org.finroc.core.RuntimeSettings;
import org.finroc.core.datatype.FrameworkElementInfo;
import org.finroc.core.parameter.ConstructorParameters;
import org.finroc.core.parameter.StaticParameterList;
import org.finroc.core.plugin.CreateExternalConnectionAction;
import org.finroc.core.plugin.ExternalConnection;
import org.finroc.core.plugin.Plugin;
import org.finroc.core.plugin.Plugins;
import org.finroc.plugins.tcp.Peer;
import org.rrlib.finroc_core_utils.serialization.InputStreamBuffer;
import org.rrlib.finroc_core_utils.serialization.OutputStreamBuffer;

/**
 * @author Max Reichardt
 *
 * Plugin for P2P TCP connections
 */
public class TCP implements Plugin {

    /** Singleton instance of TCP plugin */
    static TCP instance;

    public TCP() {
        instance = this;
    }

    /** Stream IDs for different connection types */
    public static final byte TCP_P2P_ID_EXPRESS = 9, TCP_P2P_ID_BULK = 10;

    /**
     * Protocol OpCodes
     */
    public enum OpCode {

        // Opcodes for management connection
        SUBSCRIBE,         // Subscribe to data port
        UNSUBSCRIBE,       // Unsubscribe from data port
        PULLCALL,          // Pull call
        PULLCALL_RETURN,   // Returning pull call
        RPC_CALL,          // RPC call
        TYPE_UPDATE,       // Update on remote type info (typically desired update time)
        STRUCTURE_CREATE,  // Update on remote framework elements: Element created
        STRUCTURE_CHANGE,  // Update on remote framework elements: Port changed
        STRUCTURE_DELETE,  // Update on remote framework elements: Element deleted
        PEER_INFO,         // Information about other peers

        // Change event opcodes (from subscription - or for plain setting of port)
        PORT_VALUE_CHANGE,                         // normal variant
        SMALL_PORT_VALUE_CHANGE,                   // variant with max. 256 byte message length (3 bytes smaller than normal variant)
        SMALL_PORT_VALUE_CHANGE_WITHOUT_TIMESTAMP, // variant with max. 256 byte message length and no timestamp (11 bytes smaller than normal variant)

        // Used for messages without opcode
        OTHER
    }

    public enum MessageSize {
        FIXED,                   // fixed size message
        VARIABLE_UP_TO_255_BYTE, // variable message size up to 255 bytes
        VARIABLE_UP_TO_4GB       // variable message size up to 4GB
    };

    /** Mode of peer */
    public enum PeerType {
        CLIENT_ONLY,  // Peer is client only
        SERVER_ONLY,  // Peer is server only
        FULL          // Peer is client and server
    }

    /**
     * Flags for connection properties
     */
    public static final int
    MANAGEMENT_DATA = 0x1, //<! This connection is used to transfer management data (e.g. available ports, peer info, subscriptions, rpc calls)
    EXPRESS_DATA    = 0x2, //<! This connection transfers "express data" (port values of express ports) - candidate for UDP in the future
    BULK_DATA       = 0x4; //<! This connection transfers "bulk data" (port values of bulk ports) - candidate for UDP in the future

    /** Message size encodings of different kinds of op codes */
    public static final MessageSize[] MESSAGE_SIZES = new MessageSize[] {
        MessageSize.FIXED,                   // SUBSCRIBE
        MessageSize.FIXED,                   // UNSUBSCRIBE
        MessageSize.FIXED,                   // PULLCALL
        MessageSize.VARIABLE_UP_TO_4GB,      // PULLCALL_RETURN
        MessageSize.VARIABLE_UP_TO_4GB,      // RPC_CALL
        MessageSize.VARIABLE_UP_TO_4GB,      // UPDATE_TIME
        MessageSize.VARIABLE_UP_TO_4GB,      // STRUCTURE_CREATE
        MessageSize.VARIABLE_UP_TO_4GB,      // STRUCTURE_CHANGE
        MessageSize.FIXED,                   // STRUCTURE_DELETE
        MessageSize.VARIABLE_UP_TO_4GB,      // PEER_INFO
        MessageSize.VARIABLE_UP_TO_4GB,      // PORT_VALUE_CHANGE
        MessageSize.VARIABLE_UP_TO_255_BYTE, // SMALL_PORT_VALUE_CHANGE
        MessageSize.VARIABLE_UP_TO_255_BYTE, // SMALL_PORT_VALUE_CHANGE_WITHOUT_TIMESTAMP
        MessageSize.VARIABLE_UP_TO_4GB,      // OTHER
    };

    /** Return Status */
    public final static byte SUCCESS = 100, FAIL = 101;

    /** Name of ports only connection */
    public static final String TCP_PORTS_ONLY_NAME = "TCP-shared-ports";

    /** Greet message for initializing/identifying Finroc connections */
    public static final String GREET_MESSAGE = "Greetings! I am a Finroc TCP peer.";

    /** Finroc TCP protocol version */
    public static final short PROTOCOL_VERSION = 1;

    /** Inserted at the end of messages to debug TCP stream */
    public static final byte DEBUG_TCP_NUMBER = (byte)0xCD;

    /** Standard TCP connection creator */
    public static final CreateAction creator1 = new CreateAction(FrameworkElementInfo.StructureExchange.COMPLETE_STRUCTURE, "TCP", 0);

    /** Alternative TCP connection creator */
    public static final CreateAction creator2 = new CreateAction(FrameworkElementInfo.StructureExchange.SHARED_PORTS, TCP_PORTS_ONLY_NAME, 0);

    /** Complete TCP connection creator */
    public static final CreateAction creator3 = new CreateAction(FrameworkElementInfo.StructureExchange.FINSTRUCT, "TCP finstruct", CreateExternalConnectionAction.REMOTE_EDGE_INFO);


    @Override
    public void init(/*PluginManager mgr*/) {
//        Plugins.getInstance().registerExternalConnection(creator1);
//        Plugins.getInstance().registerExternalConnection(creator2);
//        Plugins.getInstance().registerExternalConnection(creator3);
    }

    /**
     * @param stream Stream to serialize address to
     * @param address Address to serialize (IPv4 or IPv6)
     */
    public static void serializeInetAddress(OutputStreamBuffer stream, InetAddress address) {
        byte[] rawAddress = address.getAddress();
        stream.writeBoolean(rawAddress.length > 4);
        stream.write(rawAddress);
    }

    /**
     *
     * @param stream Stream to deserialize from
     * @return Deserialized address
     */
    public static InetAddress deserializeInetAddress(InputStreamBuffer stream) {
        boolean v6 = stream.readBoolean();
        byte[] rawAddress = new byte[v6 ? 16 : 4];
        stream.readFully(rawAddress);
        try {
            return InetAddress.getByAddress(rawAddress);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Invalid IP address received. This should never happen.");
        }
    }

    /**
     * @param address Address to format for output (such as status message)
     * @return Formatted String for output
     */
    public static String formatInetSocketAddress(InetSocketAddress address) {
        String result = address.toString();
        if (result.startsWith("/")) {
            return result.substring(1);
        }
        return result;
    }

    /**
     * Class for TCP create-Actions
     */
    private static class CreateAction implements CreateExternalConnectionAction {

        /** Desired structure exchange */
        private final FrameworkElementInfo.StructureExchange structureExchange;

        /** Name of connection type */
        private final String name;

        /** Flags to use */
        private final int flags;

        /** Name of module type */
        private final String group;

        public CreateAction(FrameworkElementInfo.StructureExchange structureExchange, String name, int flags) {
            this.structureExchange = structureExchange;
            this.name = name;
            this.flags = flags;
            Plugins.getInstance().registerExternalConnection(this);
            this.group = RuntimeSettings.ANDROID_PLATFORM ? "finroc_plugins_tcp" : Plugins.getInstance().getContainingJarFile(TCP.class);
        }

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

        @Override
        public ExternalConnection createExternalConnection() throws Exception {
            return new Peer(RuntimeEnvironment.getInstance().getProgramName(), Peer.DEFAULT_CONNECTION_NAME, structureExchange, true);
        }

        @Override
        public int getFlags() {
            return flags;
        }
    }
}
