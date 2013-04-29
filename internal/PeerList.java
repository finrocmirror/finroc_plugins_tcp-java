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
package org.finroc.plugins.tcp.internal;

import org.rrlib.finroc_core_utils.jc.container.SimpleList;
import org.rrlib.finroc_core_utils.jc.log.LogDefinitions;
import org.rrlib.finroc_core_utils.log.LogDomain;
import org.rrlib.finroc_core_utils.log.LogLevel;
import org.rrlib.finroc_core_utils.serialization.InputStreamBuffer;
import org.rrlib.finroc_core_utils.serialization.OutputStreamBuffer;

import org.finroc.core.RuntimeEnvironment;
import org.finroc.core.port.net.AbstractPeerTracker;

/**
 * @author Max Reichardt
 *
 * List of network peers that can be connected to.
 * Depends on external inputs.
 * Checks for duplicates etc.
 *
 * TODO: Implement proper PeerTracker based on libavahi
 */
class PeerList { /*extends AbstractPeerTracker*/

//    /** List of peers */
//    private SimpleList<IPSocketAddress> peers = new SimpleList<IPSocketAddress>();
//
//    /** Current version of list */
//    private volatile int revision = 0;
//
//    /** Server port of own peer */
//    private final int serverPort;
//
//    /** Log domain for this class */
//    public static final LogDomain logDomain = LogDefinitions.finroc.getSubDomain("tcp");
//
//    /** @param serverPort Server port of own peer */
//    public PeerList(int serverPort, int lockOrder) {
//        super(lockOrder);
//        this.serverPort = serverPort;
//        if (serverPort > 0) {
//            peers.add(new IPSocketAddress("localhost", serverPort));
//            revision++;
//        }
//    }
//
//    public void addPeer(IPSocketAddress isa, boolean notifyOnChange) {
//        synchronized (this) {
//            if (peers.contains(isa)) {
//                return;
//            }
//        }
//
//        synchronized (RuntimeEnvironment.getInstance().getRegistryLock()) {
//            boolean add = false;
//            synchronized (this) {
//                add = !peers.contains(isa);
//                if (add) {
//                    log(LogLevel.LL_DEBUG, logDomain, "received new peer: " + isa.toString());
//                    peers.add(isa);
//                }
//            }
//
//            if (add) {
//                if (notifyOnChange) {
//                    notifyDiscovered(isa, isa.toString());
//                }
//                revision++;
//            }
//        }
//    }
//
//    public void removePeer(IPSocketAddress isa) {
//
//        // make sure: peer can only be removed, while there aren't any other connection events being processed
//        SimpleList<AbstractPeerTracker.Listener> listenersCopy = new SimpleList<AbstractPeerTracker.Listener>();
//        listeners.getListenersCopy(listenersCopy);
//        SimpleList<AbstractPeerTracker.Listener> postProcess = new SimpleList<AbstractPeerTracker.Listener>();
//        SimpleList<Object> postProcessObj = new SimpleList<Object>();
//        synchronized (RuntimeEnvironment.getInstance().getRegistryLock()) {
//            synchronized (this) {
//                if (peers.contains(isa)) {
//                    peers.removeElem(isa);
//                    for (int i = 0, n = listenersCopy.size(); i < n; i++) {
//                        Object o = listenersCopy.get(i).nodeRemoved(isa, isa.toString());
//                        if (o != null) {
//                            postProcess.add(listenersCopy.get(i));
//                            postProcessObj.add(o);
//                        }
//                    }
//                    revision++;
//                }
//            }
//        }
//
//        for (int i = 0, n = postProcess.size(); i < n; i++) {
//            postProcess.get(i).nodeRemovedPostLockProcess(postProcessObj.get(i));
//        }
//    }
//
//    /**
//     * Serialize all known addresses
//     *
//     * @param co Output Stream
//     */
//    public synchronized void serializeAddresses(OutputStreamBuffer co) {
//        int size = peers.size();
//        co.writeInt(size);
//        for (int i = 0; i < size; i++) {
//            peers.get(i).serialize(co);
//        }
//    }
//
//    /**
//     * Deserialize addresses - and complete our own list
//     *
//     * @param ci Input Stream
//     * @param ownAddress Our own address from remote view
//     * @param partnerAddress IP address of partner
//     */
//    public void deserializeAddresses(InputStreamBuffer ci, IPAddress ownAddress, IPAddress partnerAddress) {
//        int size = ci.readInt();
//        for (int i = 0; i < size; i++) {
//            IPSocketAddress ia = IPSocketAddress.deserialize(ci);
//            if (ia.getAddress().equals(ownAddress) && ia.getPort() == serverPort) {
//                // skip... because we are that
//            } else {
//
//                // replace partner's localhost entries with partnerAddress
//                if (ia.getAddress().isLocalHost()) {
//                    ia = new IPSocketAddress(partnerAddress, ia.getPort());
//                }
//
//                addPeer(ia, true);
//            }
//        }
//    }
//
//    /**
//     * @return Revision of peer list (incremented with each change)
//     */
//    public int getRevision() {
//        return revision;
//    }
}
