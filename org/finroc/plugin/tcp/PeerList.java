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

import org.finroc.jc.container.SimpleList;
import org.finroc.jc.net.IPAddress;
import org.finroc.jc.net.IPSocketAddress;
import org.finroc.jc.stream.InputStreamBuffer;
import org.finroc.jc.stream.OutputStreamBuffer;

import org.finroc.core.port.net.AbstractPeerTracker;

/**
 * @author max
 *
 * List of network peers that can be connected to.
 * Depends on external inputs.
 * Checks for duplicates etc.
 *
 * TODO: Implement proper PeerTracker based on libavahi
 */
public class PeerList extends AbstractPeerTracker {

    /** List of peers */
    private SimpleList<IPSocketAddress> peers = new SimpleList<IPSocketAddress>();

    /** Current version of list */
    private volatile int revision = 0;

    /** Server port of own peer */
    private final int serverPort;

    /** @param serverPort Server port of own peer */
    public PeerList(int serverPort) {
        this.serverPort = serverPort;
        if (serverPort > 0) {
            addPeer(new IPSocketAddress("localhost", serverPort), false);
        }
    }

//  /**
//   * @param port Port that we are listening on (used so that we don't connect to ourselves)
//   */
//  public PeerList(int port) {
//      addPeer(IPSocketAddress.createUnresolved("localhost", port));
//      addPeer(I)
//  }

    public synchronized void addPeer(IPSocketAddress isa, boolean notifyOnChange) {
        if (!peers.contains(isa)) {
            System.out.println("received new peer: " + isa.toString());
            peers.add(isa);
            if (notifyOnChange) {
                notifyDiscovered(isa, isa.toString());
            }
            revision++;
        }
    }

    public synchronized void removePeer(IPSocketAddress isa) {
        if (peers.contains(isa)) {
            peers.removeElem(isa);
            notifyRemoved(isa, isa.toString());
            revision--;
        }
    }

    /**
     * Serialize all known addresses
     *
     * @param co Output Stream
     */
    public synchronized void serializeAddresses(OutputStreamBuffer co) {
        int size = peers.size();
        co.writeInt(size);
        for (int i = 0; i < size; i++) {
            peers.get(i).serialize(co);
        }
    }

    /**
     * Deserialize addresses - and complete our own list
     *
     * @param ci Input Stream
     * @param ownAddress Our own address from remote view
     * @param partnerAddress IP address of partner
     */
    public synchronized void deserializeAddresses(InputStreamBuffer ci, IPAddress ownAddress, IPAddress partnerAddress) {
        int size = ci.readInt();
        for (int i = 0; i < size; i++) {
            IPSocketAddress ia = IPSocketAddress.deserialize(ci);
            if (ia.getAddress().equals(ownAddress) && ia.getPort() == serverPort) {
                // skip... because we are that
            } else {

                // replace partner's localhost entries with partnerAddress
                if (ia.getAddress().equals(IPAddress.getLocalHost())) {
                    ia = new IPSocketAddress(partnerAddress, ia.getPort());
                }

                addPeer(ia, true);
            }
        }
    }

    /**
     * @return Revision of peer list (incremented with each change)
     */
    public int getRevision() {
        return revision;
    }
}
