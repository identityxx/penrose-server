/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.thread;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class Queue {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected List queue = new ArrayList();

    private int maxSize = 0; // no limit

    public Queue() {
    }

    public Queue(int maxSize) {
        this.maxSize = maxSize;
    }

    public synchronized int size() {
        return queue.size();
    }

    public synchronized boolean isEmpty() {
        return queue.size() == 0;
    }

    public synchronized void add(Object obj) {

        if (maxSize > 0) {
            while (queue.size() == maxSize) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }

        //log.debug("Adding object");
        queue.add(obj);
        //log.debug("size: "+queue.size());
        notifyAll();
    }

    public synchronized Collection getAll() {
        List c = new ArrayList();
        c.addAll(queue);
        return c;
    }

    public synchronized Object remove() {
        //log.debug("Removing object");
        while (queue.size() == 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        Object o = queue.remove(0);
        //log.debug("size: "+queue.size());

        notifyAll();

        return o;
    }

    public synchronized Object peek() {
        while (queue.size() == 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        return queue.get(0);
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }
}
