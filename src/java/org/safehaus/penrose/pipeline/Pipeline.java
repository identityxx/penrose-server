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
package org.safehaus.penrose.pipeline;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Pipeline implements Iterator {

    public Logger log = Logger.getLogger(getClass());

    List list = new ArrayList();
    boolean done = false;

    Collection listeners = new LinkedHashSet();

    public void addListener(PipelineListener listener) {
        listeners.add(listener);
    }

    public void removeListener(PipelineListener listener) {
        listeners.remove(listener);
    }

    public void fireEvent(final PipelineEvent event) {
        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            PipelineListener listener = (PipelineListener)i.next();
            switch (event.getType()) {
                case PipelineEvent.OBJECT_ADDED:
                    listener.objectAdded(event);
                    break;
                case PipelineEvent.OBJECT_REMOVED:
                    listener.objectRemoved(event);
                    break;
                case PipelineEvent.PIPELINE_CLOSED:
                    listener.pipelineClosed(event);
                    break;
            }
        }
    }

    public synchronized void add(Object object) {
        list.add(object);
        fireEvent(new PipelineEvent(PipelineEvent.OBJECT_ADDED, object));

        notifyAll();
    }

    public synchronized void addAll(Collection collection) {
        for (Iterator i=collection.iterator(); i.hasNext(); ) {
            Object object = i.next();
            list.add(object);
            fireEvent(new PipelineEvent(PipelineEvent.OBJECT_ADDED, object));
        }

        notifyAll();
    }

    public synchronized boolean hasNext() {
        while (!done && list.size() == 0) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        return list.size() > 0;
    }

    public synchronized Object next() {
        while (!done && list.size() == 0) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        if (list.size() == 0) return null;

        Object object = list.remove(0);
        fireEvent(new PipelineEvent(PipelineEvent.OBJECT_ADDED, object));

        return object;
    }

    public void remove() {
        while (!done && list.size() == 0) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        if (list.size() == 0) return;

        Object object = list.remove(0);
        fireEvent(new PipelineEvent(PipelineEvent.OBJECT_ADDED, object));
    }

    public synchronized void close() {
        done = true;
        fireEvent(new PipelineEvent(PipelineEvent.PIPELINE_CLOSED));
        notifyAll();
    }

    public synchronized Collection getAll() {
        while (!done) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        return list;
    }

    public synchronized boolean isEmpty() {
        while (!done) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        return list.isEmpty();
    }

    public synchronized int size() {
        while (!done) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        return list.size();
    }

    public Iterator iterator() {
        return this;
    }

    public boolean isClosed() {
        return done;
    }
}
