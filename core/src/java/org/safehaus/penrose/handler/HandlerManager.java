/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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
package org.safehaus.penrose.handler;

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.naming.PenroseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeMap;
import java.util.Map;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class HandlerManager {

    Logger log = LoggerFactory.getLogger(getClass());
    
    Map handlers = new TreeMap();

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    public HandlerManager() {
    }

    public void init(HandlerConfig handlerConfig) throws Exception {

        String name = handlerConfig.getName();
        String className = handlerConfig.getHandlerClass();
        if (className == null) {
            className = DefaultHandler.class.getName();
        }

        log.debug("Initializing handler "+name+": "+className);

        Class clazz = Class.forName(className);
        Handler handler = (Handler)clazz.newInstance();

        handler.setPenroseConfig(penroseConfig);
        handler.setPenroseContext(penroseContext);
        handler.init(handlerConfig);

        handlers.put(handlerConfig.getName(), handler);
    }

    public Handler getHandler(String name) {
        return (Handler)handlers.get(name);
    }

    public Handler getHandler(Partition partition) {
        String handlerName = partition == null ? "DEFAULT" : partition.getHandlerName();
        if (log.isDebugEnabled()) {
            log.debug("Getting handler for partition "+partition+": "+handlerName);
        }
        return (Handler)handlers.get(handlerName);
    }
    
    public void clear() {
        handlers.clear();
    }

    public void start() throws Exception {
        for (Iterator i=handlers.values().iterator(); i.hasNext(); ) {
            Handler handler = (Handler)i.next();
            handler.start();
        }
    }

    public void stop() throws Exception {
        for (Iterator i=handlers.values().iterator(); i.hasNext(); ) {
            Handler handler = (Handler)i.next();
            handler.stop();
        }
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }
}
