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
package org.safehaus.penrose.service;

import org.safehaus.penrose.server.PenroseServer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Service implements ServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    public final static String STOPPING = "STOPPING";
    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";

    private PenroseServer penroseServer;
    private ServiceConfig serviceConfig;

    private String status = STOPPED;

    public Collection getParameterNames() {
        return serviceConfig.getParameterNames();
    }

    public String getParameter(String name) {
        return serviceConfig.getParameter(name);
    }

    public void setParameter(String name, String value) {
        serviceConfig.setParameter(name, value);
    }

    public String removeParameter(String name) {
        return serviceConfig.removeParameter(name);
    }

    public void init() throws Exception {
    }

    public void start() throws Exception {
        setStatus(STARTED);
    }

    public void stop() throws Exception {
        setStatus(STOPPED);
    }

    public void restart() throws Exception {
        stop();
        start();
    }

    public ServiceConfig getServiceConfig() {
        return serviceConfig;
    }

    public void setServiceConfig(ServiceConfig serviceConfig) {
        this.serviceConfig = serviceConfig;
    }

    public PenroseServer getPenroseServer() {
        return penroseServer;
    }

    public void setPenroseServer(PenroseServer penroseServer) {
        this.penroseServer = penroseServer;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getName() {
        return serviceConfig.getName();
    }

    public String getServiceClass() {
        return serviceConfig.getServiceClass();
    }

    public String getDescription() {
        return serviceConfig.getDescription();
    }
}
