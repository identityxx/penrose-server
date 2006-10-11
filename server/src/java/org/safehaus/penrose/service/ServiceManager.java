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

import java.util.Map;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class ServiceManager implements ServiceManagerMBean {

    Logger log = LoggerFactory.getLogger(getClass());

    private PenroseServer penroseServer;

    private Map services = new LinkedHashMap();

    public void load(Collection serviceConfigs) throws Exception {
        for (Iterator i=serviceConfigs.iterator(); i.hasNext(); ) {
            ServiceConfig serviceConfig = (ServiceConfig)i.next();
            load(serviceConfig);
        }
    }

    public void load(ServiceConfig serviceConfig) throws Exception {

        Service service = getService(serviceConfig.getName());
        if (service != null) return;
        
        Class clazz = Class.forName(serviceConfig.getServiceClass());
        service = (Service)clazz.newInstance();

        service.setPenroseServer(penroseServer);
        service.setServiceConfig(serviceConfig);
        service.init();

        addService(serviceConfig.getName(), service);
    }

    public void start() throws Exception {
        //log.debug("Starting services...");
        for (Iterator i=getServiceNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            start(name);
        }
    }

    public void start(String name) throws Exception {

        Service service = getService(name);
        if (service == null) throw new Exception(name+" not found.");

        ServiceConfig serviceConfig = service.getServiceConfig();
        if (!serviceConfig.isEnabled()) return;

        log.debug("Starting "+name+".");
        service.start();
    }

    public void stop() throws Exception {
        Collection list = getServiceNames();
        String names[] = (String[])list.toArray(new String[list.size()]);

        for (int i=names.length-1; i>=0; i--) {
            String name = names[i];
            stop(name);
        }
    }

    public void stop(String name) throws Exception {

        Service service = getService(name);
        if (service == null) throw new Exception(name+" not found.");

        ServiceConfig serviceConfig = service.getServiceConfig();
        if (!serviceConfig.isEnabled()) return;

        log.debug("Stopping "+name+".");
        service.stop();
    }

    public String getStatus(String name) throws Exception {
        Service service = getService(name);
        if (service == null) throw new Exception(name+" not found.");
        return service.getStatus();
    }

    public void addService(String name, Service service) {
        services.put(name, service);
    }

    public Service getService(String name) {
        return (Service)services.get(name);
    }

    public Collection getServiceNames() {
        return services.keySet();
    }

    public Collection getServices() {
        return services.values();
    }

    public Service removeService(String name) {
        return (Service)services.remove(name);
    }

    public void clear() {
        services.clear();
    }

    public PenroseServer getPenroseServer() {
        return penroseServer;
    }

    public void setPenroseServer(PenroseServer penroseServer) {
        this.penroseServer = penroseServer;
    }
}
