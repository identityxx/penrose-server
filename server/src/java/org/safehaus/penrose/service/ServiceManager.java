/**
 * Copyright 2009 Red Hat, Inc.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class ServiceManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    PenroseServer penroseServer;

    private Map<String,Service> services = new LinkedHashMap<String,Service>();
    private ServiceConfigManager serviceConfigManager;

    public ServiceManager(PenroseServer penroseServer, ServiceConfigManager serviceConfigManager) {
        this.penroseServer = penroseServer;
        this.serviceConfigManager = serviceConfigManager;
    }

    public void addServiceConfig(ServiceConfig serviceConfig) throws Exception {
        serviceConfigManager.addServiceConfig(serviceConfig);
    }

    public File getServicesDir() {
        return serviceConfigManager.getServicesDir();
    }
    
    public void addService(Service service) {
        services.put(service.getName(), service);
    }

    public Service getService(String name) {
        return services.get(name);
    }

    public Collection<String> getServiceNames() {
        return services.keySet();
    }

    public Collection<Service> getServices() {
        return services.values();
    }

    public Service removeService(String name) {
        return services.remove(name);
    }

    public void clear() {
        services.clear();
    }

    public void loadServiceConfig(String name) throws Exception {

        log.debug("Loading "+name+" service.");

        ServiceConfig serviceConfig = serviceConfigManager.load(name);
        serviceConfigManager.addServiceConfig(serviceConfig);
    }

    public Service startService(String serviceName) throws Exception {

        ServiceConfig serviceConfig = serviceConfigManager.getServiceConfig(serviceName);
        
        if (!serviceConfig.isEnabled()) {
            log.debug(serviceConfig.getName()+" service is disabled.");
            return null;
        }

        log.debug("Starting "+serviceName+" service.");

        ServiceContext serviceContext = createServiceContext(serviceConfig);

        Service service = createService(serviceConfig, serviceContext);
        service.init(serviceConfig, serviceContext);

        addService(service);

        return service;
    }

    public ServiceContext createServiceContext(ServiceConfig serviceConfig) throws Exception {

        File serviceDir = new File(serviceConfigManager.getServicesDir(), serviceConfig.getName());
        ClassLoader classLoader = new ServiceClassLoader(serviceDir, getClass().getClassLoader());

        //Collection<URL> classPaths = serviceConfig.getClassPaths();
        //URLClassLoader classLoader = new URLClassLoader(classPaths.toArray(new URL[classPaths.size()]), getClass().getClassLoader());

        ServiceContext serviceContext = new ServiceContext();
        serviceContext.setPath(serviceDir);
        serviceContext.setPenroseServer(penroseServer);
        serviceContext.setClassLoader(classLoader);

        return serviceContext;
    }

    public Service createService(ServiceConfig serviceConfig, ServiceContext serviceContext) throws Exception {

        ClassLoader classLoader = serviceContext.getClassLoader();
        Class clazz = classLoader.loadClass(serviceConfig.getServiceClass());
        return (Service)clazz.newInstance();
    }

    public void stopService(String name) throws Exception {

        log.debug("Stopping "+name+" service.");

        Service service = getService(name);
        if (service == null) return;

        service.destroy();

        services.remove(name);
    }

    public void unloadService(String name) throws Exception {
        serviceConfigManager.removeServiceConfig(name);
    }

    public String getServiceStatus(String name) {

        Service service = services.get(name);

        if (service == null) {
            return "STOPPED";
        } else {
            return "STARTED";
        }
    }

    public ServiceConfigManager getServiceConfigManager() {
        return serviceConfigManager;
    }

    public void setServiceConfigManager(ServiceConfigManager serviceConfigManager) {
        this.serviceConfigManager = serviceConfigManager;
    }

    public ServiceConfig getServiceConfig(String serviceName) {
        return serviceConfigManager.getServiceConfig(serviceName);
    }
}