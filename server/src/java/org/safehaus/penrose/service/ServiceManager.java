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

import java.util.*;
import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * @author Endi S. Dewata
 */
public class ServiceManager implements ServiceManagerMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseServer penroseServer;

    private Map<String,Service> services = new LinkedHashMap<String,Service>();

    public void loadServices(String dir) throws Exception {

        ServiceReader serviceReader = new ServiceReader();

        File services = new File(dir);
        for (File file : services.listFiles()) {
            if (!file.isDirectory()) continue;

            String base = file.getAbsolutePath();
            ServiceConfig serviceConfig = serviceReader.read(base);

            load(base, serviceConfig);
        }
    }

    public void load(Collection<ServiceConfig> serviceConfigs) throws Exception {
        for (ServiceConfig serviceConfig : serviceConfigs) {
            load(null, serviceConfig);
        }
    }

    public void load(String base, ServiceConfig serviceConfig) throws Exception {

        if (!serviceConfig.isEnabled()) return;

        log.debug("----------------------------------------------------------------------------------");
        log.debug("Loading service "+serviceConfig.getName()+".");

        log.debug("Classpath:");
        List<URL> urls = new ArrayList<URL>();

        File classesDir = new File(base+File.separator+"SERVICE-INF"+File.separator+"classes");

        if (classesDir.exists()) {
            URL url = classesDir.toURL();
            log.debug(" - "+url);
            urls.add(url);
        }

        File libDir = new File(base+File.separator+"SERVICE-INF"+File.separator+"lib");

        if (libDir.isDirectory()) {
            File files[] = libDir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".jar");
                }
            });

            for (File file : files) {
                URL url = file.toURL();
                log.debug(" - "+url);
                urls.add(url);
            }
        }

        URLClassLoader classLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]));

        Class clazz = classLoader.loadClass(serviceConfig.getServiceClass());
        Service service = (Service)clazz.newInstance();

        service.setClassLoader(classLoader);

        service.setPenroseServer(penroseServer);
        service.setServiceConfig(serviceConfig);
        service.init();

        addService(serviceConfig.getName(), service);
    }

    public void start() throws Exception {
        for (String name : getServiceNames()) {
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
        Collection<String> list = getServiceNames();
        String names[] = list.toArray(new String[list.size()]);

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

    public PenroseServer getPenroseServer() {
        return penroseServer;
    }

    public void setPenroseServer(PenroseServer penroseServer) {
        this.penroseServer = penroseServer;
    }
}
