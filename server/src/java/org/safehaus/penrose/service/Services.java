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

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class Services implements ServicesMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private Map<String,Service> services = new LinkedHashMap<String,Service>();
    private File servicesDir;

    public Services(File servicesDir) {
        this.servicesDir = servicesDir;
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

    public File getServicesDir() {
        return servicesDir;
    }

    public void setServicesDir(File servicesDir) {
        this.servicesDir = servicesDir;
    }
}
