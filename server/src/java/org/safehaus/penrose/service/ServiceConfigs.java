package org.safehaus.penrose.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class ServiceConfigs implements ServiceConfigsMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    public ServiceReader serviceReader = new ServiceReader();

    private Map<String,ServiceConfig> serviceConfigs = new LinkedHashMap<String,ServiceConfig>();

    public ServiceConfig load(File dir) throws Exception {
        log.debug("Loading service from "+dir+".");
        ServiceConfig serviceConfig = serviceReader.read(dir);

        addServiceConfig(serviceConfig);

        return serviceConfig;
    }

    public void addServiceConfig(ServiceConfig serviceConfig) {
        serviceConfigs.put(serviceConfig.getName(), serviceConfig);
    }

    public ServiceConfig getServiceConfig(String name) {
        return serviceConfigs.get(name);
    }

    public Collection<String> getServiceNames() {
        return serviceConfigs.keySet();
    }

    public Collection<ServiceConfig> getServiceConfigs() {
        return serviceConfigs.values();
    }

    public ServiceConfig removeServiceConfig(String name) {
        return serviceConfigs.remove(name);
    }

    public void clear() {
        serviceConfigs.clear();
    }
}
