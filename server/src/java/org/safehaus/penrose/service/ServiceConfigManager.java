package org.safehaus.penrose.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.ArrayList;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class ServiceConfigManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    public ServiceReader serviceReader = new ServiceReader();
    public ServiceWriter serviceWriter = new ServiceWriter();

    private Map<String,ServiceConfig> serviceConfigs = new LinkedHashMap<String,ServiceConfig>();
    private File servicesDir;

    public ServiceConfigManager(File servicesDir) throws Exception {
        this.servicesDir = servicesDir;
    }

    public Collection<String> getAvailableServiceNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        for (File serviceDir : servicesDir.listFiles()) {
            list.add(serviceDir.getName());
        }
        return list;
    }

    public ServiceConfig load(String serviceName) throws Exception {

        File dir = new File(servicesDir, serviceName);
        log.debug("Loading service from "+dir+".");

        return serviceReader.read(dir);
    }

    public void store(String serviceName, ServiceConfig serviceConfig) throws Exception {

        File dir = new File(servicesDir, serviceName);
        log.debug("Storing service from "+dir+".");

        serviceWriter.write(dir, serviceConfig);
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

    public File getServicesDir() {
        return servicesDir;
    }

    public void setServicesDir(File servicesDir) {
        this.servicesDir = servicesDir;
    }
}
