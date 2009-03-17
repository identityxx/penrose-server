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

    public ServiceReader serviceReader = new ServiceReader();
    public ServiceWriter serviceWriter = new ServiceWriter();

    private Map<String,ServiceConfig> serviceConfigs = new LinkedHashMap<String,ServiceConfig>();
    private File servicesDir;

    public ServiceConfigManager(File servicesDir) throws Exception {
        this.servicesDir = servicesDir;
    }

    public void addServiceConfig(ServiceConfig serviceConfig) throws Exception {

        Logger log = LoggerFactory.getLogger(getClass());
        boolean debug = log.isDebugEnabled();

        String serviceName = serviceConfig.getName();

        if (debug) log.debug("Adding service \""+serviceName+"\".");

        validate(serviceConfig);

        serviceConfigs.put(serviceName, serviceConfig);
    }

    public void validate(ServiceConfig serviceConfig) throws Exception {

        String serviceName = serviceConfig.getName();

        if (serviceName == null || "".equals(serviceName)) {
            throw new Exception("Missing service name.");
        }

        char startingChar = serviceName.charAt(0);
        if (!Character.isLetter(startingChar)) {
            throw new Exception("Invalid service name: "+serviceName);
        }

        for (int i = 1; i<serviceName.length(); i++) {
            char c = serviceName.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') continue;
            throw new Exception("Invalid service name: "+serviceName);
        }

        if (serviceConfigs.containsKey(serviceName)) {
            throw new Exception("Service "+serviceName+" already exists.");
        }
    }

    public Collection<String> getAvailableServiceNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        for (File serviceDir : servicesDir.listFiles()) {
            list.add(serviceDir.getName());
        }
        return list;
    }

    public ServiceConfig load(String serviceName) throws Exception {

        Logger log = LoggerFactory.getLogger(getClass());
        boolean debug = log.isDebugEnabled();

        File dir = new File(servicesDir, serviceName);
        if (debug) log.debug("Loading service from "+dir+".");

        return serviceReader.read(dir);
    }

    public void store(String serviceName, ServiceConfig serviceConfig) throws Exception {

        Logger log = LoggerFactory.getLogger(getClass());
        boolean debug = log.isDebugEnabled();

        File dir = new File(servicesDir, serviceName);
        if (debug) log.debug("Storing service from "+dir+".");

        serviceWriter.write(dir, serviceConfig);
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
