package org.safehaus.penrose.management.service;

import org.safehaus.penrose.service.ServiceConfig;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface ServiceManagerServiceMBean {

    public Collection<String> getServiceNames() throws Exception;

    public void createService(ServiceConfig serviceConfig) throws Exception;
    public void updateService(String serviceName, ServiceConfig serviceConfig) throws Exception;
    public void removeService(String serviceName) throws Exception;

    public void startService(String serviceName) throws Exception;
    public void stopService(String serviceName) throws Exception;

    public ServiceConfig getServiceConfig(String serviceName) throws Exception;
}
