package org.safehaus.penrose.management.service;

import org.safehaus.penrose.service.*;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceService extends BaseService implements ServiceServiceMBean {

    private ServiceManager serviceManager;
    private String serviceName;

    public ServiceService(PenroseJMXService jmxService, ServiceManager serviceManager, String serviceName) throws Exception {
        super(ServiceServiceMBean.class);

        this.jmxService = jmxService;
        this.serviceManager = serviceManager;
        this.serviceName = serviceName;
    }

    public Object getObject() {
        return serviceManager.getService(serviceName);
    }

    public String getStatus() {
        return serviceManager.getServiceStatus(serviceName);
    }

    public void start() throws Exception {
        serviceManager.loadServiceConfig(serviceName);
        serviceManager.startService(serviceName);
    }

    public void stop() throws Exception {
        serviceManager.stopService(serviceName);
        serviceManager.unloadService(serviceName);
    }

    public ServiceConfig getServiceConfig() throws Exception {
        return serviceManager.getServiceConfig(serviceName);
    }

    public String getObjectName() {
        return ServiceClient.getStringObjectName(serviceName);
    }

    public ServiceManager getServiceManager() {
        return serviceManager;
    }

    public void setServiceManager(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
}
