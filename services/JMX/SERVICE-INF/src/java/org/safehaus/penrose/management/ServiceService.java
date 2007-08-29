package org.safehaus.penrose.management;

import org.safehaus.penrose.service.*;
import org.safehaus.penrose.server.PenroseServer;

import javax.management.StandardMBean;
import java.io.File;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceService extends StandardMBean implements ServiceServiceMBean {

    private PenroseJMXService jmxService;
    private Services services;
    private String name;

    public ServiceService() throws Exception {
        super(ServiceServiceMBean.class);
    }

    public String getStatus() {
        PenroseServer penroseServer = jmxService.getServiceContext().getPenroseServer();
        return penroseServer.getServiceStatus(name);
    }

    public void start() throws Exception {
        PenroseServer penroseServer = jmxService.getServiceContext().getPenroseServer();
        penroseServer.startService(name);
    }

    public void stop() throws Exception {
        PenroseServer penroseServer = jmxService.getServiceContext().getPenroseServer();
        penroseServer.stopService(name);
    }

    public void restart() throws Exception {
        stop();
        start();
    }

    public String getObjectName() {
        return ServiceClient.getObjectName(name);
    }

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);
    }

    public void unregister() throws Exception {
        jmxService.unregister(getObjectName());
    }

    public PenroseJMXService getJmxService() {
        return jmxService;
    }

    public void setJmxService(PenroseJMXService jmxService) {
        this.jmxService = jmxService;
    }

    public Services getServices() {
        return services;
    }

    public void setServices(Services services) {
        this.services = services;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
