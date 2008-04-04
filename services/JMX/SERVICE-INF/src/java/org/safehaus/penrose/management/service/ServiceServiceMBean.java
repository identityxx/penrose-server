package org.safehaus.penrose.management.service;

import org.safehaus.penrose.service.ServiceConfig;

/**
 * @author Endi Sukma Dewata
 */
public interface ServiceServiceMBean {

    public String getStatus() throws Exception;
    public void start() throws Exception;
    public void stop() throws Exception;

    public ServiceConfig getServiceConfig() throws Exception;
}
