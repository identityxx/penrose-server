package org.safehaus.penrose.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.service.ServiceServiceMBean;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceClient extends BaseClient implements ServiceServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    public ServiceClient(PenroseClient client, String name) throws Exception {
        super(client, name, getStringObjectName(name));
    }

    public String getStatus() throws Exception {
        return (String)getAttribute("Status");
    }

    public void start() throws Exception {
        invoke("start", new Object[] {}, new String[] {});
    }

    public void stop() throws Exception {
        invoke("stop", new Object[] {}, new String[] {});
    }

    public ServiceConfig getServiceConfig() throws Exception {
        return (ServiceConfig)getAttribute("ServiceConfig");
    }

    public static String getStringObjectName(String name) {
        return "Penrose:type=Service,name="+name;
    }
}
