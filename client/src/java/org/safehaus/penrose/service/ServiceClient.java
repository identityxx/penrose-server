package org.safehaus.penrose.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.management.BaseClient;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.management.service.ServiceServiceMBean;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceClient extends BaseClient implements ServiceServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    public ServiceClient(PenroseClient client, String name) throws Exception {
        super(client, name, getStringObjectName(name));
    }

    public String getStatus() throws Exception {
        return (String)connection.getAttribute(objectName, "Status");
    }

    public void start() throws Exception {
        connection.invoke(objectName, "start", new Object[] {}, new String[] {});
    }

    public void stop() throws Exception {
        connection.invoke(objectName, "stop", new Object[] {}, new String[] {});
    }

    public ServiceConfig getServiceConfig() throws Exception {
        return (ServiceConfig)connection.getAttribute(objectName, "ServiceConfig");
    }

    public static String getStringObjectName(String name) {
        return "Penrose:type=service,name="+name;
    }
}
