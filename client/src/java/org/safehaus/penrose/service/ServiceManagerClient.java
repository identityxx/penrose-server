package org.safehaus.penrose.service;

import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.management.BaseClient;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.management.service.ServiceManagerServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceManagerClient extends BaseClient implements ServiceManagerServiceMBean {

    public static Logger log = LoggerFactory.getLogger(ServiceManagerClient.class);

    public ServiceManagerClient(PenroseClient client) throws Exception {
        super(client, "ServiceManager", getStringObjectName());
    }

    public Collection<String> getServiceNames() throws Exception {
        return (Collection<String>)getAttribute("ServiceNames");
    }

    public ServiceConfig getServiceConfig(String name) throws Exception {
        return (ServiceConfig)invoke(
                "getServiceConfig",
                new Object[] { name },
                new String[] { String.class.getName() });
    }

    public void startService(String name) throws Exception {
        invoke("startService",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void stopService(String name) throws Exception {
        invoke("stopService",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void createService(ServiceConfig serviceConfig) throws Exception {
        invoke("createService",
                new Object[] { serviceConfig },
                new String[] { ServiceConfig.class.getName() }
        );
    }

    public void updateService(String name, ServiceConfig serviceConfig) throws Exception {
        invoke("updateService",
                new Object[] { name, serviceConfig },
                new String[] { String.class.getName(), ServiceConfig.class.getName() }
        );
    }

    public void removeService(String name) throws Exception {
        invoke("removeService",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public ServiceClient getServiceClient(String serviceName) throws Exception {
        return new ServiceClient(client, serviceName);
    }

    public static String getStringObjectName() {
        return "Penrose:name=ServiceManager";
    }
}
