package org.safehaus.penrose.management.service;

import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.service.ServiceConfigManager;
import org.safehaus.penrose.service.ServiceManager;
import org.safehaus.penrose.service.ServiceWriter;
import org.safehaus.penrose.util.FileUtil;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceManagerService extends BaseService implements ServiceManagerServiceMBean {

    ServiceManager serviceManager;

    public ServiceManagerService(PenroseJMXService jmxService, ServiceManager serviceManager) {
        super(ServiceManagerServiceMBean.class);

        this.jmxService = jmxService;
        this.serviceManager = serviceManager;
    }

    public Object getObject() {
        return serviceManager;
    }

    public String getObjectName() {
        return ServiceManagerClient.getStringObjectName();
    }


    public Collection<String> getServiceNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        ServiceConfigManager serviceConfigManager = serviceManager.getServiceConfigManager();
        list.addAll(serviceConfigManager.getAvailableServiceNames());
        return list;
    }

    public ServiceConfig getServiceConfig(String serviceName) throws Exception {
        return serviceManager.getServiceConfig(serviceName);
    }

    public ServiceService getServiceService(String name) throws Exception {

        ServiceService service = new ServiceService(jmxService, serviceManager, name);
        service.init();

        return service;
    }

    public void startService(String name) throws Exception {

        serviceManager.startService(name);

        ServiceService service = getServiceService(name);
        service.register();
    }

    public void stopService(String name) throws Exception {

        ServiceService service = getServiceService(name);
        service.unregister();

        serviceManager.stopService(name);
    }

    public void createService(ServiceConfig serviceConfig) throws Exception {

        String name = serviceConfig.getName();

        File servicesDir = serviceManager.getServicesDir();
        File path = new File(servicesDir, name);

        ServiceWriter serviceWriter = new ServiceWriter();
        serviceWriter.write(path, serviceConfig);

        serviceManager.addServiceConfig(serviceConfig);
    }

    public void updateService(String name, ServiceConfig serviceConfig) throws Exception {

        serviceManager.unloadService(name);

        File servicesDir = serviceManager.getServicesDir();
        File oldDir = new File(servicesDir, name);
        File newDir = new File(servicesDir, serviceConfig.getName());
        oldDir.renameTo(newDir);

        ServiceWriter serviceWriter = new ServiceWriter();
        serviceWriter.write(newDir, serviceConfig);

        serviceManager.addServiceConfig(serviceConfig);
    }

    public void removeService(String name) throws Exception {

        File servicesDir = serviceManager.getServicesDir();
        File serviceDir = new File(servicesDir, name);

        serviceManager.unloadService(name);

        FileUtil.delete(serviceDir);
    }

    public void register() throws Exception {
        super.register();

        for (String serviceName : getServiceNames()) {
            ServiceService serviceService = getServiceService(serviceName);
            serviceService.register();
        }
    }

    public void unregister() throws Exception {
        for (String serviceName : getServiceNames()) {
            ServiceService serviceService = getServiceService(serviceName);
            serviceService.unregister();
        }

        super.unregister();
    }
}
