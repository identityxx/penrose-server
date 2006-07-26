package org.safehaus.penrose.server.config;

import org.safehaus.penrose.service.ServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PenroseServerConfig implements PenroseServerConfigMBean, Cloneable {

    Logger log = LoggerFactory.getLogger(getClass());

    private String home;

    private Map systemProperties = new LinkedHashMap();
    private Map serviceConfigs   = new LinkedHashMap();

    public PenroseServerConfig() {
    }

    public String getSystemProperty(String name) {
        return (String)systemProperties.get(name);
    }

    public Map getSystemProperties() {
        return systemProperties;
    }

    public Collection getSystemPropertyNames() {
        return systemProperties.keySet();
    }

    public void setSystemProperty(String name, String value) {
        systemProperties.put(name, value);
    }

    public String removeSystemProperty(String name) {
        return (String)systemProperties.remove(name);
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }

    public void addServiceConfig(ServiceConfig serviceConfig) {
        serviceConfigs.put(serviceConfig.getName(), serviceConfig);
    }

    public ServiceConfig getServiceConfig(String name) {
        return (ServiceConfig)serviceConfigs.get(name);
    }

    public Collection getServiceConfigs() {
        return serviceConfigs.values();
    }

    public Collection getServiceNames() {
        return serviceConfigs.keySet();
    }

    public ServiceConfig removeServiceConfig(String name) {
        return (ServiceConfig)serviceConfigs.remove(name);
    }

    public int hashCode() {
        return (home == null ? 0 : home.hashCode()) +
                (systemProperties == null ? 0 : systemProperties.hashCode()) +
                (serviceConfigs == null ? 0 : serviceConfigs.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        PenroseServerConfig penroseConfig = (PenroseServerConfig)object;

        if (!equals(home, penroseConfig.home)) return false;

        if (!equals(systemProperties, penroseConfig.systemProperties)) return false;
        if (!equals(serviceConfigs, penroseConfig.serviceConfigs)) return false;

        return true;
    }

    public void copy(PenroseServerConfig penroseConfig) {
        clear();

        home = penroseConfig.home;

        systemProperties.putAll(penroseConfig.systemProperties);

        for (Iterator i=penroseConfig.serviceConfigs.values().iterator(); i.hasNext(); ) {
            ServiceConfig serviceConfig = (ServiceConfig)i.next();
            addServiceConfig((ServiceConfig)serviceConfig.clone());
        }
    }

    public void clear() {
        systemProperties.clear();
        serviceConfigs.clear();
    }

    public Object clone() {
        PenroseServerConfig penroseServerConfig = new PenroseServerConfig();
        penroseServerConfig.copy(this);

        return penroseServerConfig;
    }
}
