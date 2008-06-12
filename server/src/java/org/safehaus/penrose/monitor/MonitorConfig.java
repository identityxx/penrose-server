package org.safehaus.penrose.monitor;

import java.util.Map;
import java.util.TreeMap;
import java.util.Collection;
import java.util.ArrayList;
import java.net.URL;

/**
 * @author Endi Sukma Dewata
 */
public class MonitorConfig {

    private String name;
    private boolean enabled = true;
    private String monitorClass;
    private String description;

    private Map<String,String> parameters = new TreeMap<String,String>();

    private Collection<URL> classPaths = new ArrayList<URL>();

    public MonitorConfig() {
    }

    public MonitorConfig(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getMonitorClass() {
        return monitorClass;
    }

    public void setMonitorClass(String monitorClass) {
        this.monitorClass = monitorClass;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public Map<String,String> getParameters() {
        return parameters;
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (enabled ? 0 : 1) +
                (monitorClass == null ? 0 : monitorClass.hashCode()) +
                (description == null ? 0 : description.hashCode()) +
                (parameters == null ? 0 : parameters.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        MonitorConfig monitorConfig = (MonitorConfig)object;
        if (!equals(name, monitorConfig.name)) return false;
        if (enabled != monitorConfig.enabled) return false;
        if (!equals(monitorClass, monitorConfig.monitorClass)) return false;
        if (!equals(description, monitorConfig.description)) return false;
        if (!equals(parameters, monitorConfig.parameters)) return false;

        return true;
    }

    public void copy(MonitorConfig monitorConfig) {
        name = monitorConfig.name;
        enabled = monitorConfig.enabled;
        monitorClass = monitorConfig.monitorClass;
        description = monitorConfig.description;

        parameters = new TreeMap<String,String>();
        parameters.putAll(monitorConfig.parameters);

        classPaths = new ArrayList<URL>();
        classPaths.addAll(monitorConfig.classPaths);
    }

    public Object clone() throws CloneNotSupportedException {
        MonitorConfig monitorConfig = (MonitorConfig)super.clone();
        monitorConfig.copy(this);
        return monitorConfig;
    }

    public Collection<URL> getClassPaths() {
        return classPaths;
    }

    public void addClassPath(URL library) {
        classPaths.add(library);
    }

}
