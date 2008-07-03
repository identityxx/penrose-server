package org.safehaus.penrose.thread;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public class ThreadManagerConfig implements Serializable, Cloneable {

    public final static String CORE_POOL_SIZE         = "corePoolSize";
    public final static int DEFAULT_CORE_POOL_SIZE    = 10;

    public final static String MAXIMUM_POOL_SIZE      = "maximumPoolSize";
    public final static int DEFAULT_MAXIMUM_POOL_SIZE = 10;

    public final static String KEEP_ALIVE_TIME        = "keepAliveTime";
    public final static int DEFAULT_KEEP_ALIVE_TIME   = 60;

    private boolean enabled = true;

    private String threadManagerClass;
    private String description;

    private Map<String,String> parameters = new LinkedHashMap<String,String>();

    public ThreadManagerConfig() {
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getThreadManagerClass() {
        return threadManagerClass;
    }

    public void setThreadManagerClass(String threadManagerClass) {
        this.threadManagerClass = threadManagerClass;
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public Map<String,String> getParameters() {
        return parameters;
    }

    public int hashCode() {
        return threadManagerClass == null ? 0 : threadManagerClass.hashCode();
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

        ThreadManagerConfig threadManagerConfig = (ThreadManagerConfig)object;
        if (!equals(description, threadManagerConfig.description)) return false;
        if (!equals(threadManagerClass, threadManagerConfig.threadManagerClass)) return false;
        if (!equals(parameters, threadManagerConfig.parameters)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        ThreadManagerConfig threadManagerConfig = (ThreadManagerConfig)super.clone();
        threadManagerConfig.copy(this);
        return threadManagerConfig;
    }

    public void copy(ThreadManagerConfig threadManagerConfig) {
        description = threadManagerConfig.description;
        threadManagerClass = threadManagerConfig.threadManagerClass;

        parameters = new LinkedHashMap<String,String>();
        parameters.putAll(threadManagerConfig.parameters);
    }
}
