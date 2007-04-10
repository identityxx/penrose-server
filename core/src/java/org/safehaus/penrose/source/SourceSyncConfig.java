package org.safehaus.penrose.source;

import java.util.Map;
import java.util.TreeMap;
import java.util.Collection;

/**
 * @author Administrator
 */
public class SourceSyncConfig implements Cloneable {

    private String name;
    private String className;
    private String description;

	/**
	 * Parameters.
	 */
	public Map parameters = new TreeMap();

	public Collection getParameterNames() {
		return parameters.keySet();
	}

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public String getParameter(String name) {
        return (String)parameters.get(name);
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if((object == null) || (object.getClass() != getClass())) return false;

        SourceSyncConfig sourceSyncConfig = (SourceSyncConfig)object;
        if (!equals(name, sourceSyncConfig.name)) return false;
        if (!equals(className, sourceSyncConfig.className)) return false;
        if (!equals(description, sourceSyncConfig.description)) return false;
        if (!equals(parameters, sourceSyncConfig.parameters)) return false;

        return true;
    }

    public void copy(SourceSyncConfig sourceSyncConfig) {
        name = sourceSyncConfig.name;
        className = sourceSyncConfig.className;
        description = sourceSyncConfig.description;

        parameters.clear();
        parameters.putAll(sourceSyncConfig.parameters);
    }

    public Object clone() {
        SourceSyncConfig sourceSyncConfig = new SourceSyncConfig();
        sourceSyncConfig.copy(this);
        return sourceSyncConfig;
    }
}
