package org.safehaus.penrose.federation.repository;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public class Repository implements Serializable, Cloneable {

    protected String name;
    protected String type;

    protected Map<String,String> parameters = new LinkedHashMap<String,String>();

    public Repository() {
    }

    public Repository(Repository repository) {
        setName(repository.name);
        setType(repository.type);
        setParameters(repository.parameters);
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }
    
    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }
    
    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        if (parameters == this.parameters) return;
        this.parameters.clear();
        this.parameters.putAll(parameters);
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (type == null ? 0 : type.hashCode()) +
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

        Repository domain = (Repository)object;
        if (!equals(name, domain.name)) return false;
        if (!equals(type, domain.type)) return false;
        if (!equals(parameters, domain.parameters)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        Repository repository = (Repository)super.clone();

        repository.name = name;
        repository.type = type;

        repository.parameters = new LinkedHashMap<String,String>();
        for (String name : parameters.keySet()) {
            String value = parameters.get(name);
            repository.parameters.put(name, value);
        }

        return repository;
    }
}
