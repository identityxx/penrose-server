package org.safehaus.penrose.federation;

import java.io.Serializable;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.HashMap;

/**
 * @author Endi Sukma Dewata
 */
public class FederationPartitionConfig implements Serializable, Comparable, Cloneable {

    public final static long serialVersionUID = 1L;

    protected String name;
    protected String template;

    protected Map<String,String> repositoryRefs = new HashMap<String,String>();
    protected Map<String,String> parameters = new HashMap<String,String>();

    public FederationPartitionConfig() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTemplate() {
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public void setRepositoryRef(FederationRepositoryRef ref) {
        repositoryRefs.put(ref.getName(), ref.getRepository());
    }

    public String removeRepositoryRef(String refName) {
        return repositoryRefs.remove(refName);
    }

    public Collection<String> getRepositoryRefNames() {
        return repositoryRefs.keySet();
    }

    public String getRepository(String refName) {
        return repositoryRefs.get(refName);
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public Object removeParameter(String name) {
        return parameters.remove(name);
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public boolean getBooleanParameter(String name) {
        String s = parameters.get(name);
        return s != null && Boolean.parseBoolean(s);
    }

    public void setParameter(String name, boolean b) {
        if (name == null) {
            parameters.remove(name);
        } else {
            parameters.put(name, ""+b);
        }
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (template == null ? 0 : template.hashCode()) +
                (repositoryRefs == null ? 0 : repositoryRefs.hashCode()) +
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

        FederationPartitionConfig federationPartitionConfig = (FederationPartitionConfig)object;
        if (!equals(name, federationPartitionConfig.name)) return false;
        if (!equals(template, federationPartitionConfig.template)) return false;
        if (!equals(repositoryRefs, federationPartitionConfig.repositoryRefs)) return false;
        if (!equals(parameters, federationPartitionConfig.parameters)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        FederationPartitionConfig federationPartitionConfig = (FederationPartitionConfig)super.clone();

        federationPartitionConfig.name = name;
        federationPartitionConfig.template = template;

        federationPartitionConfig.repositoryRefs = new LinkedHashMap<String,String>();
        for (String alias : repositoryRefs.keySet()) {
            String repository = repositoryRefs.get(alias);
            federationPartitionConfig.repositoryRefs.put(alias, repository);
        }

        federationPartitionConfig.parameters = new LinkedHashMap<String,String>();
        for (String name : parameters.keySet()) {
            String value = parameters.get(name);
            federationPartitionConfig.parameters.put(name, value);
        }

        return federationPartitionConfig;
    }

    public int compareTo(Object object) {
        if (this == object) return 0;
        if (object == null) return 0;
        if (!(object instanceof FederationPartitionConfig)) return 0;

        FederationPartitionConfig federationPartitionConfig = (FederationPartitionConfig)object;
        return name.compareTo(federationPartitionConfig.name);
    }
}
