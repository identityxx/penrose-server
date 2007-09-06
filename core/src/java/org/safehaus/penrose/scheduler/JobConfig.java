package org.safehaus.penrose.scheduler;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class JobConfig implements Serializable, Cloneable {

    private boolean enabled = true;

    private String name;
    private String description;

    private String jobClass;
    private Map<String,String> parameters = new HashMap<String,String>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getJobClass() {
        return jobClass;
    }

    public void setJobClass(String jobClass) {
        this.jobClass = jobClass;
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public void setParameter(String name, String value) {
        if (value == null) {
            parameters.remove(name);
        } else {
            parameters.put(name, value);
        }
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public Map<String,String> getParameters() {
        return parameters;
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public void clearParameters() {
        parameters.clear();
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
         if (this == object) return true;
         if (object == null) return false;
         if (object.getClass() != this.getClass()) return false;

         JobConfig triggerConfig = (JobConfig)object;
         if (enabled != triggerConfig.enabled) return false;

         if (!equals(name, triggerConfig.name)) return false;
         if (!equals(description, triggerConfig.description)) return false;

         if (!equals(jobClass, triggerConfig.jobClass)) return false;

         if (!equals(parameters, triggerConfig.parameters)) return false;

         return true;
     }

     public void copy(JobConfig jobConfig) throws CloneNotSupportedException {
         enabled = jobConfig.enabled;

         name = jobConfig.name;
         description = jobConfig.description;

         jobClass = jobConfig.jobClass;

         parameters = new HashMap<String,String>();
         parameters.putAll(jobConfig.parameters);
     }

     public Object clone() throws CloneNotSupportedException {
         JobConfig jobConfig = (JobConfig)super.clone();
         jobConfig.copy(this);
         return jobConfig;
     }
}
