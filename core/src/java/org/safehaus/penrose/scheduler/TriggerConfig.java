package org.safehaus.penrose.scheduler;

import java.io.Serializable;
import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class TriggerConfig implements Serializable, Cloneable {

    private boolean enabled = true;

    private String name = "DEFAULT";
    private String description;

    private String triggerClass;
    private Collection<String> jobNames = new ArrayList<String>();

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

    public String getTriggerClass() {
        return triggerClass;
    }

    public void setTriggerClass(String triggerClass) {
        this.triggerClass = triggerClass;
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

         TriggerConfig triggerConfig = (TriggerConfig)object;
         if (enabled != triggerConfig.enabled) return false;

         if (!equals(name, triggerConfig.name)) return false;
         if (!equals(description, triggerConfig.description)) return false;

         if (!equals(triggerClass, triggerConfig.triggerClass)) return false;
         if (!equals(jobNames, triggerConfig.jobNames)) return false;

         if (!equals(parameters, triggerConfig.parameters)) return false;

         return true;
     }

     public void copy(TriggerConfig triggerConfig) throws CloneNotSupportedException {
         enabled = triggerConfig.enabled;

         name = triggerConfig.name;
         description = triggerConfig.description;

         triggerClass = triggerConfig.triggerClass;
         jobNames = new ArrayList<String>();
         jobNames.addAll(triggerConfig.jobNames);

         parameters = new HashMap<String,String>();
         parameters.putAll(triggerConfig.parameters);
     }

     public Object clone() throws CloneNotSupportedException {
         TriggerConfig triggerConfig = (TriggerConfig)super.clone();
         triggerConfig.copy(this);
         return triggerConfig;
     }

    public Collection<String> getJobNames() {
        return jobNames;
    }

    public void addJobName(String jobName) {
        jobNames.add(jobName);
    }

    public void setJobNames(Collection<String> jobNames) {
        this.jobNames = jobNames;
    }

    public void removeJobName(String jobName) {
        jobNames.remove(jobName);
    }
}
