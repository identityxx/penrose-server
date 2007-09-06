package org.safehaus.penrose.scheduler;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.Map;
import java.util.Collection;
import java.util.LinkedHashMap;

/**
 * @author Endi Sukma Dewata
 */
public class SchedulerConfig implements Serializable, Cloneable {

    private boolean enabled = true;

    private String description;

    private String schedulerClass;
    private Map<String,String> parameters = new LinkedHashMap<String,String>();

    private Map<String,JobConfig> jobConfigs = new LinkedHashMap<String,JobConfig>();
    private Map<String,TriggerConfig> triggerConfigs = new LinkedHashMap<String,TriggerConfig>();

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

    public String getSchedulerClass() {
        return schedulerClass;
    }

    public void setSchedulerClass(String schedulerClass) {
        this.schedulerClass = schedulerClass;
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

    public void addJobConfig(JobConfig jobConfig) {
        jobConfigs.put(jobConfig.getName(), jobConfig);
    }

    public JobConfig getJobConfig(String name) {
        return jobConfigs.get(name);
    }

    public Collection<JobConfig> getJobConfigs() {
        return jobConfigs.values();
    }

    public JobConfig removeJobConfig(String name) {
        return jobConfigs.remove(name);
    }

    public void addTriggerConfig(TriggerConfig triggerConfig) {
        triggerConfigs.put(triggerConfig.getName(), triggerConfig);
    }

    public TriggerConfig getTriggerConfig(String name) {
        return triggerConfigs.get(name);
    }

    public Collection<TriggerConfig> getTriggerConfigs() {
        return triggerConfigs.values();
    }
    
    public TriggerConfig removeTriggerConfig(String name) {
        return triggerConfigs.remove(name);
    }

    public int hashCode() {
         return schedulerClass == null ? 0 : schedulerClass.hashCode();
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

         SchedulerConfig schedulerConfig = (SchedulerConfig)object;
         if (enabled != schedulerConfig.enabled) return false;
         if (!equals(description, schedulerConfig.description)) return false;
         if (!equals(schedulerClass, schedulerConfig.schedulerClass)) return false;

         if (!equals(parameters, schedulerConfig.parameters)) return false;

         if (!equals(jobConfigs, schedulerConfig.jobConfigs)) return false;
         if (!equals(triggerConfigs, schedulerConfig.triggerConfigs)) return false;

         return true;
     }

     public void copy(SchedulerConfig schedulerConfig) throws CloneNotSupportedException {
         enabled = schedulerConfig.enabled;
         description = schedulerConfig.description;
         schedulerClass = schedulerConfig.schedulerClass;

         parameters = new LinkedHashMap<String,String>();
         parameters.putAll(schedulerConfig.parameters);

         jobConfigs = new LinkedHashMap<String,JobConfig>();
         for (JobConfig jobConfig : schedulerConfig.jobConfigs.values()) {
             jobConfigs.put(jobConfig.getName(), (JobConfig)jobConfig.clone());
         }

         triggerConfigs = new LinkedHashMap<String,TriggerConfig>();
         for (TriggerConfig triggerConfig : schedulerConfig.triggerConfigs.values()) {
             triggerConfigs.put(triggerConfig.getName(), (TriggerConfig)triggerConfig.clone());
         }
     }

     public Object clone() throws CloneNotSupportedException {
         SchedulerConfig schedulerConfig = (SchedulerConfig)super.clone();
         schedulerConfig.copy(this);
         return schedulerConfig;
     }
}
