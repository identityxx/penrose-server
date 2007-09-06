package org.safehaus.penrose.scheduler;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class Scheduler {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected SchedulerConfig schedulerConfig;
    protected SchedulerContext schedulerContext;

    protected Partition partition;

    public Map<String,Job> jobs         = new LinkedHashMap<String,Job>();
    public Map<String,Trigger> triggers = new LinkedHashMap<String,Trigger>();

    public void init(SchedulerConfig schedulerConfig, SchedulerContext schedulerContext) throws Exception {

        log.debug("Initializing scheduler.");

        this.schedulerConfig = schedulerConfig;
        this.schedulerContext = schedulerContext;

        this.partition = schedulerContext.getPartition();

        for (JobConfig jobConfig : schedulerConfig.getJobConfigs()) {
            Job job = createJob(jobConfig);
            addJob(job);
        }

        for (TriggerConfig triggerConfig : schedulerConfig.getTriggerConfigs()) {
            Trigger trigger = createTrigger(triggerConfig);
            addTrigger(trigger);
        }

        init();
    }

    public void init() throws Exception {
    }
    
    public void destroy() throws Exception {
    }

    public void schedule(Job job, Trigger trigger) throws Exception {
    }

    public Job createJob(JobConfig jobConfig) throws Exception {

        String className = jobConfig.getJobClass();

        PartitionContext partitionContext = partition.getPartitionContext();
        ClassLoader classLoader = partitionContext.getClassLoader();
        Class clazz = classLoader.loadClass(className);

        Job job = (Job)clazz.newInstance();
        job.init(jobConfig);

        return job;
    }

    public void addJob(Job job) {
        jobs.put(job.getName(), job);
    }

    public Job getJob(String name) {
        return jobs.get(name);
    }

    public Collection<Job> getJobs() {
        return jobs.values();
    }

    public void removeJob(String name) {
        jobs.remove(name);
    }

    public Trigger createTrigger(TriggerConfig triggerConfig) throws Exception {

        String className = triggerConfig.getTriggerClass();

        PartitionContext partitionContext = partition.getPartitionContext();
        ClassLoader classLoader = partitionContext.getClassLoader();
        Class clazz = classLoader.loadClass(className);

        Trigger trigger = (Trigger)clazz.newInstance();
        trigger.init(triggerConfig);

        return trigger;
    }

    public void addTrigger(Trigger trigger) {
        triggers.put(trigger.getName(), trigger);
    }

    public Trigger getTrigger(String name) {
        return triggers.get(name);
    }

    public Collection<Trigger> getTriggers() {
        return triggers.values();
    }
    
    public void removeTrigger(String name) {
        triggers.remove(name);
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public SchedulerConfig getSchedulerConfig() {
        return schedulerConfig;
    }

    public void setSchedulerConfig(SchedulerConfig schedulerConfig) {
        this.schedulerConfig = schedulerConfig;
    }

    public SchedulerContext getSchedulerContext() {
        return schedulerContext;
    }

    public void setSchedulerContext(SchedulerContext schedulerContext) {
        this.schedulerContext = schedulerContext;
    }
}
