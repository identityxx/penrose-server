package org.safehaus.penrose.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.scheduler.Scheduler;
import org.safehaus.penrose.scheduler.Job;

import javax.management.StandardMBean;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class SchedulerService extends StandardMBean implements SchedulerServiceMBean {

    Logger log = LoggerFactory.getLogger(getClass());

    private PenroseJMXService jmxService;
    private Partition partition;
    private Scheduler scheduler;

    public SchedulerService() throws Exception {
        super(SchedulerServiceMBean.class);
    }

    public PenroseJMXService getJmxService() {
        return jmxService;
    }

    public void setJmxService(PenroseJMXService jmxService) {
        this.jmxService = jmxService;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public String getObjectName() {
        return SchedulerClient.getObjectName(partition.getName());
    }

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);

        for (JobService jobService : getJobServices()) {
            jobService.register();
        }
    }

    public void unregister() throws Exception {
        for (JobService jobService : getJobServices()) {
            jobService.unregister();
        }

        jmxService.unregister(getObjectName());
    }

    public Collection<String> getJobNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(scheduler.getJobNames());
        return list;
    }

    public JobService getJobService(String jobName) throws Exception {
        Job job = scheduler.getJob(jobName);
        if (job== null) return null;

        return getJobService(job);
    }

    public JobService getJobService(Job job) throws Exception {
        JobService jobService = new JobService(partition, job);
        jobService.setJmxService(jmxService);

        return jobService;
    }

    public Collection<JobService> getJobServices() throws Exception {

        Collection<JobService> list = new ArrayList<JobService>();

        for (Job job : scheduler.getJobs()) {
            list.add(getJobService(job));
        }

        return list;
    }

    public void executeJob(String name) throws Exception {

        log.debug("Executing job "+name);

        Job job = scheduler.getJob(name);
        job.execute();
    }

    public Collection<String> getTriggerNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(scheduler.getTriggerNames());
        return list;
    }
}
