package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.scheduler.Job;
import org.safehaus.penrose.scheduler.JobConfig;

/**
 * @author Endi Sukma Dewata
 */
public class JobService extends JMXService implements JobServiceMBean {

    private Partition partition;
    private Job job;

    public JobService(Job job) {
        super(job);

        this.job = job;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
        setObject(job);
    }

    public JobConfig getJobConfig() throws Exception {
        return job.getJobConfig();
    }

    public String getObjectName() {
        return JobClient.getObjectName(partition.getName(), job.getName());
    }

    public void start() throws Exception {
        log.debug("Starting job "+partition.getName()+"/"+job.getName()+"...");
        job.init();
        log.debug("Job started.");
    }

    public void stop() throws Exception {
        log.debug("Stopping job "+partition.getName()+"/"+job.getName()+"...");
        job.destroy();
        log.debug("Job stopped.");
    }

    public void restart() throws Exception {
        log.debug("Restarting job "+partition.getName()+"/"+job.getName()+"...");
        job.destroy();
        job.init();
        log.debug("Module restarted.");
    }
}
