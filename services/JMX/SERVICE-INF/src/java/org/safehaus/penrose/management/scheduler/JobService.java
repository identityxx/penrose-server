package org.safehaus.penrose.management.scheduler;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.scheduler.Job;
import org.safehaus.penrose.scheduler.JobConfig;
import org.safehaus.penrose.scheduler.Scheduler;
import org.safehaus.penrose.scheduler.SchedulerConfig;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;

/**
 * @author Endi Sukma Dewata
 */
public class JobService extends BaseService implements JobServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;
    private String jobName;

    public JobService(
            PenroseJMXService jmxService,
            PartitionManager partitionManager,
            String partitionName,
            String jobName
    ) {
        super(JobServiceMBean.class);

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
        this.jobName = jobName;
    }

    public String getObjectName() {
        return JobClient.getStringObjectName(partitionName, jobName);
    }

    public Object getObject() {
        return getJob();
    }

    public JobConfig getJobConfig() throws Exception {
        return getSchedulerConfig().getJobConfig(jobName);
    }

    public Job getJob() {
        return getScheduler().getJob(jobName);
    }

    public SchedulerConfig getSchedulerConfig() {
        return getPartitionConfig().getSchedulerConfig();
    }

    public Scheduler getScheduler() {
        return getPartition().getScheduler();
    }

    public PartitionConfig getPartitionConfig() {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public Partition getPartition() {
        return partitionManager.getPartition(partitionName);
    }

    public void start() throws Exception {

        log.debug("Starting job "+partitionName+"/"+jobName+"...");

        Job job = getJob();
        job.init();

        log.debug("Job started.");
    }

    public void stop() throws Exception {

        log.debug("Stopping job "+partitionName+"/"+jobName+"...");

        Job job = getJob();
        job.destroy();

        log.debug("Job stopped.");
    }

    public void restart() throws Exception {

        log.debug("Restarting job "+partitionName+"/"+jobName+"...");

        Job job = getJob();
        job.destroy();
        job.init();

        log.debug("Module restarted.");
    }
}
