package org.safehaus.penrose.management.scheduler;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.scheduler.Job;
import org.safehaus.penrose.scheduler.Scheduler;
import org.safehaus.penrose.scheduler.SchedulerConfig;
import org.safehaus.penrose.scheduler.SchedulerClient;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SchedulerService extends BaseService implements SchedulerServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PartitionManager partitionManager;
    private String partitionName;

    public SchedulerService(
            PenroseJMXService jmxService,
            PartitionManager partitionManager,
            String partitionName
    ) throws Exception {
        super(SchedulerServiceMBean.class);

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
    }

    public String getObjectName() {
        return SchedulerClient.getStringObjectName(partitionName);
    }

    public Object getObject() {
        return getScheduler();
    }

    public SchedulerConfig getSchedulerConfig() {
        PartitionConfig partitionConfig = getPartitionConfig();
        if (partitionConfig == null) return null;
        return partitionConfig.getSchedulerConfig();
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

    public void register() throws Exception {
        super.register();

        SchedulerConfig schedulerConfig = getSchedulerConfig();
        if (schedulerConfig != null) {
            for (String jobName : schedulerConfig.getJobNames()) {
                JobService jobService = getJobService(jobName);
                jobService.register();
            }
        }
    }

    public void unregister() throws Exception {

        SchedulerConfig schedulerConfig = getSchedulerConfig();
        if (schedulerConfig != null) {
            for (String jobName : schedulerConfig.getJobNames()) {
                JobService jobService = getJobService(jobName);
                jobService.unregister();
            }
        }

        super.unregister();
    }

    public Collection<String> getJobNames() throws Exception {
        Collection<String> list = new ArrayList<String>();

        SchedulerConfig schedulerConfig = getSchedulerConfig();
        if (schedulerConfig != null) {
            list.addAll(schedulerConfig.getJobNames());
        }

        return list;
    }

    public Collection<String> getTriggerNames() throws Exception {
        Collection<String> list = new ArrayList<String>();

        SchedulerConfig schedulerConfig = getSchedulerConfig();
        if (schedulerConfig != null) {
            list.addAll(schedulerConfig.getTriggerNames());
        }

        return list;
    }

    public JobService getJobService(String jobName) throws Exception {

        JobService jobService = new JobService(jmxService, partitionManager, partitionName, jobName);
        jobService.init();

        return jobService;
    }

    public void executeJob(String name) throws Exception {

        log.debug("Executing job "+name);

        Scheduler scheduler = getScheduler();
        Job job = scheduler.getJob(name);
        job.execute();
    }
}
