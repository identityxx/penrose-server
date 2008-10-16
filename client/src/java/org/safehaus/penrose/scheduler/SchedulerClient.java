package org.safehaus.penrose.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.management.BaseClient;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.management.scheduler.SchedulerServiceMBean;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SchedulerClient extends BaseClient implements SchedulerServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected String partitionName;

    public SchedulerClient(PenroseClient client, String partitionName) throws Exception {
        super(client, "scheduler", getStringObjectName(partitionName));

        this.partitionName = partitionName;
    }

    public static String getStringObjectName(String partitionName) {
        return "Penrose:type=scheduler,partition="+partitionName+",name=scheduler";
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public Collection<String> getJobNames() throws Exception {
        return (Collection<String>)getAttribute("JobNames");
    }

    public JobClient getJobClient(String jobName) throws Exception {
        return new JobClient(client, partitionName, jobName);
    }

    public void executeJob(String name) throws Exception {
        invoke(
                "executeJob",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public Collection<String> getTriggerNames() throws Exception {
        return (Collection<String>)getAttribute("TriggerNames");
    }

}
