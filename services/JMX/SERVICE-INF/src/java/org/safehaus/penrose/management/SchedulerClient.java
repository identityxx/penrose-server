package org.safehaus.penrose.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SchedulerClient implements SchedulerServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseClient client;
    private String partitionName;

    MBeanServerConnection connection;
    ObjectName objectName;

    public SchedulerClient(PenroseClient client, String partitionName) throws Exception {
        this.client = client;
        this.partitionName = partitionName;

        connection = client.getConnection();
        objectName = ObjectName.getInstance(getObjectName(partitionName));
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }

    public static String getObjectName(String partitionName) {
        return "Penrose:type=scheduler,partition="+partitionName+",name=scheduler";
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public Object invoke(String method, Object[] paramValues, String[] paramClassNames) throws Exception {

        log.debug("Invoking method "+method+"().");

        return connection.invoke(
                objectName,
                method,
                paramValues,
                paramClassNames
        );
    }

    public Collection<String> getJobNames() throws Exception {
        return (Collection<String>)connection.getAttribute(objectName, "JobNames");
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
        return (Collection<String>)connection.getAttribute(objectName, "TriggerNames");
    }

}
