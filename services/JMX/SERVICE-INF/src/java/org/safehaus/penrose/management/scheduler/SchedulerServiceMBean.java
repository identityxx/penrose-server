package org.safehaus.penrose.management.scheduler;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface SchedulerServiceMBean {

    public Collection<String> getJobNames() throws Exception;
    public void executeJob(String name) throws Exception;

    public Collection<String> getTriggerNames() throws Exception;
}
