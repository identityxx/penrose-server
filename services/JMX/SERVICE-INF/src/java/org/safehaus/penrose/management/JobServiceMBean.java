package org.safehaus.penrose.management;

import org.safehaus.penrose.scheduler.JobConfig;

/**
 * @author Endi Sukma Dewata
 */
public interface JobServiceMBean {

    public void start() throws Exception;
    public void stop() throws Exception;
    public void restart() throws Exception;

    public JobConfig getJobConfig() throws Exception;

}
