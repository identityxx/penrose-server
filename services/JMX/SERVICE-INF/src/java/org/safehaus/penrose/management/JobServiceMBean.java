package org.safehaus.penrose.management;

import org.safehaus.penrose.scheduler.JobConfig;

/**
 * @author Endi Sukma Dewata
 */
public interface JobServiceMBean {

    public JobConfig getJobConfig() throws Exception;

}
