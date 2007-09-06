package org.safehaus.penrose.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endi Sukma Dewata
 */
public class Job {

    public Logger log = LoggerFactory.getLogger(getClass());

    public JobConfig jobConfig;

    public void init(JobConfig jobConfig) throws Exception {
        this.jobConfig = jobConfig;

        log.debug("Initializing "+jobConfig.getName()+" job.");
        
        init();
    }

    public void init() throws Exception {
    }

    public void execute() throws Exception {
    }

    public String getName() {
        return jobConfig.getName();
    }

    public JobConfig getJobConfig() {
        return jobConfig;
    }

    public void setJobConfig(JobConfig jobConfig) {
        this.jobConfig = jobConfig;
    }
}
