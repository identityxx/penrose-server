package org.safehaus.penrose.scheduler.quartz;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.safehaus.penrose.scheduler.Job;

/**
 * @author Endi Sukma Dewata
 */
public class QuartzJob implements org.quartz.Job {

    protected Job job;

    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            job.execute();

        } catch (Exception e) {
            throw new JobExecutionException(e);
        }
    }

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
    }
}
