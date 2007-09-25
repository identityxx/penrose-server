package org.safehaus.penrose.scheduler.quartz;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.safehaus.penrose.scheduler.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class QuartzJob implements org.quartz.Job {

    Logger log = LoggerFactory.getLogger(getClass());

    private Collection<Job> jobs = new ArrayList<Job>();

    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            for (Job job : jobs) {
                job.execute();
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new JobExecutionException(e);
        }
    }

    public Collection<Job> getJobs() {
        return jobs;
    }

    public void addJob(Job job) {
        jobs.add(job);
    }
    
    public void setJobs(Collection<Job> jobs) {
        this.jobs = jobs;
    }
}
