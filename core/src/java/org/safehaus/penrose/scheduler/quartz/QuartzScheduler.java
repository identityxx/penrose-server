package org.safehaus.penrose.scheduler.quartz;

import org.quartz.*;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.impl.StdSchedulerFactory;
import org.safehaus.penrose.scheduler.Job;
import org.safehaus.penrose.scheduler.Scheduler;
import org.safehaus.penrose.scheduler.Trigger;
import org.safehaus.penrose.scheduler.TriggerConfig;

/**
 * @author Endi Sukma Dewata
 */
public class QuartzScheduler extends Scheduler implements JobFactory {

    org.quartz.Scheduler scheduler;

    public void init() throws Exception {
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        scheduler = schedulerFactory.getScheduler();
        scheduler.start();

        scheduler.setJobFactory(this);

        for (Trigger trigger : getTriggers()) {

            Job job = jobs.get(trigger.getJobName());

            schedule(job, trigger);
        }
    }

    public void destroy() throws Exception {
        scheduler.shutdown();
    }

    public void schedule(Job job, Trigger trigger) throws Exception {

        JobDetail jobDetail = new JobDetail(
                job.getName(),
                null,
                QuartzJob.class
        );
        
        org.quartz.Trigger quartzTrigger = ((QuartzTrigger)trigger).getQuartzTrigger();

        scheduler.scheduleJob(jobDetail, quartzTrigger);
    }

    public org.quartz.Job newJob(TriggerFiredBundle triggerFiredBundle) throws SchedulerException {
        JobDetail jobDetail = triggerFiredBundle.getJobDetail();

        String jobName = jobDetail.getName();
        Job job = getJob(jobName);

        QuartzJob quartzJob = new QuartzJob();
        quartzJob.setJob(job);

        return quartzJob;
    }
}
