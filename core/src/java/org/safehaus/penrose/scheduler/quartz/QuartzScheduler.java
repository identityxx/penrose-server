package org.safehaus.penrose.scheduler.quartz;

import org.safehaus.penrose.scheduler.Job;
import org.safehaus.penrose.scheduler.Scheduler;
import org.safehaus.penrose.scheduler.Trigger;

/**
 * @author Endi Sukma Dewata
 */
public class QuartzScheduler extends Scheduler implements org.quartz.spi.JobFactory {

    org.quartz.Scheduler scheduler;

    public void init() throws Exception {
        org.quartz.SchedulerFactory schedulerFactory = new org.quartz.impl.StdSchedulerFactory();
        scheduler = schedulerFactory.getScheduler();
        scheduler.start();

        scheduler.setJobFactory(this);

        for (Trigger trigger : getTriggers()) {
            schedule(trigger);
        }
    }

    public void destroy() throws Exception {
        scheduler.shutdown();
    }

    public void schedule(Trigger trigger) throws Exception {

        org.quartz.JobDetail jobDetail = new org.quartz.JobDetail(
                trigger.getName(),
                partition.getName(),
                QuartzJob.class
        );
        
        org.quartz.Trigger quartzTrigger = ((QuartzTrigger)trigger).getQuartzTrigger();

        scheduler.scheduleJob(jobDetail, quartzTrigger);
    }

    public org.quartz.Job newJob(
            org.quartz.spi.TriggerFiredBundle triggerFiredBundle
    ) throws org.quartz.SchedulerException {

        org.quartz.JobDetail jobDetail = triggerFiredBundle.getJobDetail();

        Trigger trigger = getTrigger(jobDetail.getName());
        QuartzJob quartzJob = new QuartzJob();

        for (String jobName : trigger.getJobNames()) {
            Job job = getJob(jobName);
            if (job == null) continue;
            quartzJob.addJob(job);
        }

        return quartzJob;
    }
}
