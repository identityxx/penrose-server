package org.safehaus.penrose.scheduler.quartz;

import org.safehaus.penrose.scheduler.Trigger;

/**
 * @author Endi Sukma Dewata
 */
public class QuartzTrigger extends Trigger {
    
    public org.quartz.Trigger quartzTrigger;

    public org.quartz.Trigger getQuartzTrigger() {
        return quartzTrigger;
    }

    public void setQuartzTrigger(org.quartz.Trigger quartzTrigger) {
        this.quartzTrigger = quartzTrigger;
    }
}
