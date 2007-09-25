package org.safehaus.penrose.scheduler.quartz;

import org.quartz.Trigger;
import org.safehaus.penrose.partition.Partition;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Endi Sukma Dewata
 */
public class SimpleTrigger extends QuartzTrigger {

    public void init() throws Exception {

        String name = triggerConfig.getName();

        String s = triggerConfig.getParameter("dateFormat");
        SimpleDateFormat dateFormat = s == null ? new SimpleDateFormat() : new SimpleDateFormat(s);
        log.debug("Date format: "+dateFormat.toPattern());

        s = triggerConfig.getParameter("delay");
        long delay = s == null ? 0 : Long.parseLong(s);
        log.debug("Delay: "+delay);

        s = triggerConfig.getParameter("startTime");
        Date startTime = s == null ? new Date(System.currentTimeMillis() + delay * 1000) : dateFormat.parse(s);
        log.debug("Start time: "+startTime);

        s = triggerConfig.getParameter("endTime");
        Date endTime = s == null ? null : dateFormat.parse(s);
        log.debug("End time: "+endTime);

        s = triggerConfig.getParameter("count");
        Integer count = s == null ? null : Integer.parseInt(s);
        log.debug("Count: "+count);

        s = triggerConfig.getParameter("interval");
        long interval = s == null ? 0 : Long.parseLong(s);
        log.debug("Interval: "+interval);

        Partition partition = triggerContext.getPartition();

        org.quartz.SimpleTrigger simpleTrigger;

        if (count == null) { // schedule one execution
            simpleTrigger = new org.quartz.SimpleTrigger(
                    name,
                    partition.getName(),
                    startTime
            );

        } else { // schedule multiple executions
            simpleTrigger = new org.quartz.SimpleTrigger(
                    name,
                    partition.getName(),
                    startTime,
                    endTime,
                    count,
                    interval * 1000
            );

            simpleTrigger.setMisfireInstruction(org.quartz.SimpleTrigger.MISFIRE_INSTRUCTION_RESCHEDULE_NOW_WITH_EXISTING_REPEAT_COUNT);
        }

        quartzTrigger = simpleTrigger;
    }
}
