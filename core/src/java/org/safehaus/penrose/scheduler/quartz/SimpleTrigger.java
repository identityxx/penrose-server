package org.safehaus.penrose.scheduler.quartz;

import java.text.SimpleDateFormat;
import java.text.DateFormat;
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

        s = triggerConfig.getParameter("startTime");
        Date startTime = s == null ? null : dateFormat.parse(s);
        log.debug("Start time: "+startTime);

        s = triggerConfig.getParameter("endTime");
        Date endTime = s == null ? null : dateFormat.parse(s);
        log.debug("End time: "+endTime);

        s = triggerConfig.getParameter("count");
        int count = s == null ? 0 : Integer.parseInt(s);
        log.debug("Count: "+count);

        s = triggerConfig.getParameter("interval");
        long interval = s == null ? 0 : Long.parseLong(s);
        log.debug("Interval: "+interval);

        quartzTrigger = new org.quartz.SimpleTrigger(
                name,
                null,
                startTime,
                endTime,
                count,
                interval
        );
    }
}
