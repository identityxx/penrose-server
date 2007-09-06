package org.safehaus.penrose.scheduler.quartz;

import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;

/**
 * @author Endi Sukma Dewata
 */
public class CronTrigger extends QuartzTrigger {

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

        String expression = triggerConfig.getParameter("expression");
        log.debug("Expression: "+expression);
        
        quartzTrigger = new org.quartz.CronTrigger(name, null, expression);
    }
}
