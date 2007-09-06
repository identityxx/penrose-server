package org.safehaus.penrose.example.scheduler;

import org.safehaus.penrose.scheduler.Job;

/**
 * @author Endi Sukma Dewata
 */
public class DemoJob extends Job {

    public void init() throws Exception {
        log.debug("Initializing DemoJob.");
    }

    public void execute() throws Exception {
        log.debug("Executing DemoJob.");
    }
}
