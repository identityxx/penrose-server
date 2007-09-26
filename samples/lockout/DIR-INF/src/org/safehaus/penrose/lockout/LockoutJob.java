package org.safehaus.penrose.lockout;

import org.safehaus.penrose.scheduler.Job;

/**
 * @author Endi Sukma Dewata
 */
public class LockoutJob extends Job {

    public final static String MODULE = "module";
    public final static String DEFAULT_MODULE = "LockoutModule";

    public LockoutModule module;

    public void init() throws Exception {
        String s = getParameter(MODULE);
        module = (LockoutModule)partition.getModule(s == null ? DEFAULT_MODULE : s);
    }

    public void execute() throws Exception {
        module.purge();
    }
}
