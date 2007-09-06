package org.safehaus.penrose.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endi Sukma Dewata
 */
public class Trigger {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected TriggerConfig triggerConfig;

    public void init(TriggerConfig triggerConfig) throws Exception {
        this.triggerConfig = triggerConfig;

        log.debug("Initializing "+triggerConfig.getName()+" trigger.");

        init();
    }

    public void init() throws Exception {

    }

    public String getName() {
        return triggerConfig.getName();
    }

    public String getJobName() {
        return triggerConfig.getJobName();
    }
    
    public TriggerConfig getTriggerConfig() {
        return triggerConfig;
    }

    public void setTriggerConfig(TriggerConfig triggerConfig) {
        this.triggerConfig = triggerConfig;
    }
}
