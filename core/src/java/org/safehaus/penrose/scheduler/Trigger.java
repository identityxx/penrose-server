package org.safehaus.penrose.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class Trigger {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected TriggerConfig triggerConfig;
    protected TriggerContext triggerContext;

    public void init(TriggerConfig triggerConfig, TriggerContext triggerContext) throws Exception {
        this.triggerConfig = triggerConfig;
        this.triggerContext = triggerContext;

        log.debug("Initializing "+triggerConfig.getName()+" trigger.");

        init();
    }

    public void init() throws Exception {

    }

    public String getName() {
        return triggerConfig.getName();
    }

    public Collection<String> getJobNames() {
        return triggerConfig.getJobNames();
    }
    
    public TriggerConfig getTriggerConfig() {
        return triggerConfig;
    }

    public void setTriggerConfig(TriggerConfig triggerConfig) {
        this.triggerConfig = triggerConfig;
    }

    public TriggerContext getTriggerContext() {
        return triggerContext;
    }

    public void setTriggerContext(TriggerContext triggerContext) {
        this.triggerContext = triggerContext;
    }
}
