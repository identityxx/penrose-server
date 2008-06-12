package org.safehaus.penrose.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class Monitor {

    public Logger log = LoggerFactory.getLogger(getClass());

    public MonitorConfig monitorConfig;
    public MonitorContext monitorContext;

    public Monitor() {
    }

    public void init(MonitorConfig monitorConfig, MonitorContext monitorContext) throws Exception {
        this.monitorConfig = monitorConfig;
        this.monitorContext = monitorContext;

        init();
    }

    public void init() throws Exception {
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public String getName() {
        return monitorConfig.getName();
    }
    
    public String getParameter(String name) throws Exception {
        return monitorConfig.getParameter(name);
    }

    public Collection<String> getParameterNames() throws Exception {
        return monitorConfig.getParameterNames();
    }
}
