package org.safehaus.penrose.management;

import org.safehaus.penrose.module.ModuleConfig;

/**
 * @author Endi Sukma Dewata
 */
public interface ModuleServiceMBean {

    public void start() throws Exception;
    public void stop() throws Exception;
    public void restart() throws Exception;

    public ModuleConfig getModuleConfig() throws Exception;

}
