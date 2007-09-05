package org.safehaus.penrose.management;

import org.safehaus.penrose.module.ModuleConfig;

/**
 * @author Endi Sukma Dewata
 */
public interface ModuleServiceMBean {

    public ModuleConfig getModuleConfig() throws Exception;

}
