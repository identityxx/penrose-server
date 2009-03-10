package org.safehaus.penrose.module;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface ModuleManagerServiceMBean {

    public Collection<String> getModuleNames() throws Exception;
    public void createModule(ModuleConfig moduleConfig) throws Exception;
    public void updateModule(String moduleName, ModuleConfig moduleConfig) throws Exception;
    public void removeModule(String moduleName) throws Exception;
}
