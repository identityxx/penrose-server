package org.safehaus.penrose.module;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface ModuleManagerServiceMBean {

    public Collection<String> getModuleNames() throws Exception;
    public void createModule(ModuleConfig moduleConfig) throws Exception;
    public void createModule(ModuleConfig moduleConfig, Collection<ModuleMapping> moduleMappings) throws Exception;
    public void updateModule(String name, ModuleConfig moduleConfig) throws Exception;
    public void removeModule(String name) throws Exception;
}
