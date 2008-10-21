package org.safehaus.penrose.module;

import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleMapping;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface ModuleServiceMBean {

    public void start() throws Exception;
    public void stop() throws Exception;
    public void restart() throws Exception;

    public ModuleConfig getModuleConfig() throws Exception;
    public Collection<String> getParameterNames() throws Exception;
    public String getParameter(String name) throws Exception;

    public void addModuleMapping(ModuleMapping moduleMapping) throws Exception;
    public void removeModuleMapping(ModuleMapping moduleMapping) throws Exception;
    public Collection<ModuleMapping> getModuleMappings() throws Exception;
}
