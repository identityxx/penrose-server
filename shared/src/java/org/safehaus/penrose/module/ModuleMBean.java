package org.safehaus.penrose.module;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface ModuleMBean {

    public String getName() throws Exception;
    public ModuleConfig getModuleConfig() throws Exception;

    public String getParameter(String name) throws Exception;
    public Collection getParameterNames() throws Exception;

    public void start() throws Exception;
    public void stop() throws Exception;
    public void restart() throws Exception;

    public String getStatus() throws Exception;
}
