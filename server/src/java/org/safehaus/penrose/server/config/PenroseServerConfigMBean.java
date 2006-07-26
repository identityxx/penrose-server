package org.safehaus.penrose.server.config;

import java.util.Map;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface PenroseServerConfigMBean {

    public String getHome() throws Exception;
    public void setHome(String home) throws Exception;

    public Map getSystemProperties() throws Exception;
    public String getSystemProperty(String name) throws Exception;
    public void setSystemProperty(String name, String value) throws Exception;
    public String removeSystemProperty(String name) throws Exception;

    public Collection getServiceNames() throws Exception;
}
