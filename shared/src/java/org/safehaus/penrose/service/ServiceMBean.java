package org.safehaus.penrose.service;

import org.safehaus.penrose.source.SourceCounter;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface ServiceMBean {

    public String getName() throws Exception;
    public String getServiceClass() throws Exception;
    public String getDescription() throws Exception;

    public Collection getParameterNames() throws Exception;
    public String getParameter(String name) throws Exception;
    public void setParameter(String name, String value) throws Exception;
    public String removeParameter(String name) throws Exception;

    public ServiceConfig getServiceConfig() throws Exception;
    public String getStatus() throws Exception;

    public void start() throws Exception;
    public void stop() throws Exception;
    public void restart() throws Exception;
}
