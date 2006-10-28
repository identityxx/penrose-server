package org.safehaus.penrose.source;

import org.safehaus.penrose.source.SourceConfig;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface SourceMBean {

    public String getName() throws Exception;
    public SourceConfig getSourceConfig() throws Exception;
    public String getStatus() throws Exception;
    public SourceCounter getCounter() throws Exception;

    public void start() throws Exception;
    public void stop() throws Exception;
    public void restart() throws Exception;

    public Collection getParameterNames() throws Exception;
    public String getParameter(String name) throws Exception;
    public void setParameter(String name, String value) throws Exception;
    public String removeParameter(String name) throws Exception;
}
