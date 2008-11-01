package org.safehaus.penrose.source;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface SourceManagerServiceMBean {

    public Collection<String> getSourceNames() throws Exception;
    public void createSource(SourceConfig sourceConfig) throws Exception;
    public void updateSource(String name, SourceConfig sourceConfig) throws Exception;
    public void removeSource(String name) throws Exception;
}
