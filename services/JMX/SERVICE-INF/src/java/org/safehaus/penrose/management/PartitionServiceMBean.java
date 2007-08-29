package org.safehaus.penrose.management;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface PartitionServiceMBean {

    public String getStatus() throws Exception;
    public void start() throws Exception;
    public void stop() throws Exception;
    
    public Collection<String> getConnectionNames() throws Exception;
    public Collection<String> getSourceNames() throws Exception;
    public Collection<String> getModuleNames() throws Exception;
}
