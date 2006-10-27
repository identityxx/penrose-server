package org.safehaus.penrose.source;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface SourceManagerMBean {

    public final static String NAME = "Penrose:name=SourceManager";

    public Collection getPartitionNames() throws Exception;
    public Collection getSourceNames(String partitionName) throws Exception;
    public String getStatus(String partitionName, String sourceName) throws Exception;

    public void start(String partitionName, String sourceName) throws Exception;
    public void stop(String partitionName, String sourceName) throws Exception;
    public void restart(String partitionName, String sourceName) throws Exception;
}
