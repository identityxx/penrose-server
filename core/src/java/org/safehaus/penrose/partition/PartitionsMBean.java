package org.safehaus.penrose.partition;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface PartitionsMBean {

    public Collection<String> getPartitionNames() throws Exception;
}
