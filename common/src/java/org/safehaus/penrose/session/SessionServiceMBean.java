package org.safehaus.penrose.session;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface SessionServiceMBean {

    public Collection<String> getOperationNames() throws Exception;
    public void abandon(String operationName) throws Exception;
}