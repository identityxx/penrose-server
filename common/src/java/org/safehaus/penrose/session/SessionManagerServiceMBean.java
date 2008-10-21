package org.safehaus.penrose.session;

import org.safehaus.penrose.service.ServiceConfig;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface SessionManagerServiceMBean {

    public Collection<String> getSessionNames() throws Exception;
    public Collection<String> getOperationNames(String sessionName) throws Exception;

    public void closeSession(String sessionName) throws Exception;
    public void abandonOperation(String sessionName, String operationName) throws Exception;
}