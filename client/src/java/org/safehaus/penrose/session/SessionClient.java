package org.safehaus.penrose.session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.client.PenroseClient;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SessionClient extends BaseClient implements SessionServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    public SessionClient(PenroseClient client, String sessionName) throws Exception {
        super(client, sessionName, getStringObjectName(sessionName));
    }

    public OperationClient getOperationClient(String operationName) throws Exception {
        return new OperationClient(client, name, operationName);
    }

    public Collection<String> getOperationNames() throws Exception {
        return (Collection<String>)getAttribute("OperationNames");
    }

    public void abandon(String operationName) throws Exception {
        invoke("abandon", new Object[] { operationName }, new String[] { String.class.getName() });
    }

    public static String getStringObjectName(String sessionName) {
        return "Penrose:type=session,name="+sessionName;
    }
}