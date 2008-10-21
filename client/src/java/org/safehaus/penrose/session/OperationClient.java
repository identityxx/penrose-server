package org.safehaus.penrose.session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.client.PenroseClient;

/**
 * @author Endi Sukma Dewata
 */
public class OperationClient extends BaseClient implements OperationServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    public OperationClient(PenroseClient client, String sessionName, String operationName) throws Exception {
        super(client, operationName, getStringObjectName(sessionName, operationName));
    }

    public void abandon() throws Exception {
        invoke("abandon");
    }

    public static String getStringObjectName(String sessionName, String operationName) {
        return "Penrose:type=operation,session="+sessionName+",name="+operationName;
    }
}