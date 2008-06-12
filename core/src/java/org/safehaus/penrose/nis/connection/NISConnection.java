package org.safehaus.penrose.nis.connection;

import org.safehaus.penrose.nis.*;
import org.safehaus.penrose.connection.Connection;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class NISConnection extends Connection {

    public NISClient client;

    public NISConnection() throws Exception {
    }

    public void init() throws Exception {

        Map<String,String> parameters = getParameters();

        String method = parameters.get(NIS.METHOD);
        if (method == null) method = NIS.DEFAULT_METHOD;

        if (NIS.LOCAL.equals(method)) {
            client = new NISLocalClient();

        } else if (NIS.YP.equals(method)) {
            client = new NISYPClient();

        } else { // if (METHOD_JNDI.equals(method)) {
            client = new NISJNDIClient();
        }

        client.init(parameters);
    }
}