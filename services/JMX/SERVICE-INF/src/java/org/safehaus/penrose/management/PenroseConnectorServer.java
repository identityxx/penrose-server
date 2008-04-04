package org.safehaus.penrose.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import java.io.IOException;
import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public class PenroseConnectorServer extends RMIConnectorServer {
    
    public Logger log = LoggerFactory.getLogger(getClass());

    public PenroseConnectorServer(JMXServiceURL serviceURL, Map<String,?> environment, MBeanServer mbeanServer) throws IOException {
        super(serviceURL, environment, mbeanServer);
    }

    public  void connectionOpened(String connectionId, String message, Object userData) {
        log.debug("Connection "+connectionId+" opened: "+message);
    }
    
    public  void connectionClosed(String connectionId, String message, Object userData) {
        log.debug("Connection "+connectionId+" closed: "+message);
    }

    public  void connectionFailed(String connectionId, String message, Object userData) {
        log.debug("Connection "+connectionId+" failed: "+message);
    }
}
