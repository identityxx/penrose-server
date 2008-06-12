package org.safehaus.penrose.monitor.management;

import org.safehaus.penrose.monitor.Monitor;

import javax.management.*;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryUsage;
import java.text.NumberFormat;
import java.util.Hashtable;

/**
 * @author Endi Sukma Dewata
 */
public class JMXMonitor extends Monitor implements NotificationListener, NotificationFilter {

    public final static int DEFAULT_PORT            = 1099;
    public final static int DEFAULT_TRANSPORT_PORT  = 0;

    public final static String URL            = "url";
    public final static String PROTOCOL       = "protocol";
    public final static String HOST           = "host";
    public final static String PORT           = "port";
    public final static String TRANSPORT_PORT = "transportPort";

    public final static String BIND_DN        = "bindDn";
    public final static String BIND_PASSWORD  = "bindPassword";

    public final static String OBJECT_NAME    = "objectName";

    public String url;
    public String protocol;
    public String host;
    public int port;
    public int transportPort;

    public ObjectName memoryMBean;
    public MBeanServerConnection connection;

    public String bindDn;
    public String bindPassword;

    NumberFormat percentFormat = NumberFormat.getPercentInstance();

    public void init() throws Exception {
        
        super.init();

        url = getParameter(URL);

        if (url == null) {
            protocol = getParameter(PROTOCOL);
            host = getParameter(HOST);
            port = Integer.parseInt(getParameter(PORT));
            transportPort = Integer.parseInt(getParameter(TRANSPORT_PORT));

            url = "service:jmx:"+protocol+"://"+host;
            if (transportPort != DEFAULT_TRANSPORT_PORT) url += ":"+ transportPort;

            url += "/jndi/"+protocol+"://"+host;
            if (port != DEFAULT_PORT) url += ":"+port;

            url += "/penrose";

            // url = "service:jmx:rmi://localhost:rmiTransportProtocol/jndi/rmi://localhost:rmiProtocol/penrose";
        }

        bindDn = getParameter(BIND_DN);
        log.debug("Bind DN: "+bindDn);

        bindPassword = getParameter(BIND_PASSWORD);
        log.debug("Password: "+bindPassword);

        memoryMBean = ObjectName.getInstance("java.lang:type=Memory");

        Hashtable<String,Object> parameters = new Hashtable<String,Object>();

        if (bindDn != null && bindPassword != null) {
            log.debug("Binding as "+bindDn+".");

            String[] credentials = new String[] {
                    bindDn,
                    bindPassword
            };

            parameters.put(JMXConnector.CREDENTIALS, credentials);
        }

        JMXServiceURL serviceURL = new JMXServiceURL(url);
        JMXConnector connector = JMXConnectorFactory.connect(serviceURL, parameters);

        connection = connector.getMBeanServerConnection();
        connection.addNotificationListener(memoryMBean, this, null, null);
    }

    public void validate() throws Exception {

        try {
            log.debug("Connecting to "+url+".");

/*
            log.debug("Notifications:");
            MBeanInfo memoryMBeanInfo = connection.getMBeanInfo(memoryMBean);
            for (MBeanNotificationInfo notificationInfo : memoryMBeanInfo.getNotifications()) {
                log.debug(" - "+notificationInfo.getName());
                log.debug("   "+notificationInfo.getDescription());
                for (String notifType : notificationInfo.getNotifTypes()) {
                    log.debug("   - "+notifType);
                }
            }

            CompositeDataSupport heapMemoryUsage = (CompositeDataSupport)connection.getAttribute(memoryMBean, "HeapMemoryUsage");

            Long maxMemory = (Long)heapMemoryUsage.get("max");
            long maxMB = maxMemory/(1024*1024);

            Long usedMemory = (Long)heapMemoryUsage.get("used");
            long usedMB = usedMemory/(1024*1024);
            double percentUsed = (double)usedMemory/maxMemory;

            log.debug("Used memory : "+usedMB+"/"+maxMB+" MB ("+ percentFormat.format(percentUsed)+")");

            connector.close();
*/
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void handleNotification(Notification notification, Object handback) {

        log.warn("================================================================================");

        String message = notification.getMessage();
        log.warn("Message: "+message);

        String type = notification.getType();
        log.warn("Type: "+type);

        if (type.equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED) ||
            type.equals(MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED)) {

            CompositeData cd = (CompositeData)notification.getUserData();
            MemoryNotificationInfo info = MemoryNotificationInfo.from(cd);

            String poolName = info.getPoolName();
            log.warn("Pool Name: "+poolName);

            long count = info.getCount();
            log.warn("Count: "+count);

            MemoryUsage usage = info.getUsage();

            long maxMemory = usage.getMax();
            long maxMB = maxMemory/(1024*1024);

            long usedMemory = usage.getUsed();
            long usedMB = usedMemory/(1024*1024);

            double percentUsed = (double)usedMemory/maxMemory;
            log.debug("Used Memory : "+usedMB+"/"+maxMB+" MB ("+ percentFormat.format(percentUsed)+")");
        }
    }

    public boolean isNotificationEnabled(Notification notification) {
        return true;
    }
}