package org.safehaus.penrose.service;

import org.safehaus.penrose.management.BaseClient;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.management.service.ServiceManagerServiceMBean;
import org.safehaus.penrose.util.TextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.Level;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceManagerClient extends BaseClient implements ServiceManagerServiceMBean {

    public static Logger log = LoggerFactory.getLogger(ServiceManagerClient.class);

    public ServiceManagerClient(PenroseClient client) throws Exception {
        super(client, "ServiceManager", getStringObjectName());
    }

    public Collection<String> getServiceNames() throws Exception {
        return (Collection<String>)getAttribute("ServiceNames");
    }

    public void startServices() throws Exception {
        invoke(
                "startServices",
                new Object[] { },
                new String[] { });
    }

    public void stopServices() throws Exception {
        invoke(
                "stopServices",
                new Object[] { },
                new String[] { });
    }

    public ServiceConfig getServiceConfig(String name) throws Exception {
        return (ServiceConfig)invoke(
                "getServiceConfig",
                new Object[] { name },
                new String[] { String.class.getName() });
    }

    public void startService(String name) throws Exception {
        invoke("startService",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void stopService(String name) throws Exception {
        invoke("stopService",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void createService(ServiceConfig serviceConfig) throws Exception {
        invoke("createService",
                new Object[] { serviceConfig },
                new String[] { ServiceConfig.class.getName() }
        );
    }

    public void updateService(String name, ServiceConfig serviceConfig) throws Exception {
        invoke("updateService",
                new Object[] { name, serviceConfig },
                new String[] { String.class.getName(), ServiceConfig.class.getName() }
        );
    }

    public void removeService(String name) throws Exception {
        invoke("removeService",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public ServiceClient getServiceClient(String serviceName) throws Exception {
        return new ServiceClient(client, serviceName);
    }

    public static String getStringObjectName() {
        return "Penrose:name=ServiceManager";
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Command Line
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void showServices(PenroseClient client) throws Exception {

        ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();

        System.out.print(TextUtil.rightPad("PARTITION", 15)+" ");
        System.out.println(TextUtil.rightPad("STATUS", 10));

        System.out.print(TextUtil.repeat("-", 15)+" ");
        System.out.println(TextUtil.repeat("-", 10));

        for (String serviceName : serviceManagerClient.getServiceNames()) {
            ServiceClient serviceClient = serviceManagerClient.getServiceClient(serviceName);

            String status;
            try {
                status = serviceClient.getStatus();
            } catch (Exception e) {
                status = "STOPPED";
            }

            System.out.print(TextUtil.rightPad(serviceName, 15) + " ");
            System.out.println(TextUtil.rightPad(status, 10));
        }
    }

    public static void showService(PenroseClient client, String serviceName) throws Exception {

        ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();
        ServiceClient serviceClient = serviceManagerClient.getServiceClient(serviceName);

        ServiceConfig serviceConfig = serviceManagerClient.getServiceConfig(serviceName);

        System.out.println("Name        : "+serviceConfig.getName());
        System.out.println("Class       : "+serviceConfig.getServiceClass());

        String description = serviceConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Enabled     : "+serviceConfig.isEnabled());

        String status;
        try {
            status = serviceClient.getStatus();
        } catch (Exception e) {
            status = "STOPPED";
        }
        System.out.println("Status      : "+status);

        System.out.println();

        System.out.println("Parameters: ");
        for (String paramName : serviceConfig.getParameterNames()) {
            String value = serviceConfig.getParameter(paramName);
            System.out.println(" - " + paramName + ": " + value);
        }
    }

    public static void processShowCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("services".equals(target)) {
            showServices(client);

        } else if ("service".equals(target)) {
            String serviceName = iterator.next();
            showService(client, serviceName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void startServices(PenroseClient client) throws Exception {

        log.debug("Starting all services...");

        ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();
        serviceManagerClient.startServices();

        log.debug("All partitions started.");
    }

    public static void startService(PenroseClient client, String serviceName) throws Exception {

        log.debug("Starting service "+serviceName+"...");

        ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();
        ServiceClient serviceClient = serviceManagerClient.getServiceClient(serviceName);
        serviceClient.start();

        log.debug("Service "+serviceName+" started.");
    }

    public static void processStartCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("services".equals(target)) {
            startServices(client);

        } else if ("service".equals(target)) {
            String serviceName = iterator.next();
            startService(client, serviceName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void stopServices(PenroseClient client) throws Exception {

        log.debug("Stopping all services...");

        ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();
        serviceManagerClient.stopServices();

        log.debug("All services stopped.");
    }

    public static void stopService(PenroseClient client, String serviceName) throws Exception {

        log.debug("Stopping service "+serviceName+"...");

        ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();
        ServiceClient serviceClient = serviceManagerClient.getServiceClient(serviceName);
        serviceClient.stop();

        log.debug("Service "+serviceName+" stopped.");
    }

    public static void processStopCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("services".equals(target)) {
            stopServices(client);

        } else if ("service".equals(target)) {
            String serviceName = iterator.next();
            stopService(client, serviceName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void restartServices(PenroseClient client) throws Exception {
        stopServices(client);
        startServices(client);
    }

    public static void restartService(PenroseClient client, String serviceName) throws Exception {
        stopService(client, serviceName);
        startService(client, serviceName);
    }

    public static void processRestartCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("services".equals(target)) {
            restartServices(client);

        } else if ("service".equals(target)) {
            String serviceName = iterator.next();
            restartService(client, serviceName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void invokeMethod(
            PenroseClient client,
            String serviceName,
            String methodName,
            Object[] paramValues,
            String[] paramTypes
    ) throws Exception {

        ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();

        ServiceClient serviceClient = serviceManagerClient.getServiceClient(serviceName);

        Object returnValue = serviceClient.invoke(
                methodName,
                paramValues,
                paramTypes
        );

        System.out.println("Return value: "+returnValue);
    }

    public static void processInvokeCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        iterator.next(); // method
        String methodName = iterator.next();
        iterator.next(); // in

        String target = iterator.next();
        if ("service".equals(target)) {
            String serviceName = iterator.next();

            Object[] paramValues;
            String[] paramTypes;

            if (iterator.hasNext()) {
                iterator.next(); // with

                Collection<Object> values = new ArrayList<Object>();
                Collection<String> types = new ArrayList<String>();

                while (iterator.hasNext()) {
                    String value = iterator.next();
                    values.add(value);
                    types.add(String.class.getName());
                }

                paramValues = values.toArray(new Object[values.size()]);
                paramTypes = types.toArray(new String[types.size()]);

            } else {
                paramValues = new Object[0];
                paramTypes = new String[0];
            }

            invokeMethod(client, serviceName, methodName, paramValues, paramTypes);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void execute(PenroseClient client, Collection<String> parameters) throws Exception {

        Iterator<String> iterator = parameters.iterator();
        String command = iterator.next();
        log.debug("Executing "+command);

        if ("show".equals(command)) {
            processShowCommand(client, iterator);

        } else if ("start".equals(command)) {
            processStartCommand(client, iterator);

        } else if ("stop".equals(command)) {
            processStopCommand(client, iterator);

        } else if ("restart".equals(command)) {
            processRestartCommand(client, iterator);
/*
        } else if ("invoke".equals(command)) {
            processInvokeCommand(client, iterator);
*/
        } else {
            System.out.println("Invalid command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.service.ServiceManagerClient [OPTION]... <COMMAND>");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -?, --help         display this help and exit");
        System.out.println("  -P protocol        Penrose JMX protocol");
        System.out.println("  -h host            Penrose server");
        System.out.println("  -p port            Penrose JMX port");
        System.out.println("  -D username        username");
        System.out.println("  -w password        password");
        System.out.println("  -d                 run in debug mode");
        System.out.println("  -v                 run in verbose mode");
        System.out.println();
        System.out.println("Commands:");
        System.out.println();
        System.out.println("  show services");
        System.out.println("  show service <service name>");
        System.out.println();
        System.out.println("  start services");
        System.out.println("  start service <service name>");
        System.out.println();
        System.out.println("  stop services");
        System.out.println("  stop service <service name>");
        System.out.println();
        System.out.println("  restart services");
        System.out.println("  restart service <service name>");
        System.out.println();
        System.out.println("  invoke method <method name> in service <service name> [with <parameter>...]");
    }

    public static void main(String args[]) throws Exception {

        Level level          = Level.WARN;
        String serverType    = PenroseClient.PENROSE;
        String protocol      = PenroseClient.DEFAULT_PROTOCOL;
        String hostname      = "localhost";
        int portNumber       = PenroseClient.DEFAULT_RMI_PORT;
        int rmiTransportPort = PenroseClient.DEFAULT_RMI_TRANSPORT_PORT;

        String bindDn = null;
        String bindPassword = null;

        LongOpt[] longopts = new LongOpt[1];
        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, '?');

        Getopt getopt = new Getopt("ServiceManagerClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

        Collection<String> parameters = new ArrayList<String>();
        int c;
        while ((c = getopt.getopt()) != -1) {
            switch (c) {
                case ':':
                case '?':
                    showUsage();
                    System.exit(0);
                    break;
                case 1:
                    parameters.add(getopt.getOptarg());
                    break;
                case 'd':
                    level = Level.DEBUG;
                    break;
                case 'v':
                    level = Level.INFO;
                    break;
                case 'P':
                    protocol = getopt.getOptarg();
                    break;
                case 't':
                    serverType = getopt.getOptarg();
                    break;
                case 'h':
                    hostname = getopt.getOptarg();
                    break;
                case 'p':
                    portNumber = Integer.parseInt(getopt.getOptarg());
                    break;
                case 'r':
                    rmiTransportPort = Integer.parseInt(getopt.getOptarg());
                    break;
                case 'D':
                    bindDn = getopt.getOptarg();
                    break;
                case 'w':
                    bindPassword = getopt.getOptarg();
            }
        }

        if (parameters.size() == 0) {
            showUsage();
            System.exit(0);
        }

        File clientHome = new File(System.getProperty("org.safehaus.penrose.client.home"));

        //Logger rootLogger = Logger.getRootLogger();
        //rootLogger.setLevel(Level.OFF);

        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.safehaus.penrose");

        File log4jXml = new File(clientHome, "conf"+File.separator+"log4j.xml");

        if (level.equals(Level.DEBUG)) {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%-20C{1} [%4L] %m%n"));
            BasicConfigurator.configure(appender);

        } else if (level.equals(Level.INFO)) {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);

        } else if (log4jXml.exists()) {
            DOMConfigurator.configure(log4jXml.getAbsolutePath());

        } else {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);
        }

        try {
            PenroseClient client = new PenroseClient(
                    serverType,
                    protocol,
                    hostname,
                    portNumber,
                    bindDn,
                    bindPassword
            );

            client.setRmiTransportPort(rmiTransportPort);
            client.connect();

            execute(client, parameters);

            client.close();

        } catch (SecurityException e) {
            log.error(e.getMessage());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
