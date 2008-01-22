package org.safehaus.penrose.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.util.ClassUtil;
import org.apache.log4j.Level;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

import javax.management.*;
import java.util.Iterator;
import java.util.Collection;
import java.util.ArrayList;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * @author Endi Sukma Dewata
 */
public class ModuleClient implements ModuleServiceMBean {

    public static Logger log = LoggerFactory.getLogger(ModuleClient.class);

    private PenroseClient client;
    private String partitionName;
    private String name;

    MBeanServerConnection connection;
    ObjectName objectName;

    public ModuleClient(PenroseClient client, String partitionName, String name) throws Exception {
        this.client = client;
        this.partitionName = partitionName;
        this.name = name;

        connection = client.getConnection();
        objectName = ObjectName.getInstance(getObjectName(partitionName, name));
    }

    public ModuleConfig getModuleConfig() throws Exception {
        return (ModuleConfig)connection.getAttribute(objectName, "ModuleConfig");
    }

    public static String getObjectName(String partitionName, String name) {
        return "Penrose:type=module,partition="+partitionName+",name="+name;
    }

    public MBeanAttributeInfo[] getAttributes() throws Exception {
        MBeanInfo info = connection.getMBeanInfo(objectName);
        return info.getAttributes();
    }

    public MBeanOperationInfo[] getOperations() throws Exception {
        MBeanInfo info = connection.getMBeanInfo(objectName);
        return info.getOperations();
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void start() throws Exception {
        invoke("start", new Object[] {}, new String[] {});
    }

    public void stop() throws Exception {
        invoke("stop", new Object[] {}, new String[] {});
    }

    public void restart() throws Exception {
        invoke("restart", new Object[] {}, new String[] {});
    }

    public Object getAttribute(String attributeName) throws Exception {

        log.debug("Getting attribute "+ attributeName +" from "+name+".");

        return connection.getAttribute(
                objectName,
                attributeName
        );
    }

    public Object invoke(String method, Object[] paramValues, String[] paramTypes) throws Exception {

        log.debug("Invoking method "+method+"() on "+name+".");

        return connection.invoke(
                objectName,
                method,
                paramValues,
                paramTypes
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Command Line
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void showModules(PenroseClient client, String partitionName) throws Exception {

        PartitionClient partitionClient = client.getPartitionClient(partitionName);

        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("MODULE", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.rightPad("STATUS", 10));

        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.repeat("-", 10));

        for (String moduleName : partitionClient.getModuleNames()) {

            //ModuleClient moduleClient = partitionClient.getModuleClient(moduleName);
            String status = "OK";

            System.out.print(org.safehaus.penrose.util.Formatter.rightPad(moduleName, 15)+" ");
            System.out.println(org.safehaus.penrose.util.Formatter.rightPad(status, 10)+" ");
        }
    }

    public static void showModule(PenroseClient client, String partitionName, String moduleName) throws Exception {

        PartitionClient partitionClient = client.getPartitionClient(partitionName);
        ModuleClient moduleClient = partitionClient.getModuleClient(moduleName);
        ModuleConfig moduleConfig = moduleClient.getModuleConfig();

        System.out.println("Module      : "+moduleConfig.getName());
        System.out.println("Partition   : "+partitionName);

        String description = moduleConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Enabled     : "+moduleConfig.isEnabled());
        System.out.println();

        System.out.println("Parameters  :");
        for (String paramName : moduleConfig.getParameterNames()) {
            String value = moduleConfig.getParameter(paramName);
            System.out.println(" - " + paramName + ": " + value);
        }
        System.out.println();

        System.out.println("Attributes:");
        for (MBeanAttributeInfo attributeInfo  : moduleClient.getAttributes()) {
            System.out.println(" - "+attributeInfo.getName()+" ("+attributeInfo.getType()+")");
        }
        System.out.println();

        System.out.println("Operations:");
        for (MBeanOperationInfo operationInfo : moduleClient.getOperations()) {

            Collection<String> paramTypes = new ArrayList<String>();
            for (MBeanParameterInfo parameterInfo : operationInfo.getSignature()) {
                paramTypes.add(parameterInfo.getType());
            }

            String operation = operationInfo.getReturnType()+" "+ ClassUtil.getSignature(operationInfo.getName(), paramTypes);
            System.out.println(" - "+operation);
        }
    }

    public static void processShowCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("modules".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showModules(client, partitionName);

        } else if ("module".equals(target)) {
            String moduleName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showModule(client, partitionName, moduleName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void startModules(PenroseClient client, String partitionName) throws Exception {

        PartitionClient partitionClient = client.getPartitionClient(partitionName);

        for (String moduleName : partitionClient.getModuleNames()) {

            log.debug("Starting module "+moduleName+" in partition "+partitionName+"...");

            ModuleClient moduleClient = partitionClient.getModuleClient(moduleName);
            moduleClient.start();

            log.debug("Module started.");
        }
    }

    public static void startModule(PenroseClient client, String partitionName, String moduleName) throws Exception {

        PartitionClient partitionClient = client.getPartitionClient(partitionName);

        log.debug("Starting module "+moduleName+" in partition "+partitionName+"...");

        ModuleClient moduleClient = partitionClient.getModuleClient(moduleName);
        moduleClient.start();

        log.debug("Module started.");
    }

    public static void processStartCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("modules".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            startModules(client, partitionName);

        } else if ("module".equals(target)) {
            String moduleName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            startModule(client, partitionName, moduleName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void stopModules(PenroseClient client, String partitionName) throws Exception {

        PartitionClient partitionClient = client.getPartitionClient(partitionName);

        for (String moduleName : partitionClient.getModuleNames()) {

            log.debug("Starting module "+moduleName+" in partition "+partitionName+"...");

            ModuleClient moduleClient = partitionClient.getModuleClient(moduleName);
            moduleClient.stop();

            log.debug("Module started.");
        }
    }

    public static void stopModule(PenroseClient client, String partitionName, String moduleName) throws Exception {

        PartitionClient partitionClient = client.getPartitionClient(partitionName);

        log.debug("Stopping module "+moduleName+" in partition "+partitionName+"...");

        ModuleClient moduleClient = partitionClient.getModuleClient(moduleName);
        moduleClient.stop();

        log.debug("Module stopped.");
    }

    public static void processStopCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("modules".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            stopModules(client, partitionName);

        } else if ("module".equals(target)) {
            String moduleName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            stopModule(client, partitionName, moduleName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void restartModules(PenroseClient client, String partitionName) throws Exception {

        PartitionClient partitionClient = client.getPartitionClient(partitionName);

        for (String moduleName : partitionClient.getModuleNames()) {

            log.debug("Restarting module "+moduleName+" in partition "+partitionName+"...");

            ModuleClient moduleClient = partitionClient.getModuleClient(moduleName);
            moduleClient.restart();

            log.debug("Module restarted.");
        }
    }

    public static void restartModule(PenroseClient client, String partitionName, String moduleName) throws Exception {

        PartitionClient partitionClient = client.getPartitionClient(partitionName);

        log.debug("Restarting module "+moduleName+" in partition "+partitionName+"...");

        ModuleClient moduleClient = partitionClient.getModuleClient(moduleName);
        moduleClient.restart();

        log.debug("Module restarted.");
    }

    public static void processRestartCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("modules".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            restartModules(client, partitionName);

        } else if ("module".equals(target)) {
            String moduleName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            restartModule(client, partitionName, moduleName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void invokeMethod(
            PenroseClient client,
            String partitionName,
            String moduleName,
            String methodName,
            Object[] paramValues,
            String[] paramTypes
    ) throws Exception {

        PartitionClient partitionClient = client.getPartitionClient(partitionName);
        ModuleClient moduleClient = partitionClient.getModuleClient(moduleName);

        Object returnValue = moduleClient.invoke(
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
        if ("module".equals(target)) {
            String moduleName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();

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

            invokeMethod(client, partitionName, moduleName, methodName, paramValues, paramTypes);

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

        } else if ("invoke".equals(command)) {
            processInvokeCommand(client, iterator);

        } else {
            System.out.println("Invalid command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.management.ModuleClient [OPTION]... <COMMAND>");
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
        System.out.println("  show modules in partition <partition name>");
        System.out.println("  show module <module name> in partition <partition name>>");
        System.out.println();
        System.out.println("  start modules in partition <partition name>");
        System.out.println("  start module <module name> in partition <partition name>");
        System.out.println();
        System.out.println("  stop modules in partition <partition name>");
        System.out.println("  stop module <module name> in partition <partition name>");
        System.out.println();
        System.out.println("  restart modules in partition <partition name>");
        System.out.println("  restart module <module name> in partition <partition name>");
        System.out.println();
        System.out.println("  invoke method <method name> in module <module name> in partition <partition name> [with <parameter>...]");
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

        Getopt getopt = new Getopt("ModuleClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

        Collection<String> parameters = new ArrayList<String>();
        int c;
        while ((c = getopt.getopt()) != -1) {
            switch (c) {
                case ':':
                case '?':
                    Client.showUsage();
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

        File serviceHome = new File(System.getProperty("org.safehaus.penrose.management.home"));

        //Logger rootLogger = Logger.getRootLogger();
        //rootLogger.setLevel(Level.OFF);

        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.safehaus.penrose");

        File log4jXml = new File(serviceHome, "conf"+File.separator+"log4j.xml");

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
