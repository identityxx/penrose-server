package org.safehaus.penrose.session;

import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.client.PenroseClient;
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
public class SessionManagerClient extends BaseClient implements SessionManagerServiceMBean {

    public static Logger log = LoggerFactory.getLogger(SessionManagerClient.class);

    public SessionManagerClient(PenroseClient client) throws Exception {
        super(client, "SessionManager", getStringObjectName());
    }

    public Collection<String> getSessionNames() throws Exception {
        return (Collection<String>)getAttribute("SessionNames");
    }

    public Collection<String> getOperationNames(String sessionName) throws Exception {
        return (Collection<String>)invoke(
                "getOperationNames",
                new Object[] { sessionName },
                new String[] { String.class.getName() }
        );
    }

    public SessionClient getSessionClient(String sessionName) throws Exception {
        return new SessionClient(client, sessionName);
    }

    public void closeSession(String sessionName) throws Exception {
        invoke(
                "closeSession",
                new Object[] { sessionName },
                new String[] { String.class.getName() }
        );
    }

    public void abandonOperation(String sessionName, String operationName) throws Exception {
        invoke(
                "abandonOperation",
                new Object[] { sessionName, operationName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public static String getStringObjectName() {
        return "Penrose:name=SessionManager";
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Command Line
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void showSessions(PenroseClient client) throws Exception {

        SessionManagerClient sessionManagerClient = client.getSessionManagerClient();

        System.out.print(TextUtil.rightPad("SESSIONS", 15)+" ");
        System.out.println(TextUtil.rightPad("STATUS", 10));

        System.out.print(TextUtil.repeat("-", 15)+" ");
        System.out.println(TextUtil.repeat("-", 10));

        for (String sessionName : sessionManagerClient.getSessionNames()) {
            //SessionClient sessionClient = sessionManagerClient.getSessionClient(sessionName);

            String status;
            try {
                status = null; // sessionClient.getStatus();
            } catch (Exception e) {
                status = "STOPPED";
            }

            System.out.print(TextUtil.rightPad(sessionName, 15) + " ");
            System.out.println(TextUtil.rightPad(status, 10));
        }
    }

    public static void showSession(PenroseClient client, String sessionName) throws Exception {

        SessionManagerClient sessionManagerClient = client.getSessionManagerClient();
        //SessionClient sessionClient = sessionManagerClient.getSessionClient(sessionName);

        System.out.println("Name        : "+sessionName);

        System.out.println();

        System.out.println("Operations: ");
        for (String operationName : sessionManagerClient.getOperationNames(sessionName)) {
            //OperationClient operationClient = sessionClient.getOperationClient(operationName);
            System.out.println(" - "+operationName);
        }
    }

    public static void processShowCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("sessions".equals(target)) {
            showSessions(client);

        } else if ("session".equals(target)) {
            String sessionName = iterator.next();
            showSession(client, sessionName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void processCloseCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        iterator.next(); // session
        String sessionName = iterator.next();

        SessionManagerClient sessionManagerClient = client.getSessionManagerClient();
        sessionManagerClient.closeSession(sessionName);
    }

    public static void processAbandonCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        iterator.next(); // operation
        String operationName = iterator.next();

        iterator.next(); // in
        iterator.next(); // session
        String sessionName = iterator.next();

        SessionManagerClient sessionManagerClient = client.getSessionManagerClient();
        sessionManagerClient.abandonOperation(sessionName, operationName);
    }

    public static void invokeMethod(
            PenroseClient client,
            String sessionName,
            String methodName,
            Object[] paramValues,
            String[] paramTypes
    ) throws Exception {

        SessionManagerClient sessionManagerClient = client.getSessionManagerClient();

        SessionClient sessionClient = sessionManagerClient.getSessionClient(sessionName);

        Object returnValue = sessionClient.invoke(
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
        if ("session".equals(target)) {
            String sessionName = iterator.next();

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

            invokeMethod(client, sessionName, methodName, paramValues, paramTypes);

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

        } else if ("close".equals(command)) {
            processCloseCommand(client, iterator);

        } else if ("abandon".equals(command)) {
            processAbandonCommand(client, iterator);
/*
        } else if ("invoke".equals(command)) {
            processInvokeCommand(client, iterator);
*/
        } else {
            System.out.println("Invalid command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.session.SessionManagerClient [OPTION]... <COMMAND>");
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
        System.out.println("  show sessions");
        System.out.println("  show session <session name>");
        System.out.println();
        System.out.println("  close session <session name>");
        System.out.println();
        System.out.println("  abandon operation <operation name> in session <session name>");
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

        Getopt getopt = new Getopt("SessionManagerClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

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