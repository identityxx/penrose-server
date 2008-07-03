package org.safehaus.penrose.management.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.scheduler.JobConfig;
import org.safehaus.penrose.util.ClassUtil;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.management.partition.PartitionClient;
import org.safehaus.penrose.management.partition.PartitionManagerClient;
import org.safehaus.penrose.management.BaseClient;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.management.Client;
import org.apache.log4j.Level;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

import javax.management.*;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * @author Endi Sukma Dewata
 */
public class JobClient extends BaseClient implements JobServiceMBean {

    public static Logger log = LoggerFactory.getLogger(JobClient.class);

    protected String partitionName;

    public JobClient(PenroseClient client, String partitionName, String name) throws Exception {
        super(client, name, getStringObjectName(partitionName, name));

        this.partitionName = partitionName;
    }

    public JobConfig getJobConfig() throws Exception {
        return (JobConfig)connection.getAttribute(objectName, "JobConfig");
    }

    public static String getStringObjectName(String partitionName, String name) {
        return "Penrose:type=job,partition="+partitionName+",name="+name;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Command Line
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void showJobs(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        System.out.print(TextUtil.rightPad("JOB", 15)+" ");
        System.out.println(TextUtil.rightPad("STATUS", 10));

        System.out.print(TextUtil.repeat("-", 15)+" ");
        System.out.println(TextUtil.repeat("-", 10));

        SchedulerClient schedulerClient = partitionClient.getSchedulerClient();
        for (String jobName : schedulerClient.getJobNames()) {

            //JobClient jobClient = partitionClient.getJobClient(jobName);
            String status = "OK";

            System.out.print(TextUtil.rightPad(jobName, 15)+" ");
            System.out.println(TextUtil.rightPad(status, 10)+" ");
        }
    }

    public static void showJob(PenroseClient client, String partitionName, String jobName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        SchedulerClient schedulerClient = partitionClient.getSchedulerClient();
        JobClient jobClient = schedulerClient.getJobClient(jobName);
        JobConfig jobConfig = jobClient.getJobConfig();

        System.out.println("Job         : "+jobConfig.getName());
        System.out.println("Partition   : "+partitionName);

        String description = jobConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Enabled     : "+jobConfig.isEnabled());
        System.out.println();

        System.out.println("Parameters  :");
        for (String paramName : jobConfig.getParameterNames()) {
            String value = jobConfig.getParameter(paramName);
            System.out.println(" - " + paramName + ": " + value);
        }
        System.out.println();

        System.out.println("Attributes:");
        for (MBeanAttributeInfo attributeInfo  : jobClient.getAttributes()) {
            System.out.println(" - "+attributeInfo.getName()+" ("+attributeInfo.getType()+")");
        }
        System.out.println();

        System.out.println("Operations:");
        for (MBeanOperationInfo operationInfo : jobClient.getOperations()) {

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
        if ("jobs".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showJobs(client, partitionName);

        } else if ("job".equals(target)) {
            String jobName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showJob(client, partitionName, jobName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void startJobs(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        SchedulerClient schedulerClient = partitionClient.getSchedulerClient();
        for (String jobName : schedulerClient.getJobNames()) {

            log.debug("Starting job "+jobName+" in partition "+partitionName+"...");

            JobClient jobClient = schedulerClient.getJobClient(jobName);
            jobClient.start();

            log.debug("Job started.");
        }
    }

    public static void startJob(PenroseClient client, String partitionName, String jobName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        log.debug("Starting job "+jobName+" in partition "+partitionName+"...");

        SchedulerClient schedulerClient = partitionClient.getSchedulerClient();
        JobClient jobClient = schedulerClient.getJobClient(jobName);
        jobClient.start();

        log.debug("Job started.");
    }

    public static void processStartCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("jobs".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            startJobs(client, partitionName);

        } else if ("job".equals(target)) {
            String jobName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            startJob(client, partitionName, jobName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void stopJobs(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        SchedulerClient schedulerClient = partitionClient.getSchedulerClient();
        for (String jobName : schedulerClient.getJobNames()) {

            log.debug("Starting job "+jobName+" in partition "+partitionName+"...");

            JobClient jobClient = schedulerClient.getJobClient(jobName);
            jobClient.stop();

            log.debug("Job started.");
        }
    }

    public static void stopJob(PenroseClient client, String partitionName, String jobName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        log.debug("Stopping job "+jobName+" in partition "+partitionName+"...");

        SchedulerClient schedulerClient = partitionClient.getSchedulerClient();
        JobClient jobClient = schedulerClient.getJobClient(jobName);
        jobClient.stop();

        log.debug("Job stopped.");
    }

    public static void processStopCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("jobs".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            stopJobs(client, partitionName);

        } else if ("job".equals(target)) {
            String jobName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            stopJob(client, partitionName, jobName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void restartJobs(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        SchedulerClient schedulerClient = partitionClient.getSchedulerClient();
        for (String jobName : schedulerClient.getJobNames()) {

            log.debug("Restarting job "+jobName+" in partition "+partitionName+"...");

            JobClient jobClient = schedulerClient.getJobClient(jobName);
            jobClient.restart();

            log.debug("Job restarted.");
        }
    }

    public static void restartJob(PenroseClient client, String partitionName, String jobName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        log.debug("Restarting job "+jobName+" in partition "+partitionName+"...");

        SchedulerClient schedulerClient = partitionClient.getSchedulerClient();
        JobClient jobClient = schedulerClient.getJobClient(jobName);
        jobClient.restart();

        log.debug("Job restarted.");
    }

    public static void processRestartCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("jobs".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            restartJobs(client, partitionName);

        } else if ("job".equals(target)) {
            String jobName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            restartJob(client, partitionName, jobName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void invokeMethod(
            PenroseClient client,
            String partitionName,
            String jobName,
            String methodName,
            Object[] paramValues,
            String[] paramTypes
    ) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        SchedulerClient schedulerClient = partitionClient.getSchedulerClient();
        JobClient jobClient = schedulerClient.getJobClient(jobName);

        Object returnValue = jobClient.invoke(
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
        if ("job".equals(target)) {
            String jobName = iterator.next();
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

            invokeMethod(client, partitionName, jobName, methodName, paramValues, paramTypes);

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
        System.out.println("Usage: org.safehaus.penrose.management.scheduler.JobClient [OPTION]... <COMMAND>");
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
        System.out.println("  show jobs in partition <partition name>");
        System.out.println("  show job <job name> in partition <partition name>>");
        System.out.println();
        System.out.println("  start jobs in partition <partition name>");
        System.out.println("  start job <job name> in partition <partition name>");
        System.out.println();
        System.out.println("  stop jobs in partition <partition name>");
        System.out.println("  stop job <job name> in partition <partition name>");
        System.out.println();
        System.out.println("  restart jobs in partition <partition name>");
        System.out.println("  restart job <job name> in partition <partition name>");
        System.out.println();
        System.out.println("  invoke method <method name> in job <job name> in partition <partition name> [with <parameter>...]");
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

        Getopt getopt = new Getopt("JobClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

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
