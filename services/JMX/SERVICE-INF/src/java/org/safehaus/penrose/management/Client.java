package org.safehaus.penrose.management;

import org.apache.log4j.*;
import org.apache.log4j.xml.DOMConfigurator;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.ClassUtil;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.scheduler.JobConfig;

import java.util.*;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanAttributeInfo;

public class Client {

    public static Logger log = Logger.getLogger(Client.class);

    PenroseClient client;

    public Client(
            String serverType,
            String protocol,
            String hostname,
            int port,
            String bindDn,
            String bindPassword
    ) throws Exception {

        client = new PenroseClient(
                serverType,
                protocol,
                hostname,
                port,
                bindDn,
                bindPassword
        );
    }

    public void setRmiTransportPort(int rmiTransportPort) {
        client.setRmiTransportPort(rmiTransportPort);
    }

    public void connect() throws Exception {
        client.connect();
    }

    public void close() throws Exception {
        client.close();
    }

    public void showServices() throws Exception {
        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("SERVICE", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.rightPad("STATUS", 10));

        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.repeat("-", 10));

        for (String serviceName : client.getServiceNames()) {
            ServiceClient serviceClient = client.getServiceClient(serviceName);
            String status = serviceClient.getStatus();

            System.out.print(org.safehaus.penrose.util.Formatter.rightPad(serviceName, 15) + " ");
            System.out.println(Formatter.rightPad(status, 10));
        }
    }

    public void showService(String serviceName) throws Exception {
        ServiceClient serviceClient = client.getServiceClient(serviceName);
        ServiceConfig serviceConfig = serviceClient.getServiceConfig();

        System.out.println("Name        : "+serviceConfig.getName());
        System.out.println("Class       : "+serviceConfig.getServiceClass());

        String description = serviceConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Enabled     : "+serviceConfig.isEnabled());
        System.out.println("Status      : "+serviceClient.getStatus());
        System.out.println();

        System.out.println("Parameters: ");
        for (String paramName : serviceConfig.getParameterNames()) {
            String value = serviceConfig.getParameter(paramName);
            System.out.println(" - " + paramName + ": " + value);
        }
    }

    public void showPartitions() throws Exception {
        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("PARTITION", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.rightPad("STATUS", 10));

        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.repeat("-", 10));

        for (String partitionName : client.getPartitionNames()) {
            PartitionClient partitionClient = client.getPartitionClient(partitionName);
            String status = partitionClient.getStatus();

            System.out.print(Formatter.rightPad(partitionName, 15) + " ");
            System.out.println(Formatter.rightPad(status, 10));
        }
    }

    public void showPartition(String partitionName) throws Exception {

        PartitionClient partitionClient = client.getPartitionClient(partitionName);
        PartitionConfig partitionConfig = partitionClient.getPartitionConfig();

        System.out.println("Name        : "+partitionConfig.getName());

        String description = partitionConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Enabled     : "+partitionConfig.isEnabled());
        System.out.println("Status      : "+partitionClient.getStatus());
        System.out.println();

        System.out.println("Connections:");
        for (ConnectionConfig connectionConfig : partitionConfig.getConnectionConfigs().getConnectionConfigs()) {
            System.out.println(" - "+connectionConfig.getName());
        }
        System.out.println();

        System.out.println("Sources:");
        for (String sourceName : partitionClient.getSourceNames()) {
            System.out.println(" - "+sourceName);
        }
        System.out.println();

        System.out.println("Modules:");
        for (String moduleName : partitionClient.getModuleNames()) {
            System.out.println(" - "+moduleName);
        }
    }
/*
    public void printSources() throws Exception {

        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("SOURCE", 15)+" ");
        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("PARTITION", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.rightPad("STATUS", 10));

        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.repeat("-", 10));

        SourceManagerClient sourceManagerClient = client.getSourceManagerClient();
        for (Iterator i=sourceManagerClient.getPartitionNames().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();

            for (Iterator j=sourceManagerClient.getSourceNames(partitionName).iterator(); j.hasNext(); ) {
                String sourceName = (String)j.next();
                String status = sourceManagerClient.getStatus(partitionName, sourceName);

                System.out.print(org.safehaus.penrose.util.Formatter.rightPad(sourceName, 15)+" ");
                System.out.print(org.safehaus.penrose.util.Formatter.rightPad(partitionName, 15)+" ");
                System.out.println(org.safehaus.penrose.util.Formatter.rightPad(status, 10));
            }
        }
    }
*/
    public void showModules(String partitionName) throws Exception {

        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("MODULE", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.rightPad("STATUS", 10));

        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.repeat("-", 10));

        PartitionClient partitionClient = client.getPartitionClient(partitionName);
        for (String moduleName : partitionClient.getModuleNames()) {
            ModuleClient moduleClient = partitionClient.getModuleClient(moduleName);
            String status = "STARTED"; // moduleClient.getStatus();

            System.out.print(org.safehaus.penrose.util.Formatter.rightPad(moduleName, 15)+" ");
            System.out.println(org.safehaus.penrose.util.Formatter.rightPad(status, 10));
        }
    }

    public void showJobs(String partitionName) throws Exception {

        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("JOB", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.rightPad("STATUS", 10));

        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.repeat("-", 10));

        PartitionClient partitionClient = client.getPartitionClient(partitionName);
        SchedulerClient schedulerClient = partitionClient.getSchedulerClient();

        for (String jobName : schedulerClient.getJobNames()) {
            JobClient jobClient = schedulerClient.getJobClient(jobName);
            String status = "STARTED"; // jobClient.getStatus();

            System.out.print(org.safehaus.penrose.util.Formatter.rightPad(jobName, 15)+" ");
            System.out.println(org.safehaus.penrose.util.Formatter.rightPad(status, 10));
        }
    }

    public void showConnection(String partitionName, String connectionName) throws Exception {
        PartitionClient partitionClient = client.getPartitionClient(partitionName);
        ConnectionClient connectionClient = partitionClient.getConnectionClient(connectionName);
        ConnectionConfig connectionConfig = connectionClient.getConnectionConfig();

        System.out.println("Connection  : "+connectionConfig.getName());
        System.out.println("Partition   : "+partitionName);
        System.out.println("Adapter     : "+connectionConfig.getAdapterName());

        String description = connectionConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));
        System.out.println();

        System.out.println("Parameters  :");
        for (String paramName : connectionConfig.getParameterNames()) {
            String value = connectionConfig.getParameter(paramName);
            System.out.println(" - " + paramName + ": " + value);
        }
        System.out.println();
    }

    public void showSource(String partitionName, String sourceName) throws Exception {
        PartitionClient partitionClient = client.getPartitionClient(partitionName);
        SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
        SourceConfig sourceConfig = sourceClient.getSourceConfig();

        System.out.println("Source      : "+sourceConfig.getName());
        System.out.println("Connection  : "+sourceConfig.getConnectionName());
        System.out.println("Partition   : "+partitionName);

        String description = sourceConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));
        System.out.println();

        System.out.println("Parameters  :");
        for (String paramName : sourceConfig.getParameterNames()) {
            String value = sourceConfig.getParameter(paramName);
            System.out.println(" - " + paramName + ": " + value);
        }
        System.out.println();
    }

    public void showModule(String partitionName, String moduleName) throws Exception {
        PartitionClient partitionClient = client.getPartitionClient(partitionName);
        ModuleClient moduleClient = partitionClient.getModuleClient(moduleName);
        ModuleConfig moduleConfig = moduleClient.getModuleConfig();

        System.out.println("Name        : "+moduleConfig.getName());
        System.out.println("Class       : "+moduleConfig.getModuleClass());
        System.out.println("Partition   : "+partitionName);

        String description = moduleConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Enabled     : "+moduleConfig.isEnabled());
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

            String operation = operationInfo.getReturnType()+" "+ClassUtil.getSignature(operationInfo.getName(), paramTypes);
            System.out.println(" - "+operation);
        }
    }

    public void showJob(String partitionName, String jobName) throws Exception {
        PartitionClient partitionClient = client.getPartitionClient(partitionName);
        SchedulerClient schedulerClient = partitionClient.getSchedulerClient();
        JobClient jobClient = schedulerClient.getJobClient(jobName);
        JobConfig jobConfig = jobClient.getJobConfig();

        System.out.println("Name        : "+jobConfig.getName());
        System.out.println("Class       : "+jobConfig.getJobClass());
        System.out.println("Partition   : "+partitionName);

        String description = jobConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Enabled     : "+jobConfig.isEnabled());
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

            String operation = operationInfo.getReturnType()+" "+ClassUtil.getSignature(operationInfo.getName(), paramTypes);
            System.out.println(" - "+operation);
        }
    }

    public void processShowCommand(Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("services".equals(target)) {
            showServices();

        } else if ("service".equals(target)) {
            String serviceName = (String)iterator.next();
            showService(serviceName);

        } else if ("partitions".equals(target)) {
            showPartitions();

        } else if ("partition".equals(target)) {
            String partitionName = iterator.next();
            showPartition(partitionName);

        } else if ("connection".equals(target)) {
            String connectionName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showConnection(partitionName, connectionName);

        } else if ("source".equals(target)) {
            String sourceName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showSource(partitionName, sourceName);

        } else if ("module".equals(target)) {
            String moduleName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showModule(partitionName, moduleName);

        } else if ("modules".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showModules(partitionName);

        } else if ("jobs".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showJobs(partitionName);

        } else if ("job".equals(target)) {
            String jobName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showJob(partitionName, jobName);

/*
        } else if ("sources".equals(target)) {
            showSources();
*/


        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void processInvokeCommand(Iterator<String> iterator) throws Exception {
        iterator.next(); // method
        String methodName = iterator.next();
        iterator.next(); // in

        String target = iterator.next(); // module
        if ("module".equals(target)) {
            String moduleName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();

            Object[] paramValues;
            String[] paramTypes;

            if (iterator.hasNext()) {
                iterator.next(); // with

                Collection<String> args = new ArrayList<String>();
                while (iterator.hasNext()) {
                    args.add(iterator.next());
                }

                paramValues = new Object[] {
                        args.toArray(new String[args.size()])
                };

                paramTypes = new String[] {
                        ("[L"+String.class.getName()+";")
                };

            } else {
                paramValues = new Object[0];
                paramTypes = new String[0];
            }

            PartitionClient partitionClient = client.getPartitionClient(partitionName);
            ModuleClient moduleClient = partitionClient.getModuleClient(moduleName);

            Object returnValue = moduleClient.invoke(
                    methodName,
                    paramValues,
                    paramTypes
            );
            
            System.out.println("Return value: "+returnValue);

        } else if ("job".equals(target)) {

            String jobName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();

            Object[] paramValues;
            String[] paramTypes;

            if (iterator.hasNext()) {
                iterator.next(); // with

                Collection<String> args = new ArrayList<String>();
                while (iterator.hasNext()) {
                    args.add(iterator.next());
                }

                paramValues = new Object[] {
                        args.toArray(new String[args.size()])
                };

                paramTypes = new String[] {
                        ("[L"+String.class.getName()+";")
                };

            } else {
                paramValues = new Object[0];
                paramTypes = new String[0];
            }

            PartitionClient partitionClient = client.getPartitionClient(partitionName);
            SchedulerClient schedulerClient = partitionClient.getSchedulerClient();
            JobClient jobClient = schedulerClient.getJobClient(jobName);

            Object returnValue = jobClient.invoke(
                    methodName,
                    paramValues,
                    paramTypes
            );

            System.out.println("Return value: "+returnValue);
        }
    }

    public void processStartCommand(Iterator iterator) throws Exception {
        String target = (String)iterator.next();
        if ("service".equals(target)) {
            String serviceName = (String)iterator.next();
            ServiceClient serviceClient = client.getServiceClient(serviceName);
            serviceClient.start();

        } else if ("partition".equals(target)) {
            String partitionName = (String)iterator.next();
            client.startPartition(partitionName);
/*

        } else if ("connection".equals(target)) {
            String connectionName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getConnectionManagerClient().start(partitionName, connectionName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("source".equals(target)) {
            String sourceName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getSourceManagerClient().start(partitionName, sourceName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("module".equals(target)) {
            String moduleName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getModuleManagerClient().start(partitionName, moduleName);

            } else {
                System.out.println("Missing partition name");
            }
*/
        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void processStopCommand(Iterator iterator) throws Exception {
        String target = (String)iterator.next();
        if ("service".equals(target)) {
            String serviceName = (String)iterator.next();
            ServiceClient serviceClient = client.getServiceClient(serviceName);
            serviceClient.stop();

        } else if ("partition".equals(target)) {
            String partitionName = (String)iterator.next();
            client.stopPartition(partitionName);
/*
        } else if ("connection".equals(target)) {
            String connectionName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getConnectionManagerClient().stop(partitionName, connectionName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("source".equals(target)) {
            String sourceName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getSourceManagerClient().stop(partitionName, sourceName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("module".equals(target)) {
            String moduleName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getModuleManagerClient().stop(partitionName, moduleName);

            } else {
                System.out.println("Missing partition name");
            }
*/
        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void processRestartCommand(Iterator iterator) throws Exception {

        String target = (String)iterator.next();
        if ("service".equals(target)) {
            String serviceName = (String)iterator.next();
            ServiceClient serviceClient = client.getServiceClient(serviceName);
            serviceClient.stop();
            serviceClient.start();

        } else if ("partition".equals(target)) {
            String partitionName = (String)iterator.next();
            client.stopPartition(partitionName);
            client.startPartition(partitionName);
/*
        } else if ("connection".equals(target)) {
            String connectionName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getConnectionManagerClient().restart(partitionName, connectionName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("source".equals(target)) {
            String sourceName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getSourceManagerClient().restart(partitionName, sourceName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("module".equals(target)) {
            String moduleName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getModuleManagerClient().restart(partitionName, moduleName);

            } else {
                System.out.println("Missing partition name");
            }
*/
        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void processSearchCommand(Iterator iterator) throws Exception {

        String target = (String)iterator.next();

        if ("source".equals(target)) {
            String partitionName = (String)iterator.next();
            String sourceName = (String)iterator.next();

            PartitionClient partitionClient = client.getPartitionClient(partitionName);
            SourceClient sourceClient = partitionClient.getSourceClient(sourceName);

            SearchRequest request = new SearchRequest();
            SearchResponse response = new SearchResponse();

            sourceClient.search(request, response);

            System.out.println("Results:");
            while (response.hasNext()) {
                SearchResult result = response.next();
                System.out.println(" - "+result.getDn());
            }

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void execute(Collection<String> parameters) throws Exception {

        Iterator<String> iterator = parameters.iterator();
        String command = iterator.next();
        log.debug("Executing "+command);

        if ("version".equals(command)) {
            String version = client.getProductName()+" "+client.getProductVersion();
            System.out.println(version);

        } else if ("show".equals(command)) {
            processShowCommand(iterator);

        } else if ("invoke".equals(command)) {
            processInvokeCommand(iterator);

        } else if ("start".equals(command)) {
            processStartCommand(iterator);

        } else if ("stop".equals(command)) {
            processStopCommand(iterator);

        } else if ("restart".equals(command)) {
            processRestartCommand(iterator);

        } else if ("restart".equals(command)) {
            processSearchCommand(iterator);

        } else if ("reload".equals(command)) {
            client.reload();

        } else if ("store".equals(command)) {
            client.store();

        } else if ("rename".equals(command)) {
            String object = iterator.next();
            if ("entry".equals(object)) {
                String oldDn = iterator.next();
                String newDn = iterator.next();
                log.debug("Renaming "+oldDn+" to "+newDn);
                client.renameEntryMapping(oldDn, newDn);
            }

        } else {
            System.out.println("Invalid command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.management.Client [OPTION]... <COMMAND>");
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
        System.out.println("  version");
        System.out.println();
        System.out.println("  show services");
        System.out.println("  show service <service name>");
        System.out.println("  start service <service name>");
        System.out.println("  stop service <service name>");
        System.out.println("  restart service <service name>");
        System.out.println();
        System.out.println("  show partitions");
        System.out.println("  show partition <partition name>");
        System.out.println("  start partition <partition name>");
        System.out.println("  stop partition <partition name>");
        System.out.println("  restart partition <partition name>");
        System.out.println();
        System.out.println("  show connections in partition <partition name>");
        System.out.println("  show connection <connection name> in partition <partition name>");
        System.out.println("  start connection <connection name> in partition <partition name>");
        System.out.println("  stop connection <connection name> in partition <partition name>");
        System.out.println("  restart connection <connection name> in partition <partition name>");
        System.out.println();
        System.out.println("  show sources in partition <partition name>");
        System.out.println("  show source <source name> in partition <partition name>>");
        System.out.println("  start source <source name> in partition <partition name>");
        System.out.println("  stop source <source name> in partition <partition name>");
        System.out.println("  restart source <source name> in partition <partition name>");
        System.out.println("  search source <source name> in partition <partition name>");
        System.out.println();
        System.out.println("  show modules in partition <partition name>");
        System.out.println("  show module <module name> in partition <partition name>");
        System.out.println("  start module <module name> in partition <partition name>");
        System.out.println("  stop module <module name> in partition <partition name>");
        System.out.println("  restart module <module name> in partition <partition name>");
        System.out.println("  invoke method <method name> in module <module name> in partition <partition name> [with <parameter>...]");
        System.out.println();
        System.out.println("  show jobs in partition <partition name>");
        System.out.println("  show job <job name> in partition <partition name>");
        System.out.println("  start job <job name> in partition <partition name>");
        System.out.println("  stop job <job name> in partition <partition name>");
        System.out.println("  restart job <job name> in partition <partition name>");
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

        Getopt getopt = new Getopt("Client", args, "-:?dvt:h:p:r:P:D:w:", longopts);

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
            Client.showUsage();
            System.exit(0);
        }

        File serviceHome = new File(System.getProperty("org.safehaus.penrose.management.home"));
        
        //Logger rootLogger = Logger.getRootLogger();
        //rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");

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
            Client client = new Client(
                    serverType,
                    protocol,
                    hostname,
                    portNumber,
                    bindDn,
                    bindPassword
            );

            client.setRmiTransportPort(rmiTransportPort);
            client.connect();

            client.execute(parameters);

            client.close();

        } catch (SecurityException e) {
            log.error(e.getMessage());
            
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
