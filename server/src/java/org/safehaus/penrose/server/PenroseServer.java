/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.server;

import java.util.*;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.log4j.*;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.safehaus.penrose.service.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.config.*;

/**
 * @author Endi S. Dewata
 */
public class PenroseServer {

    public static Logger log = Logger.getLogger(PenroseServer.class);
    public static org.slf4j.Logger errorLog = org.safehaus.penrose.log.Error.log;
    public static boolean debug = log.isDebugEnabled();

    private PenroseConfig penroseConfig;
    private Penrose penrose;

    private ServiceConfigs serviceConfigs;
    private Services services;

    public PenroseServer() throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose();

        init();
    }

    public PenroseServer(String home) throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(home);

        init();
    }

    public PenroseServer(File home) throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(home);

        init();
    }

    public PenroseServer(String home, PenroseConfig penroseConfig) throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(home, penroseConfig);

        init();
    }

    public PenroseServer(File home, PenroseConfig penroseConfig) throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(home, penroseConfig);

        init();
    }

    public PenroseServer(PenroseConfig penroseConfig) throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);

        init();
    }

    public void init() throws Exception {
        
        penroseConfig = penrose.getPenroseConfig();

        File home = penrose.getHome();
        File servicesDir = new File(home, "services");

        serviceConfigs = new ServiceConfigs(servicesDir);
        services = new Services(servicesDir);
    }

    public void start() throws Exception {

        log.debug("Starting Penrose Server...");

        penrose.start();

        log.debug("----------------------------------------------------------------------------------");

        for (String serviceName : serviceConfigs.getAvailableServiceNames()) {

            try {
                startService(serviceName);

            } catch (Exception e) {
                errorLog.error(e.getMessage(), e);
            }
        }

        log.fatal("Server is ready.");
    }

    public void stop() throws Exception {

        log.debug("----------------------------------------------------------------------------------");
        log.debug("Stopping Penrose Server...");

        for (String serviceName : serviceConfigs.getAvailableServiceNames()) {

            try {
                stopService(serviceName);

            } catch (Exception e) {
                errorLog.error(e.getMessage(), e);
            }
        }

        penrose.stop();

        log.fatal("Server has been shutdown.");
    }

    public void startService(String serviceName) throws Exception {

        log.debug("Loading "+serviceName+" service.");

        ServiceConfig serviceConfig = serviceConfigs.load(serviceName);
        serviceConfigs.addServiceConfig(serviceConfig);
        
        if (!serviceConfig.isEnabled()) {
            log.debug(serviceConfig.getName()+" service is disabled.");
            return;
        }

        log.debug("Starting "+serviceName+" service.");

        File serviceDir = new File(serviceConfigs.getServicesDir(), serviceName);

        Collection<URL> classPaths = serviceConfig.getClassPaths();
        URLClassLoader classLoader = new URLClassLoader(classPaths.toArray(new URL[classPaths.size()]), getClass().getClassLoader());

        Class clazz = classLoader.loadClass(serviceConfig.getServiceClass());

        Service service = (Service)clazz.newInstance();

        ServiceContext serviceContext = new ServiceContext();
        serviceContext.setPath(serviceDir);
        serviceContext.setPenroseServer(this);
        serviceContext.setClassLoader(classLoader);

        service.init(serviceConfig, serviceContext);


        services.addService(service);
    }
    
    public void stopService(String serviceName) throws Exception {

        log.debug("Stopping "+serviceName+" service.");

        Service service = services.getService(serviceName);
        if (service == null) return;

        service.destroy();

        services.removeService(serviceName);
        serviceConfigs.removeServiceConfig(serviceName);
    }

    public String getServiceStatus(String serviceName) {

        Service service = services.getService(serviceName);

        if (service == null) {
            return "STOPPED";
        } else {
            return "STARTED";
        }
    }

    public String getStatus() {
        return penrose.getStatus();
    }

    public File getHome() {
        return penrose.getHome();
    }

    public void listAllThreads() {
        // Find the root thread group
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        log.debug("ThreadGroup: "+root.getName());

        while (root.getParent() != null) {
            root = root.getParent();
            log.debug("ThreadGroup: "+root.getName());
        }

        // Visit each thread group
        visit(root, 0);
    }

    private void visit(ThreadGroup group, int level) {
        //logger.debug(group.toString());

        // Get threads in 'group'
        int numThreads = group.activeCount();
        Thread[] threads = new Thread[numThreads * 2];
        numThreads = group.enumerate(threads, false);

        // Enumerate each thread in 'group'
        for (int i = 0; i < numThreads; i++) {
            // Get thread
            Thread thread = threads[i];
            StringBuilder sb = new StringBuilder();
            for (int j=0; j<level; j++) sb.append("  ");
            sb.append(thread.toString());
            log.debug(sb.toString());
        }

        // Get thread subgroups of 'group'
        int numGroups = group.activeGroupCount();
        ThreadGroup[] groups = new ThreadGroup[numGroups * 2];
        numGroups = group.enumerate(groups, false);

        // Recursively visit each subgroup
        for (int i = 0; i < numGroups; i++) {
            visit(groups[i], level + 1);
        }
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }

    public Services getServices() {
        return services;
    }

    public void setServices(Services services) {
        this.services = services;
    }

    public void reload() throws Exception {

        penrose.reload();

        services.clear();
        //serviceManager.load(penroseConfig.getServiceConfigs());
    }

    public static void main( String[] args ) throws Exception {

        try {
            Collection parameters = Arrays.asList(args);

            if (parameters.contains("-?") || parameters.contains("--help")) {
                System.out.println("Usage: org.safehaus.penrose.server.PenroseServer [OPTION]...");
                System.out.println();
                System.out.println("  -?, --help     display this help and exit");
                System.out.println("  -d             run in debug mode");
                System.out.println("  -v             run in verbose mode");
                System.out.println("      --version  output version information and exit");
                System.exit(0);
            }

            if (parameters.contains("--version")) {
                System.out.println(Penrose.PRODUCT_NAME+" Server "+Penrose.PRODUCT_VERSION);
                System.out.println(Penrose.PRODUCT_COPYRIGHT);
                System.exit(0);
            }

            String homeDirectory = System.getProperty("penrose.home");

            File log4jXml = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"log4j.xml");

            Logger rootLogger = Logger.getRootLogger();
            rootLogger.setLevel(Level.OFF);

            Logger logger = Logger.getLogger("org.safehaus.penrose");

            if (parameters.contains("-d")) {
                logger.setLevel(Level.DEBUG);
                ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%-20C{1} [%4L] %m%n"));
                BasicConfigurator.configure(appender);

            } else if (parameters.contains("-v")) {
                logger.setLevel(Level.INFO);
                ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
                BasicConfigurator.configure(appender);

            } else if (log4jXml.exists()) {
                DOMConfigurator.configure(log4jXml.getAbsolutePath());

            } else {
                logger.setLevel(Level.WARN);
                ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
                BasicConfigurator.configure(appender);
            }

            log.warn("Starting "+Penrose.PRODUCT_NAME+" Server "+Penrose.PRODUCT_VERSION+".");

            String javaVersion = System.getProperty("java.version");
            log.info("Java version: "+javaVersion);

            String javaVendor = System.getProperty("java.vendor");
            log.info("Java vendor: "+javaVendor);
            
            String javaHome = System.getProperty("java.home");
            log.info("Java home: "+javaHome);

            String userDir = System.getProperty("user.dir");
            log.info("Current directory: "+userDir);

            log.info("Penrose home: "+homeDirectory);

            final PenroseServer server = new PenroseServer(homeDirectory);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    try {
                        server.stop();
                    } catch (Exception e) {
                        errorLog.error(e.getMessage(), e);
                    }
                }
            });

            server.start();

        } catch (Exception e) {
            String name = e.getClass().getName();
            name = name.substring(name.lastIndexOf(".")+1);
            log.error(e.getMessage(), e);
            log.error("Server failed to start: "+name+": "+e.getMessage());
            System.exit(1);
        }
    }

    public ServiceConfigs getServiceConfigs() {
        return serviceConfigs;
    }

    public String getProductName() {
        return Penrose.PRODUCT_NAME+" Server";
    }

    public String getProductVersion() {
        return Penrose.PRODUCT_VERSION;
    }
}
