/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
package org.safehaus.penrose;


import java.util.*;
import java.io.File;
import java.io.Reader;
import java.io.BufferedReader;
import java.io.FileReader;

import javax.naming.Context;
import javax.naming.directory.InitialDirContext;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.apache.ldap.server.configuration.SyncConfiguration;
import org.apache.ldap.server.configuration.MutableServerStartupConfiguration;
import org.apache.ldap.server.configuration.MutableAuthenticatorConfiguration;
import org.apache.ldap.server.configuration.MutableInterceptorConfiguration;
import org.apache.ldap.server.jndi.ServerContextFactory;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.apache.log4j.Logger;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.apacheds.PenroseAuthenticator;
import org.safehaus.penrose.apacheds.PenroseInterceptor;
import mx4j.log.Log4JLogger;
import mx4j.tools.config.ConfigurationLoader;
import sun.misc.Signal;
import sun.misc.SignalHandler;


/**
 * @author Endi S. Dewata
 */
public class PenroseServer implements SignalHandler {

    public static Logger log = Logger.getLogger(PenroseServer.class);

    Properties env;

    String homeDirectory;
    Penrose penrose;

    public PenroseServer(String homeDirectory) throws Exception {
        this.homeDirectory = homeDirectory;
    }

    public void run() throws Exception {

        String config = (homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"apacheds.xml";
        String workingDirectory = (homeDirectory == null ? "" : homeDirectory+File.separator)+"var"+File.separator+"data";

        File file = new File(config);

        ApplicationContext factory = new FileSystemXmlApplicationContext("file:///"+file.getAbsolutePath());
        env = (Properties)factory.getBean("environment");

        MutableServerStartupConfiguration cfg = (MutableServerStartupConfiguration)factory.getBean("configuration");
        cfg.setWorkingDirectory(new File(workingDirectory));

        penrose = new Penrose();
        penrose.setHomeDirectory(homeDirectory);
        penrose.setRootDn(env.getProperty(Context.SECURITY_PRINCIPAL));
        penrose.setRootPassword(env.getProperty(Context.SECURITY_CREDENTIALS));
        penrose.init();

        PenroseAuthenticator authenticator = new PenroseAuthenticator();
        authenticator.setPenrose(penrose);

        MutableAuthenticatorConfiguration authenticatorConfig = new MutableAuthenticatorConfiguration();
        authenticatorConfig.setName("penrose");
        authenticatorConfig.setAuthenticator(authenticator);

        Set authenticators = cfg.getAuthenticatorConfigurations();
        authenticators.add(authenticatorConfig);
        cfg.setAuthenticatorConfigurations(authenticators);

        PenroseInterceptor interceptor = new PenroseInterceptor();
        interceptor.setPenrose(penrose);

        MutableInterceptorConfiguration interceptorConfig = new MutableInterceptorConfiguration();
        interceptorConfig.setName("penroseService");
        interceptorConfig.setInterceptor(interceptor);

        List interceptors = new ArrayList();
        interceptors.add(interceptorConfig);
        interceptors.addAll(cfg.getInterceptorConfigurations());
        cfg.setInterceptorConfigurations(interceptors);

        env.setProperty(Context.PROVIDER_URL, "ou=system");
        env.setProperty(Context.INITIAL_CONTEXT_FACTORY, ServerContextFactory.class.getName() );
        env.putAll(cfg.toJndiEnvironment());

        new InitialDirContext(env);
    }

    public void loop() throws Exception {
        while (true) {
            try {
                Thread.sleep( 20000 );
            } catch ( InterruptedException e ) {
                // ignore
            }

            env.putAll(new SyncConfiguration().toJndiEnvironment());
            new InitialDirContext(env);
        }
    }

    public void runJmx() {

        File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"mx4j.xml");
        if (!file.exists()) return;

        // Register JMX
        MBeanServer server = null;
        try {
            ArrayList servers = MBeanServerFactory.findMBeanServer(null);
            server = (MBeanServer) servers.get(0);

        } catch (Exception ex) {
            //log.debug("Default MBeanServer has not been created yet.");
        }

        if (server == null) {
            try {
                log.debug("Creating MBeanServer.");

                // MX4J's logging redirection to Apache's Commons Logging
                mx4j.log.Log.redirectTo(new Log4JLogger());

                // Create the MBeanServer
                server = MBeanServerFactory.createMBeanServer();

                // Create the ConfigurationLoader
                ConfigurationLoader loader = new ConfigurationLoader();

                // Register the configuration loader into the MBeanServer
                ObjectName name = ObjectName.getInstance(":service=configuration");
                server.registerMBean(loader, name);

                // Tell the configuration loader the XML configuration file
                Reader reader = new BufferedReader(new FileReader(file));
                loader.startup(reader);
                reader.close();

                //log.debug("Done creating MBeanServer.");

            } catch (Exception ex) {
                log.error(ex.toString(), ex);
            }
        }

        if (server != null) {
            try {
                server.registerMBean(penrose, ObjectName.getInstance(PenroseClient.MBEAN_NAME));
                //server.registerMBean(engine, ObjectName.getInstance("Penrose:type=Engine"));
                //server.registerMBean(connectionPool, ObjectName.getInstance("Penrose:type=PenroseConnectionPool"));
                //server.registerMBean(threadPool, ObjectName.getInstance("Penrose:type=ThreadPool"));
            } catch (Exception ex) {
                log.error(ex.toString(), ex);
            }
        }

    }

    /**
     * Ctrl-C (Interrupt Signal) handler
     *
     * The obvious drawback with this is that it relies on undocumented classes from
     * Sun. There are other solutions, including one given at
     * http://www.naturalbridge.com/useful/index.html and another at
     * http://interstice.com/~kevinh/projects/javasignals/ but both of these use
     * native code.
     */
    public void initSignalHandler() {
        Signal.handle(new Signal("INT"), this);
    }

    public void handle(Signal sig) {
        //code to be executed goes here
        log.info("Interrupt Signal (Ctrl-C) caught! Initiating shutdown...");
        listAllThreads();
        penrose.stop();
    }

    public void listAllThreads() {
        // Find the root thread group
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        log.debug(".. ThreadGroup: "+root.getName());

        while (root.getParent() != null) {
            root = root.getParent();
            log.debug(".. ThreadGroup: "+root.getName());
        }

        // Visit each thread group
        visit(root, 0);
    }

    /**
     * This method recursively visits all thread groups under `group'.
     */
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
            StringBuffer sb = new StringBuffer();
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

    public static void main( String[] args ) throws Exception {

        try {
            String home = System.getProperty("penrose.home");

            log.debug("PENROSE_HOME: "+home);

            File log4jProperties = new File((home == null ? "" : home+File.separator)+"conf"+File.separator+"log4j.properties");
            if (log4jProperties.exists()) {
                log.debug("Loading "+log4jProperties.getPath());
                PropertyConfigurator.configure(log4jProperties.getAbsolutePath());
            }

            PenroseServer server = new PenroseServer(home);
            server.run();
            server.runJmx();

            log.info("Penrose Server 0.9.8 is ready.");

            server.loop();

        } catch (Exception e) {
            String name = e.getClass().getName();
            name = name.substring(name.lastIndexOf(".")+1);
            log.debug(name, e);
            log.error("Penrose Server failed to start: "+name+": "+e.getMessage());
            System.exit(1);
        }
    }
}
