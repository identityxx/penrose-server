/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose;


import java.util.Properties;
import java.io.File;

import javax.naming.Context;
import javax.naming.directory.InitialDirContext;

import org.apache.ldap.server.configuration.SyncConfiguration;
import org.apache.ldap.server.configuration.MutableServerStartupConfiguration;
import org.apache.ldap.server.jndi.ServerContextFactory;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


/**
 * @author Endi S. Dewata
 */
public class PenroseServer {

    Logger log = LoggerFactory.getLogger(getClass());

    Properties env;

    public PenroseServer() throws Exception {
    }

    public void run() throws Exception {

        String homeDirectory = System.getProperty("penrose.home");

        File log4jProperties = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"log4j.properties");
        if (log4jProperties.exists()) {
            PropertyConfigurator.configure(log4jProperties.getAbsolutePath());
        }

        String config = (homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"apacheds.xml";

        log.info("Loading server configuration "+config);

        ApplicationContext factory = new FileSystemXmlApplicationContext(config);

        MutableServerStartupConfiguration cfg = (MutableServerStartupConfiguration)factory.getBean("configuration");
        cfg.setWorkingDirectory(new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"var"+File.separator+"data"));

        env = (Properties)factory.getBean("environment");
        env.setProperty(Context.PROVIDER_URL, "ou=system");
        env.setProperty(Context.INITIAL_CONTEXT_FACTORY, ServerContextFactory.class.getName() );
        env.putAll(cfg.toJndiEnvironment());

        Penrose penrose = (Penrose)factory.getBean("penrose");
        penrose.setHomeDirectory(homeDirectory);
        penrose.setRootDn(env.getProperty(Context.SECURITY_PRINCIPAL));
        penrose.setRootPassword(env.getProperty(Context.SECURITY_CREDENTIALS));
        penrose.init();

        new InitialDirContext(env);

        log.info("Penrose Server is ready.");

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

    public static void main( String[] args ) throws Exception {

        PenroseServer server = new PenroseServer();
        server.run();
    }
}
