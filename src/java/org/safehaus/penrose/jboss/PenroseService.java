/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.jboss;

import org.apache.ldap.server.jndi.ServerContextFactory;
import org.apache.ldap.server.configuration.ServerStartupConfiguration;
import org.apache.ldap.server.configuration.MutableServerStartupConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import javax.naming.Context;
import javax.naming.directory.InitialDirContext;
import java.util.Properties;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class PenroseService implements PenroseServiceMBean {

    private String home;
    private int port;

    public void create() throws Exception {
        System.out.println("[Penrose Service] create()");
    }

    public void start() throws Exception {
        System.out.println("[Penrose Service] start()");

        System.out.println("[Penrose Service] Home: "+home);
        System.out.println("[Penrose Service] Port: "+port);

        String config = "file:"+home+File.separator+"conf"+File.separator+"apacheds.xml";
        System.out.println("[Penrose Service] Config: "+config);

        ApplicationContext factory = new FileSystemXmlApplicationContext( config );
        ServerStartupConfiguration cfg = ( ServerStartupConfiguration ) factory.getBean( "configuration" );
        Properties env = ( Properties ) factory.getBean( "environment" );

        env.setProperty( Context.PROVIDER_URL, "ou=system" );
        env.setProperty( Context.INITIAL_CONTEXT_FACTORY, ServerContextFactory.class.getName() );
        env.putAll( cfg.toJndiEnvironment() );

        new InitialDirContext( env );

    }

    public void stop() {
        System.out.println("[Penrose Service] stop()");
    }

    public void destroy() {
        System.out.println("[Penrose Service] destroy()");
    }
/*
    public String getName() {
        System.out.println("[Penrose Service] getName()");
        return "PenroseService";
    }

    public int getState() {
        System.out.println("[Penrose Service] getState()");
        return 0;
    }

    public String getStateString() {
        System.out.println("[Penrose Service] getStateString()");
        return null;
    }

    public void jbossInternalLifecycle(String s) throws Exception {
        System.out.println("[Penrose Service] jbossInternalLifecycle(\""+s+"\")");
    }
*/
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }
}
