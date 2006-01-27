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
package org.safehaus.penrose.ldap;

import org.safehaus.penrose.service.Service;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.config.PenroseConfig;
import org.apache.ldap.server.configuration.*;
import org.apache.ldap.server.jndi.ServerContextFactory;
import org.apache.ldap.server.jndi.CoreContextFactory;

import javax.naming.Context;
import javax.naming.directory.InitialDirContext;
import java.io.File;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PenroseLDAPService extends Service {

    public final static String LDAP_PORT       = "ldapPort";
    public final static int DEFAULT_LDAP_PORT  = 10389;

    public final static String LDAPS_PORT      = "ldapsPort";
    public final static int DEFAULT_LDAPS_PORT = 10639;

    public final static String ALLOW_ANONYMOUS_ACCESS          = "allowAnonymousAccess";
    public final static boolean DEFAULT_ALLOW_ANONYMOUS_ACCESS = true;

    private int ldapPort;
    private int ldapsPort;

    private boolean allowAnonymousAccess;

    public void init() throws Exception {
        ServiceConfig serviceConfig = getServiceConfig();

        String s = serviceConfig.getParameter(LDAP_PORT);
        ldapPort = s == null ? DEFAULT_LDAP_PORT : Integer.parseInt(s);

        s = serviceConfig.getParameter(LDAPS_PORT);
        ldapsPort = s == null ? DEFAULT_LDAPS_PORT : Integer.parseInt(s);

        s = serviceConfig.getParameter(ALLOW_ANONYMOUS_ACCESS);
        allowAnonymousAccess = s == null ? DEFAULT_ALLOW_ANONYMOUS_ACCESS : Boolean.valueOf(s).booleanValue();
    }

    public void start() throws Exception {

        //log.warn("Starting LDAP Service.");

        if (ldapPort < 0) return;

        setStatus(STARTING);

        PenroseConfig penroseConfig = getPenroseServer().getPenroseConfig();
        String home = penroseConfig.getHome();

        MutableServerStartupConfiguration configuration = new MutableServerStartupConfiguration();

        // Configure LDAP ports
        configuration.setLdapPort(ldapPort);
        configuration.setLdapsPort(ldapsPort);

        // Configure working directory
        String workingDirectory = (home == null ? "" : home+File.separator)+"var"+File.separator+"data";
        configuration.setWorkingDirectory(new File(workingDirectory));

        // Configure bootstrap schemas
        Set bootstrapSchemas = new HashSet();
        for (Iterator i=penroseConfig.getSchemaConfigs().iterator(); i.hasNext(); ) {
            SchemaConfig schemaConfig = (SchemaConfig)i.next();

            String name = schemaConfig.getName();
            String className = "org.apache.ldap.server.schema.bootstrap."+
                    name.substring(0, 1).toUpperCase()+name.substring(1)+
                    "Schema";

            log.debug("Loading "+className+".");
            Class clazz = Class.forName(className);
            Object object = clazz.newInstance();
            bootstrapSchemas.add(object);
        }

        configuration.setBootstrapSchemas(bootstrapSchemas);

        configuration.setAllowAnonymousAccess(allowAnonymousAccess);

        // Register Penrose authenticator
        PenroseAuthenticator authenticator = new PenroseAuthenticator();
        authenticator.setPenrose(getPenroseServer().getPenrose());

        MutableAuthenticatorConfiguration authenticatorConfig = new MutableAuthenticatorConfiguration();
        authenticatorConfig.setName("penrose");
        authenticatorConfig.setAuthenticator(authenticator);

        Set authenticators = configuration.getAuthenticatorConfigurations();
        authenticators.add(authenticatorConfig);
        configuration.setAuthenticatorConfigurations(authenticators);
/*
        log.debug("Authenticators:");
        for (Iterator i=authenticators.iterator(); i.hasNext(); ) {
            AuthenticatorConfiguration ac = (AuthenticatorConfiguration)i.next();
            log.debug(" - "+ac.getName());
        }
*/
        // Register Penrose interceptor
        PenroseInterceptor interceptor = new PenroseInterceptor();
        interceptor.setPenrose(getPenroseServer().getPenrose());

        MutableInterceptorConfiguration interceptorConfig = new MutableInterceptorConfiguration();
        interceptorConfig.setName("penroseService");
        interceptorConfig.setInterceptor(interceptor);

        List interceptors = new ArrayList();
        interceptors.add(interceptorConfig);
        interceptors.addAll(configuration.getInterceptorConfigurations());
        configuration.setInterceptorConfigurations(interceptors);

        // Initialize ApacheDS
        final Properties env = new Properties();
        env.setProperty(Context.PROVIDER_URL, "ou=system");
        env.setProperty(Context.INITIAL_CONTEXT_FACTORY, ServerContextFactory.class.getName() );
        env.setProperty(Context.SECURITY_PRINCIPAL, penroseConfig.getRootUserConfig().getDn());
        env.setProperty(Context.SECURITY_CREDENTIALS, penroseConfig.getRootUserConfig().getPassword());
        env.setProperty(Context.SECURITY_AUTHENTICATION, "simple");

        env.setProperty("asn.1.berlib.provider", "org.apache.ldap.common.berlib.asn1.SnickersProvider");
        //env.setProperty("asn.1.berlib.provider", "org.apache.asn1new.ldap.TwixProvider");

        env.setProperty("java.naming.ldap.attributes.binary",
                "photo personalSignature audio jpegPhoto javaSerializedData "+
                "userPassword userCertificate cACertificate "+
                "authorityRevocationList certificateRevocationList crossCertificatePair "+
                "x500UniqueIdentifier krb5Key");

        env.putAll(configuration.toJndiEnvironment());

        new InitialDirContext(env);

        log.warn("Listening to port "+ldapPort+".");

        // Start ApacheDS synchronization thread
        Thread thread = new Thread() {
            public void run() {
                try {
                    env.putAll(new SyncConfiguration().toJndiEnvironment());
                    while (true) {
                        try {
                            Thread.sleep(20000);
                        } catch ( InterruptedException e ) {
                            // ignore
                        }

                        new InitialDirContext(env);
                    }
                } catch (Exception e) {
                    log.debug(e.getMessage());
                }
            }
        };

        thread.start();

        setStatus(STARTED);
    }

    public void stop() throws Exception {

        if (ldapPort < 0) return;

        setStatus(STOPPING);
        
        PenroseConfig penroseConfig = getPenroseServer().getPenroseConfig();

        Hashtable env = new ShutdownConfiguration().toJndiEnvironment();
        env.put(Context.INITIAL_CONTEXT_FACTORY, CoreContextFactory.class.getName());
        env.put(Context.PROVIDER_URL, "ou=system");
        env.put(Context.SECURITY_PRINCIPAL, penroseConfig.getRootUserConfig().getDn());
        env.put(Context.SECURITY_CREDENTIALS, penroseConfig.getRootUserConfig().getPassword());
        env.put(Context.SECURITY_AUTHENTICATION, "simple");

        new InitialDirContext(env);

        setStatus(STOPPED);

        log.warn("LDAP Service has been shutdown.");
    }

    public int getLdapPort() {
        return ldapPort;
    }

    public void setLdapPort(int ldapPort) {
        this.ldapPort = ldapPort;
    }

    public int getLdapsPort() {
        return ldapsPort;
    }

    public void setLdapsPort(int ldapsPort) {
        this.ldapsPort = ldapsPort;
    }

    public boolean isAllowAnonymousAccess() {
        return allowAnonymousAccess;
    }

    public void setAllowAnonymousAccess(boolean allowAnonymousAccess) {
        this.allowAnonymousAccess = allowAnonymousAccess;
    }
}
