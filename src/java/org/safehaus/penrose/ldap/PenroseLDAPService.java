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

import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.config.PenroseConfig;
import org.apache.ldap.server.configuration.MutableServerStartupConfiguration;
import org.apache.ldap.server.configuration.MutableAuthenticatorConfiguration;
import org.apache.ldap.server.configuration.MutableInterceptorConfiguration;
import org.apache.ldap.server.configuration.SyncConfiguration;
import org.apache.ldap.server.jndi.ServerContextFactory;
import org.apache.ldap.server.schema.bootstrap.*;
import org.apache.log4j.Logger;

import javax.naming.Context;
import javax.naming.directory.InitialDirContext;
import java.io.File;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PenroseLDAPService {

    public Logger log = Logger.getLogger(PenroseLDAPService.class);

    private String homeDirectory;
    private Penrose penrose;

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }

    public void start() throws Exception {

        PenroseConfig penroseConfig = penrose.getConfig();

        MutableServerStartupConfiguration configuration =  new MutableServerStartupConfiguration();

        // Configure LDAP ports
        configuration.setLdapPort(penroseConfig.getPort());
        configuration.setLdapsPort(penroseConfig.getSecurePort());

        // Configure working directory
        String workingDirectory = (homeDirectory == null ? "" : homeDirectory+File.separator)+"var"+File.separator+"data";
        configuration.setWorkingDirectory(new File(workingDirectory));

        // Configure bootstrap schemas
        Set bootstrapSchemas = new HashSet();
        bootstrapSchemas.add(new AutofsSchema());
        bootstrapSchemas.add(new CorbaSchema());
        bootstrapSchemas.add(new CoreSchema());
        bootstrapSchemas.add(new CosineSchema());
        bootstrapSchemas.add(new ApacheSchema());
        bootstrapSchemas.add(new CollectiveSchema());
        bootstrapSchemas.add(new InetorgpersonSchema());
        bootstrapSchemas.add(new JavaSchema());
        bootstrapSchemas.add(new Krb5kdcSchema());
        bootstrapSchemas.add(new NisSchema());
        bootstrapSchemas.add(new SystemSchema());
        bootstrapSchemas.add(new ApachednsSchema());
        configuration.setBootstrapSchemas(bootstrapSchemas);

        // Register Penrose authenticator
        PenroseAuthenticator authenticator = new PenroseAuthenticator();
        authenticator.setPenrose(penrose);

        MutableAuthenticatorConfiguration authenticatorConfig = new MutableAuthenticatorConfiguration();
        authenticatorConfig.setName("penrose");
        authenticatorConfig.setAuthenticator(authenticator);

        Set authenticators = configuration.getAuthenticatorConfigurations();
        authenticators.add(authenticatorConfig);
        configuration.setAuthenticatorConfigurations(authenticators);

        // Register Penrose interceptor
        PenroseInterceptor interceptor = new PenroseInterceptor();
        interceptor.setPenrose(penrose);

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
        env.setProperty(Context.SECURITY_PRINCIPAL, penroseConfig.getRootDn());
        env.setProperty(Context.SECURITY_CREDENTIALS, penroseConfig.getRootPassword());
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

        log.warn("Listening to port "+penroseConfig.getPort()+".");

        // Start ApacheDS synchronization thread
        Thread thread = new Thread() {
            public void run() {
                try {
                    env.putAll(new SyncConfiguration().toJndiEnvironment());
                    while (true) {
                        try {
                            Thread.sleep( 20000 );
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
    }

    public void stop() throws Exception {

    }

    public String getHomeDirectory() {
        return homeDirectory;
    }

    public void setHomeDirectory(String homeDirectory) {
        this.homeDirectory = homeDirectory;
    }
}
