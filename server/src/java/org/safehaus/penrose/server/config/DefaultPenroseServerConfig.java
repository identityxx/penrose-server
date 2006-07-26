package org.safehaus.penrose.server.config;

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.ldap.PenroseLDAPService;

/**
 * @author Endi S. Dewata
 */
public class DefaultPenroseServerConfig extends PenroseServerConfig {

    public DefaultPenroseServerConfig() {

        addServiceConfig(new ServiceConfig("JMX", PenroseJMXService.class.getName()));
        addServiceConfig(new ServiceConfig("LDAP", PenroseLDAPService.class.getName()));
    }
}
