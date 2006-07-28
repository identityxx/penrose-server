package org.safehaus.penrose.server;

import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.ldap.PenroseLDAPService;
import org.safehaus.penrose.config.PenroseServerConfig;

/**
 * @author Endi S. Dewata
 */
public class DefaultPenroseServerConfig extends PenroseServerConfig {

    public DefaultPenroseServerConfig() {

        addServiceConfig(new ServiceConfig("JMX", PenroseJMXService.class.getName()));
        addServiceConfig(new ServiceConfig("LDAP", PenroseLDAPService.class.getName()));
    }
}
