package org.safehaus.penrose.opends;

import org.safehaus.penrose.backend.PenroseBackend;
import org.safehaus.penrose.ldap.LDAPService;
import org.safehaus.penrose.ldapbackend.opends.LDAPBackendPlugin;
import org.opends.server.core.DirectoryServer;
import org.opends.server.core.PluginConfigManager;
import org.opends.server.api.ConfigHandler;
import org.opends.server.types.*;
import org.opends.server.tasks.ShutdownTaskThread;
import org.opends.server.extensions.ConfigFileHandler;
import org.opends.server.util.ServerConstants;
import org.opends.messages.MessageBuilder;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Endi S. Dewata
 */
public class OpenDSService extends LDAPService {

    String ldapPort;
    String ldapsPort;

    String configClass = ConfigFileHandler.class.getName();
    File configFile;
    File schemaPath;

    String ldapBackendPluginName = "cn=LDAP Backend,cn=Plugins,cn=config";

    public void init() throws Exception {
        super.init();

        ldapPort = getParameter("ldapPort");
        ldapsPort = getParameter("ldapsPort");

        String s = getParameter("ldapBackendPlugin");
        if (s != null) ldapBackendPluginName = s;

        File serviceDir = serviceContext.getPath();
        log.debug("Service path: "+serviceDir);

        configFile = new File(serviceDir, "config"+File.separator+"config.ldif");
        log.debug("Config file: "+configFile);

        schemaPath = new File(serviceDir, "config"+File.separator+"schema");
        log.debug("Schema path: "+schemaPath);

        try {
            File logs = new File(serviceDir, "logs");

            File pidFile = new File(logs, "server.pid");
            if (pidFile.exists()) {
                pidFile.deleteOnExit();
            }

            File startingFile = new File(logs, "server.starting");
            if (startingFile.exists()) {
                startingFile.deleteOnExit();
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        DirectoryEnvironmentConfig environmentConfig = new DirectoryEnvironmentConfig();
        environmentConfig.setProperty(ServerConstants.PROPERTY_SERVER_ROOT, serviceDir.getAbsolutePath());
        environmentConfig.setProperty(ServerConstants.PROPERTY_CONFIG_CLASS, configClass);
        environmentConfig.setProperty(ServerConstants.PROPERTY_CONFIG_FILE, configFile.getAbsolutePath());
        environmentConfig.setProperty(ServerConstants.PROPERTY_SCHEMA_DIRECTORY, schemaPath.getAbsolutePath());

        DirectoryServer directoryServer = DirectoryServer.getInstance();
        directoryServer.setEnvironmentConfig(environmentConfig);
        directoryServer.bootstrapServer();

        directoryServer.initializeConfiguration(
                configClass,
                configFile.getAbsolutePath()
        );

        ConfigHandler configHandler = DirectoryServer.getConfigHandler();

        Entry ldapEntry = configHandler.getConfigEntry(DN.decode("cn=LDAP Connection Handler,cn=Connection Handlers,cn=config")).getEntry();
        Attribute ldapPortAttribute = ldapEntry.getAttribute("ds-cfg-listen-port").get(0);
        Set<AttributeValue> ldapPortValues = ldapPortAttribute.getValues();
        AttributeValue ldapPortValue = ldapPortValues.iterator().next();

        //if (ldapPort == null) {
            ldapPort = ldapPortValue.getStringValue();
        //} else {
        //    ldapPortValues.clear();
        //    ldapPortValues.add(new AttributeValue(ldapPortAttribute.getAttributeType(), ldapPort));
        //}

        Entry ldapsEntry = configHandler.getConfigEntry(DN.decode("cn=LDAPS Connection Handler,cn=Connection Handlers,cn=config")).getEntry();
        Attribute ldapsPortAttribute = ldapsEntry.getAttribute("ds-cfg-listen-port").get(0);
        Set<AttributeValue> ldapsPortValues = ldapsPortAttribute.getValues();
        AttributeValue ldapsPortValue = ldapsPortValues.iterator().next();

        //if (ldapsPort == null) {
            ldapsPort = ldapsPortValue.getStringValue();
        //} else {
        //    ldapsPortValues.clear();
        //    ldapsPortValues.add(new AttributeValue(ldapsPortAttribute.getAttributeType(), ldapsPort));
        //}

        directoryServer.startServer();

        PluginConfigManager pluginConfigManager = DirectoryServer.getPluginConfigManager();
        LDAPBackendPlugin LDAPBackend = (LDAPBackendPlugin)pluginConfigManager.getRegisteredPlugin(DN.decode(ldapBackendPluginName));

        if (LDAPBackend != null) {
            LDAPBackend.setBackend(new PenroseBackend(serviceContext.getPenroseServer()));
        }

        Attribute ldapEnabledAttribute = ldapEntry.getAttribute("ds-cfg-enabled").get(0);
        String ldapEnabled = ldapEnabledAttribute.getValues().iterator().next().getStringValue();

        if ("true".equals(ldapEnabled)) {
            log.warn("Listening to port "+ldapPort+" (LDAP).");
        }

        Attribute ldapsEnabledAttribute = ldapsEntry.getAttribute("ds-cfg-enabled").get(0);
        String ldapsEnabled = ldapsEnabledAttribute.getValues().iterator().next().getStringValue();

        if ("true".equals(ldapsEnabled)) {
            log.warn("Listening to port "+ldapsPort+" (LDAPS).");
        }
    }

    public void destroy() throws Exception {

        ShutdownTaskThread shutdownThread = new ShutdownTaskThread(new MessageBuilder("Shutdown").toMessage());
        shutdownThread.start();
        shutdownThread.join();

        log.warn("LDAP Service has been shutdown.");
    }
}
