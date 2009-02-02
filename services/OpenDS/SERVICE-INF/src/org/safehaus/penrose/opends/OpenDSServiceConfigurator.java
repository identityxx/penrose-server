package org.safehaus.penrose.opends;

import org.safehaus.penrose.service.ServiceConfigurator;
import org.safehaus.penrose.config.Parameter;
import org.opends.server.extensions.ConfigFileHandler;
import org.opends.server.types.*;
import org.opends.server.core.DirectoryServer;

import java.io.File;
import java.util.LinkedHashSet;

/**
 * @author Endi Sukma Dewata
 */
public class OpenDSServiceConfigurator extends ServiceConfigurator {

    ConfigFileHandler configHandler = new ConfigFileHandler();

    Attribute ldapEnabledAttribute;
    Attribute ldapPortAttribute;

    Attribute ldapsEnabledAttribute;
    Attribute ldapsPortAttribute;
    Attribute keyManagerAttribute;
    Attribute sslCertificateNameAttribute;

    public void init() throws Exception {

        DirectoryServer.bootstrapClient();
        DirectoryServer.initializeJMX();

        File configFile = new File(serviceDir, "config"+File.separator+"config.ldif");

        configHandler.initializeConfigHandler(configFile.getAbsolutePath(), false);

        Entry ldapEntry = configHandler.getConfigEntry(org.opends.server.types.DN.decode("cn=LDAP Connection Handler,cn=Connection Handlers,cn=config")).getEntry();

        ldapEnabledAttribute = ldapEntry.getAttribute("ds-cfg-enabled").get(0);
        String ldapEnabled = ldapEnabledAttribute.getValues().iterator().next().getStringValue();
        if (ldapEnabled == null) ldapEnabled = "true";

        addParameter(new Parameter("ldapEnabled", "LDAP Enabled", ldapEnabled));

        ldapPortAttribute = ldapEntry.getAttribute("ds-cfg-listen-port").get(0);
        String ldapPort = ldapPortAttribute.getValues().iterator().next().getStringValue();
        if (ldapPort == null) ldapPort = "10389";

        addParameter(new Parameter("ldapPort", "LDAP Port", ldapPort));

        Entry ldapsEntry = configHandler.getConfigEntry(DN.decode("cn=LDAPS Connection Handler,cn=Connection Handlers,cn=config")).getEntry();

        ldapsEnabledAttribute = ldapsEntry.getAttribute("ds-cfg-enabled").get(0);
        String ldapsEnabled = ldapsEnabledAttribute.getValues().iterator().next().getStringValue();
        if (ldapsEnabled == null) ldapsEnabled = "true";

        addParameter(new Parameter("ldapsEnabled", "Secure LDAP Enabled", ldapsEnabled));

        ldapsPortAttribute = ldapsEntry.getAttribute("ds-cfg-listen-port").get(0);
        String ldapsPort = ldapsPortAttribute.getValues().iterator().next().getStringValue();
        if (ldapsPort == null) ldapsPort = "10636";

        addParameter(new Parameter("ldapsPort", "Secure LDAP Port", ldapsPort));

        sslCertificateNameAttribute = ldapsEntry.getAttribute("ds-cfg-ssl-cert-nickname").get(0);
        String sslCertificateName = sslCertificateNameAttribute.getValues().iterator().next().getStringValue();
        if (sslCertificateName == null) sslCertificateName = "server-cert";

        addParameter(new Parameter("sslCertificateName", "SSL Certificate Name", sslCertificateName));

        keyManagerAttribute = ldapsEntry.getAttribute("ds-cfg-key-manager-provider").get(0);
        String keyManagerDn = keyManagerAttribute.getValues().iterator().next().getStringValue();
        int i = keyManagerDn.indexOf("=");
        int j = keyManagerDn.indexOf(",", i+1);
        String keyStoreType = keyManagerDn.substring(i+1, j);
        if (keyStoreType == null) keyStoreType = "JKS";

        Parameter keyStoreTypeParameter = new Parameter("keyStoreType", "Key Store Type (JKS/PKCS12)", keyStoreType);
        keyStoreTypeParameter.addOption("JKS");
        keyStoreTypeParameter.addOption("PKCS12");
        addParameter(keyStoreTypeParameter);

        Entry keyManagerEntry = configHandler.getConfigEntry(DN.decode(keyManagerDn)).getEntry();
        Attribute keyStoreFileAttribute = keyManagerEntry.getAttribute("ds-cfg-key-store-file").get(0);
        Attribute keyStorePinFileAttribute = keyManagerEntry.getAttribute("ds-cfg-key-store-pin-file").get(0);

        String keyStoreFile = keyStoreFileAttribute.getValues().iterator().next().getStringValue();
        if (keyStoreFile == null) keyStoreFile = "config/penrose.keystore";

        addParameter(new Parameter("keyStoreFile", "Key Store File", keyStoreFile));

        String keyStorePinFile = keyStorePinFileAttribute.getValues().iterator().next().getStringValue();
        if (keyStorePinFile == null) keyStorePinFile = "config/keystore.pin";

        addParameter(new Parameter("keyStorePinFile", "Key Store PIN File", keyStorePinFile));
    }

    public void setParameterValue(Parameter parameter, String value) throws Exception {

        if (parameter.getName().equals("ldapEnabled")) {

            LinkedHashSet<AttributeValue> values = ldapEnabledAttribute.getValues();
            values.clear();
            values.add(new AttributeValue(ldapEnabledAttribute.getAttributeType(), value));

        } else if (parameter.getName().equals("ldapPort")) {

            LinkedHashSet<AttributeValue> values = ldapPortAttribute.getValues();
            values.clear();
            values.add(new AttributeValue(ldapPortAttribute.getAttributeType(), value));

        } else if (parameter.getName().equals("ldapsEnabled")) {

            LinkedHashSet<AttributeValue> values = ldapsEnabledAttribute.getValues();
            values.clear();
            values.add(new AttributeValue(ldapsEnabledAttribute.getAttributeType(), value));

        } else if (parameter.getName().equals("ldapsPort")) {

            LinkedHashSet<AttributeValue> values = ldapsPortAttribute.getValues();
            values.clear();
            values.add(new AttributeValue(ldapsPortAttribute.getAttributeType(), value));

        } else if (parameter.getName().equals("sslCertificateName")) {

            LinkedHashSet<AttributeValue> values = sslCertificateNameAttribute.getValues();
            values.clear();
            values.add(new AttributeValue(sslCertificateNameAttribute.getAttributeType(), value));

        } else if (parameter.getName().equals("keyStoreType")) {

            LinkedHashSet<AttributeValue> values = keyManagerAttribute.getValues();

            String oldKeyManagerDn = values.iterator().next().getStringValue();
            int i = oldKeyManagerDn.indexOf("=");
            int j = oldKeyManagerDn.indexOf(",", i+1);
            String newKeyManagerDn = oldKeyManagerDn.substring(0, i+1)+value+oldKeyManagerDn.substring(j);

            values.clear();
            values.add(new AttributeValue(keyManagerAttribute.getAttributeType(), newKeyManagerDn));

            for (String keyStoreType : parameter.getOptions()) {

                String keyManagerDn = oldKeyManagerDn.substring(0, i+1)+keyStoreType+oldKeyManagerDn.substring(j);

                Entry keyManagerEntry = configHandler.getConfigEntry(DN.decode(keyManagerDn)).getEntry();
                Attribute enabledAttribute = keyManagerEntry.getAttribute("ds-cfg-enabled").get(0);

                LinkedHashSet<AttributeValue> enabledValues = enabledAttribute.getValues();
                enabledValues.clear();
                enabledValues.add(new AttributeValue(enabledAttribute.getAttributeType(), keyStoreType.equals(value) ? "true" : "false"));
            }

        } else if (parameter.getName().equals("keyStoreFile")) {

            String keyManagerDn = keyManagerAttribute.getValues().iterator().next().getStringValue();
            Entry keyManagerEntry = configHandler.getConfigEntry(DN.decode(keyManagerDn)).getEntry();
            Attribute keyStoreFileAttribute = keyManagerEntry.getAttribute("ds-cfg-key-store-file").get(0);

            LinkedHashSet<AttributeValue> values = keyStoreFileAttribute.getValues();
            values.clear();
            values.add(new AttributeValue(keyStoreFileAttribute.getAttributeType(), value));

        } else if (parameter.getName().equals("keyStorePinFile")) {

            String keyManagerDn = keyManagerAttribute.getValues().iterator().next().getStringValue();
            Entry keyManagerEntry = configHandler.getConfigEntry(DN.decode(keyManagerDn)).getEntry();
            Attribute keyStorePinFileAttribute = keyManagerEntry.getAttribute("ds-cfg-key-store-pin-file").get(0);

            LinkedHashSet<AttributeValue> values = keyStorePinFileAttribute.getValues();
            values.clear();
            values.add(new AttributeValue(keyStorePinFileAttribute.getAttributeType(), value));
        }
    }

    public void close() throws Exception {
        configHandler.writeUpdatedConfig();
    }
}
