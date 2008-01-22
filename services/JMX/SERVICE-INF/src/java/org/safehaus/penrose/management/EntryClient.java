package org.safehaus.penrose.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.directory.EntryMapping;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.DN;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class EntryClient implements EntryServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseClient client;
    private String partitionName;
    private String name;

    MBeanServerConnection connection;
    ObjectName objectName;

    public EntryClient(PenroseClient client, String partitionName, String name) throws Exception {
        this.client = client;
        this.partitionName = partitionName;
        this.name = name;

        connection = client.getConnection();
        objectName = ObjectName.getInstance(getObjectName(partitionName, name));
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DN getDn() throws Exception {
        return (DN)getAttribute("Dn");
    }

    public EntryMapping getEntryMapping() throws Exception {
        return (EntryMapping)getAttribute("EntryMapping");
    }

    public Collection<String> getChildIds(RDN rdn) throws Exception {
        return (Collection<String>)invoke(
                "getChildIds",
                new Object[] { rdn },
                new String[] { RDN.class.getName() }
        );
    }

    public static String getObjectName(String partitionName, String entryName) {
        return "Penrose:type=entry,partition="+partitionName+",name="+entryName;
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public Object getAttribute(String attributeName) throws Exception {

        log.debug("Getting attribute "+ attributeName +" from "+name+".");

        return connection.getAttribute(
                objectName,
                attributeName
        );
    }

    public Object invoke(String method, Object[] paramValues, String[] paramClassNames) throws Exception {

        log.debug("Invoking method "+method+"() on "+name+".");

        return connection.invoke(
                objectName,
                method,
                paramValues,
                paramClassNames
        );
    }
}
