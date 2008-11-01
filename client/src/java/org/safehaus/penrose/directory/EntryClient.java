package org.safehaus.penrose.directory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.client.PenroseClient;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class EntryClient extends BaseClient implements EntryServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected String partitionName;

    public EntryClient(PenroseClient client, String partitionName, String name) throws Exception {
        super(client, name, getStringObjectName(partitionName, name));

        this.partitionName = partitionName;
    }

    public DN getDn() throws Exception {
        return (DN)getAttribute("Dn");
    }

    public EntryConfig getEntryConfig() throws Exception {
        return (EntryConfig)getAttribute("EntryConfig");
    }

    public String getParentId() throws Exception {
        return (String)getAttribute("ParentId");
    }

    public Collection<String> getChildIds() throws Exception {
        return (Collection<String>)getAttribute("ChildIds");
    }

    public static String getStringObjectName(String partitionName, String entryName) {
        return "Penrose:type=entry,partition="+partitionName+",name="+entryName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }
}
