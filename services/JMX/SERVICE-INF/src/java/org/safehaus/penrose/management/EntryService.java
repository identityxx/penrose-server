package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntryMapping;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.DN;

import javax.management.StandardMBean;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class EntryService extends StandardMBean implements EntryServiceMBean {

    private PenroseJMXService jmxService;
    private Partition partition;
    private Entry entry;

    public EntryService() throws Exception {
        super(EntryServiceMBean.class);
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public Entry getEntry() {
        return entry;
    }

    public void setEntry(Entry entry) {
        this.entry = entry;
    }

    public DN getDn() throws Exception {
        return entry.getDn();
    }
    
    public EntryMapping getEntryMapping() throws Exception {
        return entry.getEntryMapping();
    }

    public Collection<String> getChildIds(RDN rdn) {
        Collection<String> list = new ArrayList<String>();
        for (Entry child : entry.getChildren(rdn)) {
            list.add(child.getId());
        }
        return list;
    }

    public String getObjectName() {
        return EntryClient.getObjectName(partition.getName(), entry.getId());
    }

    public PenroseJMXService getJmxService() {
        return jmxService;
    }

    public void setJmxService(PenroseJMXService jmxService) {
        this.jmxService = jmxService;
    }

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);
    }

    public void unregister() throws Exception {
        jmxService.unregister(getObjectName());
    }
}
