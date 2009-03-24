package org.safehaus.penrose.synchronization;

import org.safehaus.penrose.module.ModuleClient;
import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.ldap.DN;

/**
 * @author Endi Sukma Dewata
 */
public class SynchronizationModuleClient extends ModuleClient implements SynchronizationModuleMBean {

    public SynchronizationModuleClient(PenroseClient client, String partitionName, String name) throws Exception {
        super(client, partitionName, name);
    }

    public void createBase() throws Exception {
        invoke("createBase");
    }

    public void removeBase() throws Exception {
        invoke("removeBase");
    }

    public void create(DN dn) throws Exception {
        invoke(
                "create",
                new Object[] { dn },
                new String[] { DN.class.getName() }
        );
    }

    public void clear(DN dn) throws Exception {
        invoke(
                "clear",
                new Object[] { dn },
                new String[] { DN.class.getName() }
        );
    }

    public void remove(DN dn) throws Exception {
        invoke(
                "remove",
                new Object[] { dn },
                new String[] { DN.class.getName() }
        );
    }

    public SynchronizationResult synchronize(DN dn) throws Exception {
        return (SynchronizationResult)invoke(
                "synchronize",
                new Object[] { dn },
                new String[] { DN.class.getName() }
        );
    }

    public SynchronizationResult synchronize() throws Exception {
        return (SynchronizationResult)invoke("synchronize");
    }

    public Long getSourceCount(DN dn) throws Exception {
        return (Long)invoke(
                "getSourceCount",
                new Object[] { dn },
                new String[] { DN.class.getName() }
        );
    }

    public Long getTargetCount(DN dn) throws Exception {
        return (Long)invoke(
                "getTargetCount",
                new Object[] { dn },
                new String[] { DN.class.getName() }
        );
    }
}
