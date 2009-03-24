package org.safehaus.penrose.nis;

import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.synchronization.SynchronizationModuleClient;

import java.util.Map;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class NISSynchronizationModuleClient extends SynchronizationModuleClient implements NISSynchronizationModuleMBean {

    public NISSynchronizationModuleClient(PenroseClient client, String partitionName, String name) throws Exception {
        super(client, partitionName, name);
    }

    public Map<String,String> getNisMapRDNs() throws Exception {
        return (Map<String,String>)getAttribute("NisMapRDNs");
    }

    public Collection<String> getNisMaps() throws Exception {
        return (Collection<String>)getAttribute("NisMaps");
    }

    public String getNisMapRDN(String nisMap) throws Exception {
        return (String)invoke(
                "getNisMapRDN",
                new Object[] { nisMap },
                new String[] { String.class.getName() }
        );
    }
}