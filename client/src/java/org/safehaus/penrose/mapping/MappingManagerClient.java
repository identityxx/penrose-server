package org.safehaus.penrose.mapping;

import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.client.PenroseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class MappingManagerClient extends BaseClient implements MappingManagerServiceMBean {

    public static Logger log = LoggerFactory.getLogger(MappingManagerClient.class);

    protected String partitionName;

    public MappingManagerClient(PenroseClient client, String partitionName) throws Exception {
        super(client, "MappingManager", getStringObjectName(partitionName));

        this.partitionName = partitionName;
    }

    public static String getStringObjectName(String partitionName) {
        return "Penrose:type=MappingManager,partition="+partitionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public MappingClient getMappingClient(String mappingName) throws Exception {
        return new MappingClient(client, partitionName, mappingName);
    }

    public Collection<String> getMappingNames() throws Exception {
        return (Collection<String>)getAttribute("MappingNames");
    }

    public void createMapping(MappingConfig mappingConfig) throws Exception {
        invoke(
                "createMapping",
                new Object[] { mappingConfig },
                new String[] { MappingConfig.class.getName() }
        );
    }

    public void updateMapping(String mappingName, MappingConfig mappingConfig) throws Exception {
        invoke(
                "updateMapping",
                new Object[] { mappingName, mappingConfig },
                new String[] { String.class.getName(), MappingConfig.class.getName() }
        );
    }

    public void removeMapping(String mappingName) throws Exception {
        invoke(
                "removeMapping",
                new Object[] { mappingName },
                new String[] { String.class.getName() }
        );
    }

}
