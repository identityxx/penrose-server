package org.safehaus.penrose.mapping;

import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.client.PenroseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endi Sukma Dewata
 */
public class MappingClient extends BaseClient implements MappingServiceMBean {

    public static Logger log = LoggerFactory.getLogger(MappingClient.class);

    protected String partitionName;

    public MappingClient(PenroseClient client, String partitionName, String name) throws Exception {
        super(client, name, getStringObjectName(partitionName, name));

        this.partitionName = partitionName;
    }

    public MappingConfig getMappingConfig() throws Exception {
        return (MappingConfig)getAttribute("MappingConfig");
    }

    public static String getStringObjectName(String partitionName, String mappingName) {
        return "Penrose:type=Mapping,partition="+partitionName+",name="+mappingName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }
}