package org.safehaus.penrose.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionClient {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseClient client;
    private String name;

    ObjectName objectName;

    public PartitionClient(PenroseClient client, String name) throws Exception {
        this.client = client;
        this.name = name;

        objectName = ObjectName.getInstance(getObjectName(name));
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SourceClient getSourceClient(String sourceName) throws Exception {
        return new SourceClient(client, name, sourceName);
    }

    public static String getObjectName(String name) {
        return "Penrose:type=partition,name="+name;
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }
}
