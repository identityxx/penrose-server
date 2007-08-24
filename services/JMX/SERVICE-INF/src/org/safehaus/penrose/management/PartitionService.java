package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.source.Source;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionService implements PartitionServiceMBean {

    private PenroseJMXService service;
    private Partition partition;

    public PartitionService() {
    }

    public String getName() {
        return partition.getName();
    }
    
    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public SourceService getSourceService(String sourceName) {
        Source source = partition.getSource(sourceName);
        if (source == null) return null;

        SourceService sourceService = new SourceService();
        sourceService.setService(service);
        sourceService.setPartition(partition);
        sourceService.setSource(source);

        return sourceService;
    }
    
    public Collection<SourceService> getSourceServices() {

        Collection<SourceService> list = new ArrayList<SourceService>();

        for (Source source : partition.getSources()) {
            SourceService sourceService = new SourceService();
            sourceService.setService(service);
            sourceService.setPartition(partition);
            sourceService.setSource(source);
            list.add(sourceService);
        }

        return list;
    }

    public String getObjectName() {
        return PartitionClient.getObjectName(partition.getName());
    }

    public void register() throws Exception {
        service.register(getObjectName(), this);

        for (SourceService sourceService : getSourceServices()) {
            sourceService.register();
        }
    }

    public void unregister() throws Exception {
        for (SourceService sourceService : getSourceServices()) {
            sourceService.unregister();
        }

        service.unregister(getObjectName());
    }

    public PenroseJMXService getService() {
        return service;
    }

    public void setService(PenroseJMXService service) {
        this.service = service;
    }
}
