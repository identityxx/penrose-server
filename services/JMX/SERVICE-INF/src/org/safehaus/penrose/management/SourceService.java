package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceSync;

/**
 * @author Endi Sukma Dewata
 */
public class SourceService implements SourceServiceMBean {

    private PenroseJMXService service;
    private Partition partition;
    private Source source;

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Long getCount() throws Exception {
        return source.getCount();
    }

    public void createCache() throws Exception {
        SourceSync sourceSync = partition.getSourceSync(source.getName());
        sourceSync.create();
    }

    public void loadCache() throws Exception {
        SourceSync sourceSync = partition.getSourceSync(source.getName());
        sourceSync.load();
    }

    public void cleanCache() throws Exception {
        SourceSync sourceSync = partition.getSourceSync(source.getName());
        sourceSync.clean();
    }

    public void dropCache() throws Exception {
        SourceSync sourceSync = partition.getSourceSync(source.getName());
        sourceSync.drop();
    }

    public String getObjectName() {
        return SourceClient.getObjectName(partition.getName(), source.getName());
    }

    public PenroseJMXService getService() {
        return service;
    }

    public void setService(PenroseJMXService service) {
        this.service = service;
    }

    public void register() throws Exception {
        service.register(getObjectName(), this);
    }

    public void unregister() throws Exception {
        service.unregister(getObjectName());

    }
}
