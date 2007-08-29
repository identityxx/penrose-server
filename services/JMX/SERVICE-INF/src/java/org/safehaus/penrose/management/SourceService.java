package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceSync;

import javax.management.StandardMBean;

/**
 * @author Endi Sukma Dewata
 */
public class SourceService extends StandardMBean implements SourceServiceMBean {

    private PenroseJMXService jmxService;
    private Partition partition;
    private Source source;

    public SourceService() throws Exception {
        super(SourceServiceMBean.class);
    }

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
