package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceSync;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.SearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.StandardMBean;

/**
 * @author Endi Sukma Dewata
 */
public class SourceService extends StandardMBean implements SourceServiceMBean {

    Logger log = LoggerFactory.getLogger(getClass());

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

    public SourceConfig getSourceConfig() throws Exception {
        return source.getSourceConfig();
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

    public SearchResponse search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        source.search(request, response);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }
}
