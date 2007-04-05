package org.safehaus.penrose.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    public final static Collection EMPTY = new ArrayList();

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    public Map sources = new LinkedHashMap();
    public Map sourceRefs = new LinkedHashMap();

    public void init(Partition partition, SourceConfig sourceConfig) throws Exception {

        Source source = getSource(partition, sourceConfig.getName());
        if (source != null) return;

        log.debug("Initializing source "+sourceConfig.getName()+".");

        source = new Source(partition, sourceConfig);

        ConnectionManager connectionManager = penroseContext.getConnectionManager();
        Connection connection = connectionManager.getConnection(partition, sourceConfig.getConnectionName());
        source.setConnection(connection);

        addSource(partition.getName(), source);
    }

    public void init(Partition partition, EntryMapping entryMapping, SourceMapping sourceMapping) throws Exception {

        SourceRef sourceRef = getSourceRef(partition.getName(), entryMapping, sourceMapping.getName());
        if (sourceRef != null) return;

        log.debug("Initializing source mapping "+sourceMapping.getName()+".");

        Source source = getSource(partition, sourceMapping.getSourceName());
        sourceRef = new SourceRef(source, sourceMapping);

        addSourceRef(partition.getName(), entryMapping, sourceRef);
    }

    public void addSourceRef(String partitionName, EntryMapping entryMapping, SourceRef sourceRef) {
        Map entryMappings = (Map)sourceRefs.get(partitionName);
        if (entryMappings == null) {
            entryMappings = new LinkedHashMap();
            sourceRefs.put(partitionName, entryMappings);
        }

        Map map = (Map)entryMappings.get(entryMapping.getId());
        if (map == null) {
            map = new LinkedHashMap();
            entryMappings.put(entryMapping.getId(), map);
        }

        map.put(sourceRef.getAlias(), sourceRef);
    }

    public Collection getSourceRefNames(Partition partition, EntryMapping entryMapping) {
        Map entryMappings = (Map)sourceRefs.get(partition.getName());
        if (entryMappings == null) return EMPTY;

        Map map = (Map)entryMappings.get(entryMapping.getId());
        if (map == null) return EMPTY;

        return new ArrayList(map.keySet()); // return Serializable list
    }

    public Collection getSourceRefs(Partition partition, EntryMapping entryMapping) {
        Map entryMappings = (Map)sourceRefs.get(partition.getName());
        if (entryMappings == null) return EMPTY;

        Map map = (Map)entryMappings.get(entryMapping.getId());
        if (map == null) return EMPTY;

        return map.values();
    }

    public SourceRef getSourceRef(String partitionName, EntryMapping entryMapping, String sourceName) {
        Map entryMappings = (Map)sourceRefs.get(partitionName);
        if (entryMappings == null) return null;

        Map map = (Map)entryMappings.get(entryMapping.getId());
        if (map == null) return null;

        return (SourceRef)map.get(sourceName);
    }

    public void addSource(String partitionName, Source source) {
        Map map = (Map)sources.get(partitionName);
        if (map == null) {
            map = new LinkedHashMap();
            sources.put(partitionName, map);
        }
        map.put(source.getName(), source);
    }

    public Collection getSourceNames(String partitionName) {
        Map map = (Map)sources.get(partitionName);
        if (map == null) return EMPTY;
        return new ArrayList(map.keySet()); // return Serializable list
    }

    public Collection getSources(Partition partition) {
        Map map = (Map)sources.get(partition.getName());
        if (map == null) return EMPTY;
        return map.values();
    }

    public Source getSource(Partition partition, String sourceName) {
        return getSource(partition.getName(), sourceName);
    }

    public Source getSource(String partitionName, String sourceName) {
        Map map = (Map)sources.get(partitionName);
        if (map == null) return null;
        return (Source)map.get(sourceName);
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }
}
