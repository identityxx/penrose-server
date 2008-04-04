package org.safehaus.penrose.partition;

import org.safehaus.penrose.connection.ConnectionReader;
import org.safehaus.penrose.connection.ConnectionWriter;
import org.safehaus.penrose.source.SourceReader;
import org.safehaus.penrose.source.SourceWriter;
import org.safehaus.penrose.mapping.MappingReader;
import org.safehaus.penrose.mapping.MappingWriter;
import org.safehaus.penrose.module.ModuleReader;
import org.safehaus.penrose.module.ModuleWriter;

import java.io.File;

/**
 * @author Endi Sukma Dewata
 */
public class DefaultPartitionConfig extends PartitionConfig {

    public DefaultPartitionConfig() {
        super("DEFAULT");
        partitionClass = DefaultPartition.class.getName();
    }

    public void load(File partitionDir) throws Exception {

        File conf = new File(partitionDir, "conf");

        File connectionsXml = new File(conf, "connections.xml");
        ConnectionReader connectionReader = new ConnectionReader();
        connectionReader.read(connectionsXml, connectionConfigManager);

        File sourcesXml = new File(conf, "sources.xml");
        SourceReader sourceReader = new SourceReader();
        sourceReader.read(sourcesXml, sourceConfigManager);

        File mappingXml = new File(conf, "mapping.xml");
        MappingReader mappingReader = new MappingReader();
        mappingReader.read(mappingXml, directoryConfig);

        File modulesXml = new File(conf, "modules.xml");
        ModuleReader moduleReader = new ModuleReader();
        moduleReader.read(modulesXml, moduleConfigManager);
    }

    public void store(File partitionDir) throws Exception {

        File conf = new File(partitionDir, "conf");

        File connectionsXml = new File(conf, "connections.xml");
        ConnectionWriter connectionWriter = new ConnectionWriter();
        connectionWriter.write(connectionsXml, connectionConfigManager);

        File sourcesXml = new File(conf, "sources.xml");
        SourceWriter sourceWriter = new SourceWriter();
        sourceWriter.write(sourcesXml, sourceConfigManager);

        File mappingXml = new File(conf, "mapping.xml");
        MappingWriter mappingWriter = new MappingWriter();
        mappingWriter.write(mappingXml, directoryConfig);

        File modulesXml = new File(conf, "modules.xml");
        ModuleWriter moduleWriter = new ModuleWriter();
        moduleWriter.write(modulesXml, moduleConfigManager);
    }
}
