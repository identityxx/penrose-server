package org.safehaus.penrose.partition;

import org.safehaus.penrose.connection.ConnectionReader;
import org.safehaus.penrose.connection.ConnectionWriter;
import org.safehaus.penrose.source.SourceReader;
import org.safehaus.penrose.source.SourceWriter;
import org.safehaus.penrose.directory.DirectoryReader;
import org.safehaus.penrose.directory.DirectoryWriter;
import org.safehaus.penrose.module.ModuleReader;
import org.safehaus.penrose.module.ModuleWriter;
import org.safehaus.penrose.mapping.MappingReader;
import org.safehaus.penrose.mapping.MappingWriter;

import java.io.File;

/**
 * @author Endi Sukma Dewata
 */
public class DefaultPartitionConfig extends PartitionConfig {

    public DefaultPartitionConfig() {
        name = PartitionConfig.ROOT;
    }

    public void load(File partitionDir) throws Exception {
/*
        File baseDir = new File(partitionDir, "conf");

        File connectionsXml = new File(baseDir, "connections.xml");
        ConnectionReader connectionReader = new ConnectionReader();
        connectionReader.read(connectionsXml, connectionConfigManager);

        File sourcesXml = new File(baseDir, "sources.xml");
        SourceReader sourceReader = new SourceReader();
        sourceReader.read(sourcesXml, sourceConfigManager);

        File mappingsXml = new File(baseDir, "mappings.xml");
        MappingReader mappingReader = new MappingReader();
        mappingReader.read(mappingsXml, mappingConfigManager);

        File directoryXml = new File(baseDir, "directory.xml");
        DirectoryReader directoryReader = new DirectoryReader();
        directoryReader.read(directoryXml, directoryConfig);

        File modulesXml = new File(baseDir, "modules.xml");
        ModuleReader moduleReader = new ModuleReader();
        moduleReader.read(modulesXml, moduleConfigManager);
*/
    }

    public void store(File partitionDir) throws Exception {

        File baseDir = new File(partitionDir, "conf");

        File connectionsXml = new File(baseDir, "connections.xml");
        ConnectionWriter connectionWriter = new ConnectionWriter();
        connectionWriter.write(connectionsXml, connectionConfigManager);

        File sourcesXml = new File(baseDir, "sources.xml");
        SourceWriter sourceWriter = new SourceWriter();
        sourceWriter.write(sourcesXml, sourceConfigManager);

        File mappingsXml = new File(baseDir, "mappings.xml");
        MappingWriter mappingWriter = new MappingWriter();
        mappingWriter.write(mappingsXml, mappingConfigManager);

        File directoryXml = new File(baseDir, "directory.xml");
        DirectoryWriter directoryWriter = new DirectoryWriter();
        directoryWriter.write(directoryXml, directoryConfig);

        File modulesXml = new File(baseDir, "modules.xml");
        ModuleWriter moduleWriter = new ModuleWriter();
        moduleWriter.write(modulesXml, moduleConfigManager);
    }
}
