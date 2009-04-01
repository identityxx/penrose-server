/**
 * Copyright 2009 Red Hat, Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.partition;

import org.safehaus.penrose.scheduler.SchedulerReader;
import org.safehaus.penrose.scheduler.SchedulerConfig;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * @author Endi S. Dewata
 */
public class PartitionReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    SchedulerReader  schedulerReader = new SchedulerReader();

    public PartitionReader() {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/partition/partition.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/partition/partition-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
    }

    public void read(File baseDir, PartitionConfig partitionConfig) throws Exception {

        File partitionXml = new File(baseDir, "partition.xml");
        if (partitionXml.exists()) {
            log.debug("Loading "+partitionXml+".");
            digester.push(partitionConfig);
            digester.parse(partitionXml);
            digester.pop();
        }

        readSchedulerConfig(baseDir, partitionConfig);
    }

    public void readSchedulerConfig(File dir, PartitionConfig partitionConfig) throws Exception {
        File schedulerFile = new File(dir, "scheduler.xml");
        if (!schedulerFile.exists()) return;

        log.debug("Loading "+schedulerFile+".");
        SchedulerConfig schedulerConfig = new SchedulerConfig();
        schedulerReader.read(schedulerFile, schedulerConfig);
        
        partitionConfig.setSchedulerConfig(schedulerConfig);
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        return new InputSource(dtdUrl.openStream());
    }
}
