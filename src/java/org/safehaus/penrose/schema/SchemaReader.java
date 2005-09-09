/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;

/**
 * @author Endi S. Dewata
 */
public class SchemaReader {

    private Logger log = LoggerFactory.getLogger(getClass());

    private Schema schema;

    public SchemaReader() {
        schema = new Schema();
    }

    public SchemaReader(Schema schema) {
        this.schema = schema;
    }

    public void readDirectory(String directory) throws Exception {
        File schemaDir = new File(directory);
        String filenames[] = schemaDir.list(new FilenameFilter() {
            public boolean accept(File dir, String fname) {
                return fname.endsWith(".schema");
            }
        });

        for (int i=0; i<filenames.length; i++) {
            read(directory+"/"+filenames[i]);
        }

    }

    public void read(String filename) throws Exception {

        log.debug("Loading schema from "+filename);

        FileReader in = new FileReader(filename);
        SchemaParser parser = new SchemaParser(in);
        Collection c = parser.parse();

        Map attributeTypes = schema.getAttributeTypes();
        Map objectClasses = schema.getObjectClasses();

        for (Iterator i = c.iterator(); i.hasNext();) {
            Object o = i.next();
            if (o instanceof AttributeType) {
                AttributeType at = (AttributeType) o;
                attributeTypes.put(at.getName(), at);

            } else if (o instanceof ObjectClass) {
                ObjectClass oc = (ObjectClass) o;
                //log.debug("Adding object class "+oc.getName());
                objectClasses.put(oc.getName(), oc);

            } else {
                //log.debug(" - ERROR");
            }
        }
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
