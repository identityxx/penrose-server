/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

        for (Iterator i = c.iterator(); i.hasNext();) {
            Object o = i.next();
            if (o instanceof AttributeType) {
                AttributeType at = (AttributeType) o;
                schema.addAttributeType(at);

            } else if (o instanceof ObjectClass) {
                ObjectClass oc = (ObjectClass) o;
                schema.addObjectClass(oc);

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
