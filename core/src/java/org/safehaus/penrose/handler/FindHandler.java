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
package org.safehaus.penrose.handler;

import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.engine.Engine;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.ietf.ldap.LDAPDN;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class FindHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private Handler handler;

    public FindHandler(Handler handler) {
        this.handler = handler;
    }

    /**
	 * Find an entry given a dn.
	 *
	 * @param dn
	 * @return path from the entry to the root entry
	 */
    public Entry find(String dn) throws Exception {

        Partition partition = handler.getPartitionManager().findPartition(dn);
        if (partition == null) return null;

        List path = new ArrayList();
        AttributeValues sourceValues = new AttributeValues();

        find(partition, dn, path, sourceValues);
        if (path.size() == 0) return null;

        return (Entry)path.iterator().next();
    }

    public void find(
            Partition partition,
            String dn,
            List path,
            AttributeValues sourceValues
    ) throws Exception {

        if (partition == null) return;
        if (dn == null) return;

        Entry parent = null;

        String rdns[] = LDAPDN.explodeDN(dn, false);

        int position = 0;
        log.debug("Length ["+rdns.length+"]");
        while (position < rdns.length) {

            String prefix = null;
            for (int i = 0; i < rdns.length-1-position; i++) prefix = EntryUtil.append(prefix, rdns[i]);

            String suffix = null;
            for (int i = rdns.length-1-position; i < rdns.length; i++) suffix = EntryUtil.append(suffix, rdns[i]);

            log.debug("Position ["+position+"]: ["+prefix+"] ["+suffix+"]");

            Collection entryMappings = partition.findEntryMappings(suffix);

            int count = 0;

            //AttributeValues attributeValues = new AttributeValues();

            if (entryMappings == null) {
                path.add(0, null);
                count = 1;

            } else {

                AttributeValues parentSourceValues = new AttributeValues();
                parentSourceValues.add(sourceValues);

                for (Iterator iterator = entryMappings.iterator(); iterator.hasNext(); ) {
                    EntryMapping entryMapping = (EntryMapping)iterator.next();

                    log.debug("Check mapping ["+entryMapping.getDn()+"]");

                    String engineName = "DEFAULT";
                    if (partition.isProxy(entryMapping)) engineName = "PROXY";

                    Engine engine = handler.getEngine(engineName);

                    if (engine == null) {
                        log.debug("Engine "+engineName+" not found");
                        continue;
                    }

                    count = engine.find(
                            partition,
                            parent,
                            parentSourceValues,
                            entryMapping,
                            rdns,
                            position,
                            path
                    );

                    log.debug("Returned ["+count+"]");

                    if (count == 0) continue;

                    Entry entry = (Entry)path.get(0);

                    if (entry != null) {
                        //attributeValues.add(entry.getAttributeValues());
                        sourceValues.add(entry.getSourceValues());
                    }

                    break;
                }

                if (count == 0) {
                    path.add(0, null);
                    count = 1;
                }
            }

            for (int i = 0; i < count; i++) {
                AttributeValues newSourceValues = new AttributeValues();
                for (Iterator j=sourceValues.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection c = sourceValues.get(name);
                    if (name.startsWith("parent")) name = "parent."+name;
                    newSourceValues.add(name, c);
                }
                sourceValues.clear();
                sourceValues.add(newSourceValues);
            }

            position += count;
            log.debug("New position ["+position+"] = old position + "+count);

            log.debug("Source values:");
            for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Collection c = sourceValues.get(name);
                log.debug(" - "+name+": "+c);
            }
        }
    }
}
