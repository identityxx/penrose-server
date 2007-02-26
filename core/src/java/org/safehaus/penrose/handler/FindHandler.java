/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDN;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

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
    public Entry find(Partition partition, String dn) throws Exception {

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

        List rdns = EntryUtil.parseDn(dn);
        log.debug("Length ["+rdns.size()+"]");

        int p = 0;
        int position = 0;

        while (position < rdns.size()) {

            for (int i = p; i < position; i++) {
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

            String prefix = null;
            for (int i = 0; i < rdns.size()-1-position; i++) {
                prefix = EntryUtil.append(prefix, (RDN)rdns.get(i));
            }

            String suffix = null;
            for (int i = rdns.size()-1-position; i < rdns.size(); i++) {
                suffix = EntryUtil.append(suffix, (RDN)rdns.get(i));
            }

            log.debug("Position ["+position +"]: ["+(prefix == null ? "" : prefix)+"] ["+suffix+"]");

            PartitionManager partitionManager = handler.getPartitionManager();
            Collection entryMappings = partitionManager.findEntryMappings(partition, suffix);

            if (entryMappings == null) {
                path.add(0, null);
                position++;
                continue;
            }

            //AttributeValues parentSourceValues = new AttributeValues();
            //parentSourceValues.add(sourceValues);

            List list = null;

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

                list = engine.find(
                        partition,
                        sourceValues,
                        entryMapping,
                        rdns,
                        position
                );

                log.debug("Returned ["+list.size()+"]");

                if (list == null || list.size() == 0) continue;

                break;
            }

            if (list == null || list.size() == 0) {
                path.add(0, null);
                position++;
                continue;
            }

            int c=0;
            for (Iterator i=list.iterator(); i.hasNext(); c++) {
                Entry entry = (Entry)i.next();

                //if (entry != null) {
                //    sourceValues.add(entry.getSourceValues());
                //}

                path.add(c, entry);
                position++;
            }

        }

        log.debug("Entries:");
        for (Iterator i=path.iterator(); i.hasNext(); ) {
            Entry entry = (Entry)i.next();
            log.debug(" - "+(entry == null ? null : entry.getDn()));
        }
    }
}
