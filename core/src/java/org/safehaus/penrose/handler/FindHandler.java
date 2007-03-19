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

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.filter.Filter;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class FindHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    public Handler handler;

    public FindHandler(Handler handler) {
        this.handler = handler;
    }

    /**
	 * Find an entry given a dn.
	 *
	 * @param dn
	 * @return path from the entry to the root entry
	 */
    public Entry find(Session session, Partition partition, EntryMapping entryMapping, DN dn) throws Exception {

        Engine engine = handler.getEngine(partition, entryMapping);

        AttributeValues sourceValues = new AttributeValues();

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setFilter((Filter)null);

        SearchResponse response = new SearchResponse();

        engine.search(
                session,
                partition,
                sourceValues,
                entryMapping,
                request,
                response
        );

        if (!response.hasNext()) return null;
        return (Entry)response.next();
/*
        List path = new ArrayList();

        find(session, partition, entryMapping, dn, path, sourceValues);
        if (path.size() == 0) return null;

        return (Entry)path.iterator().next();
*/
    }

    public void find(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            List path,
            AttributeValues sourceValues
    ) throws Exception {

        if (partition == null) return;
        if (dn == null) return;

        log.debug("DN: "+dn);
        log.debug("Mapping: "+entryMapping.getDn());
        
        for (int i=0; i<dn.getSize() && entryMapping != null; i++) {
            RDN rdn = (RDN)dn.get(i);
            log.debug("RDN: "+rdn);

            Collection sourceMappings = entryMapping.getSourceMappings();
            for (Iterator j=sourceMappings.iterator(); j.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)j.next();

                Collection fieldMappings = sourceMapping.getFieldMappings();
                for (Iterator k=fieldMappings.iterator(); k.hasNext(); ) {
                    FieldMapping fieldMapping = (FieldMapping)k.next();
                    if (fieldMapping.getVariable() == null) continue;

                    String variable = fieldMapping.getVariable();
                    Object value = rdn.get(variable);
                    if (value == null) continue;

                    sourceValues.set(sourceMapping.getName()+"."+fieldMapping.getName(), value);
                }
            }

            Engine engine = handler.getEngine(partition, entryMapping);

            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter((Filter)null);

            SearchResponse response = new SearchResponse();

            engine.search(
                    session,
                    partition,
                    sourceValues,
                    entryMapping,
                    request,
                    response
            );

            while (response.hasNext()) {
                Entry entry = (Entry)response.next();
                path.add(entry);
            }

            entryMapping = partition.getParent(entryMapping);
        }

        log.debug("Source values: "+sourceValues);
/*
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

            Collection entryMappings = partition.findEntryMappings(suffix);

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
*/
        log.debug("Entries:");
        for (Iterator i=path.iterator(); i.hasNext(); ) {
            Entry entry = (Entry)i.next();
            log.debug(" - "+(entry == null ? null : entry.getDn()));
        }
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
