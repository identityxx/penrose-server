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

import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.JNDIClient;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.event.SearchEvent;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.ConnectionConfig;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.mapping.*;
import org.apache.log4j.Logger;
import org.ietf.ldap.*;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class FindHandler {

    Logger log = Logger.getLogger(getClass());

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
    public Entry find(
            PenroseSession session,
            String dn) throws Exception {

        List path = findPath(session, dn);
        if (path == null) return null;
        if (path.size() == 0) return null;

        //Map map = (Map)path.get(0);
        //return (Entry)map.get("entry");
        return (Entry)path.get(0);
    }

    /**
     * @return path (List of Entries).
     */
/*
    public List findPath(
            PenroseSession session,
            String dn) throws Exception {

        if (dn == null) return null;

        dn = dn.toLowerCase();
        String parentDn = Entry.getParentDn(dn);
        Row rdn = Entry.getRdn(dn);

        log.debug("Find entry: ["+rdn+"] ["+parentDn+"]");

        List path = findPath(session, parentDn);
        Entry parent;

        if (path == null) {
            path = new ArrayList();
            parent = null;
        } else {
            parent = (Entry)path.iterator().next();
            //Map map = (Map)path.iterator().next();
            //parent = (Entry)map.get("entry");
        }

        log.debug("Found parent: "+(parent == null ? null : parent.getDn()));

        Partition partition = handler.getPartitionManager().getPartitionByDn(dn);
        if (partition == null) {
            //log.error("Missing config for "+dn);
            return null;
        }

        // search the entry directly
        Collection entryMappings = partition.findEntryMappings(dn);

        Filter filter = null;
        for (Iterator iterator=rdn.getNames().iterator(); iterator.hasNext(); ) {
            String name = (String)iterator.next();
            String value = (String)rdn.get(name);

            SimpleFilter sf = new SimpleFilter(name, "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);
        }

        log.debug("Searching children with filter "+filter);

		for (Iterator iterator = entryMappings.iterator(); iterator.hasNext(); ) {
			EntryMapping childMapping = (EntryMapping) iterator.next();

            String childDn = childMapping.getDn().toLowerCase();
            Row childRdn = Entry.getRdn(childDn);
            log.debug("Finding entry in "+childDn);

            if (!rdn.getNames().equals(childRdn.getNames())) continue;

            Engine engine = handler.getEngine();
            AttributeValues parentSourceValues = new AttributeValues();
            engine.getParentSourceValues(path, childMapping, parentSourceValues);

            PenroseSearchResults sr = search(
                    path,
                    parentSourceValues,
                    childMapping,
                    true,
                    filter,
                    new ArrayList()
            );

            while (sr.hasNext()) {
                Entry child = (Entry)sr.next();
                if (handler.getFilterTool().isValid(child, filter)) {

                    if (sr.getReturnCode() != LDAPException.SUCCESS) return null;

                    log.debug("Adding "+child.getDn()+" into path");
                    //Map map = new HashMap();
                    //map.put("dn", child.getDn());
                    //map.put("entry", child);
                    //map.put("entryDefinition", child.getEntryMapping());
                    //path.add(0, map);
                    path.add(0, child);
                    return path;
                }
            }
		}

		return null;
	}
*/
    
    public List findPath(
            PenroseSession session,
            String dn) throws Exception {

        if (dn == null) return null;
        dn = dn.toLowerCase();

        PartitionManager partitionManager = handler.getPartitionManager();
        Partition partition = null;

        String firstDn = null;
        String secondDn = dn;

        while (secondDn != null) {

            log.error("Searching partition for \""+secondDn+"\"");
            partition = partitionManager.getPartitionByDn(secondDn);

            if (partition != null) break;

            int index = secondDn.indexOf(",");
            firstDn = EntryUtil.append(firstDn, index < 0 ? secondDn : secondDn.substring(0, index));
            secondDn = index < 0 ? null : secondDn.substring(index+1);
        }

        if (partition == null) {
            log.error("Can't find partition for \""+dn+"\"");
            return null;
        }

        log.error("Found partition "+partition.getName());

        Engine engine = handler.getEngine();
        List path = new ArrayList();

        while (true) {

            Row rdn = EntryUtil.getRdn(secondDn);
            Filter filter = null;
            for (Iterator iterator=rdn.getNames().iterator(); iterator.hasNext(); ) {
                String name = (String)iterator.next();
                String value = (String)rdn.get(name);

                SimpleFilter sf = new SimpleFilter(name, "=", value);
                filter = FilterTool.appendAndFilter(filter, sf);
            }

            log.debug("Searching for \""+secondDn+"\" with filter "+filter);

            Collection entryMappings = partition.findEntryMappings(secondDn);

            if (entryMappings != null) {
                Entry entry = null;
    
                for (Iterator iterator = entryMappings.iterator(); iterator.hasNext(); ) {
                    EntryMapping entryMapping = (EntryMapping) iterator.next();

                    if (partition.isProxy(entryMapping)) {
                        PenroseSearchResults results = new PenroseSearchResults();

                        engine.searchProxy(
                                partition,
                                entryMapping,
                                dn,
                                LDAPConnection.SCOPE_BASE,
                                "(objectClass=*)",
                                null,
                                results
                        );

                        if (results.hasNext()) {
                            entry = (Entry)results.next();
                        }

                    } else {
                        String entryDn = entryMapping.getDn().toLowerCase();
                        log.debug("Searching entry in \""+entryDn+"\"");

                        Row entryRdn = EntryUtil.getRdn(entryDn);
                        if (!rdn.getNames().equals(entryRdn.getNames())) {
                            log.debug("RDN doesn't match");
                            continue;
                        }

                        AttributeValues parentSourceValues = new AttributeValues();
                        engine.getParentSourceValues(path, entryMapping, parentSourceValues);

                        PenroseSearchResults sr = handler.getSearchHandler().search(
                                path,
                                parentSourceValues,
                                entryMapping,
                                true,
                                filter,
                                new ArrayList()
                        );

                        if (!sr.hasNext()) {
                            log.debug("Search returned no results");
                            continue;
                        }

                        Entry en = (Entry)sr.next();
                        if (!handler.getFilterTool().isValid(en, filter)) {
                            log.debug("Entry \""+en.getDn()+"\" doesn't match "+filter);
                            continue;
                        }

                        if (sr.getReturnCode() != LDAPException.SUCCESS) {
                            log.debug("An error occured: "+sr.getReturnCode());
                            continue;
                        }

                        log.debug("Adding "+en.getDn()+" into path");
                        entry = en;
                        break;
                    }
                }

                if (entry == null) {
                    log.debug("Can't find "+secondDn);
                    return null;
                }

                path.add(0, entry);

                if (firstDn == null) {
                    log.debug("No more DN to search");
                    break;
                }

            }

            int index = secondDn.indexOf(",");
            firstDn = EntryUtil.append(firstDn, index < 0 ? secondDn : secondDn.substring(0, index));
            secondDn = index < 0 ? null : secondDn.substring(index+1);
        }

        log.debug("Path:");

        for (Iterator i=path.iterator(); i.hasNext(); ) {
            Entry entry = (Entry)i.next();
            log.debug(" - "+entry.getDn());
        }

		return path;
	}

    public Entry find(
            Collection path,
            EntryMapping entryMapping
            ) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND", 80));
            log.debug(Formatter.displayLine("Entry: "+entryMapping.getDn(), 80));
            log.debug(Formatter.displayLine("Parents:", 80));

            for (Iterator i=path.iterator(); i.hasNext(); ) {
                Entry entry = (Entry)i.next();
                String dn = entry.getDn();
                //Map map = (Map)i.next();
                //String dn = (String)map.get("dn");
                log.debug(Formatter.displayLine(" - "+dn, 80));
            }

            log.debug(Formatter.displaySeparator(80));
        }

        AttributeValues parentSourceValues = new AttributeValues();

        PenroseSearchResults results = handler.getSearchHandler().search(
                path,
                parentSourceValues,
                entryMapping,
                true,
                null,
                null
        );

        if (results.size() == 0) return null;
        if (results.getReturnCode() != LDAPException.SUCCESS) return null;

        Entry entry = (Entry)results.next();

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND RESULT", 80));
            log.debug(Formatter.displayLine("dn: "+entry.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        return entry;
    }
}
