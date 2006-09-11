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
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.mapping.*;
import org.ietf.ldap.*;
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
    public Entry find(String dn) throws Exception {

        List path = findPath(dn);
        if (path == null) return null;
        if (path.size() == 0) return null;

        //Map map = (Map)path.get(0);
        //return (Entry)map.get("entry");
        return (Entry)path.get(0);
    }

    /**
     * @return path (List of Entries).
     */
    public List findPath(String dn) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND", 80));
            log.debug(Formatter.displayLine("Entry: "+dn, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        List path = findPathRecursive(dn);

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND RESULT", 80));

            log.debug(Formatter.displayLine("Path:", 80));
            if (path != null) {
                for (Iterator i=path.iterator(); i.hasNext(); ) {
                    Entry entry = (Entry)i.next();
                    log.debug(Formatter.displayLine(" - "+entry.getDn(), 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        return path;
    }

    public List findPathRecursive(String dn) throws Exception {

        if (dn == null) return null;
        //dn = dn.toLowerCase();

        PartitionManager partitionManager = handler.getPartitionManager();
        Partition partition = null;

        String firstDn = null;
        String secondDn = dn;

        while (secondDn != null) {

            //log.debug("Searching partition for \""+secondDn+"\"");
            partition = partitionManager.getPartitionByDn(secondDn);

            if (partition != null) break;

            int index = secondDn.indexOf(",");
            firstDn = EntryUtil.append(firstDn, index < 0 ? secondDn : secondDn.substring(0, index));
            secondDn = index < 0 ? null : secondDn.substring(index+1);
        }

        if (partition == null) {
            log.debug("Can't find partition for \""+dn+"\"");
            return null;
        }

        log.debug("Found partition "+partition.getName());

        Engine engine = handler.getEngine();
        List path = new ArrayList();

        while (true) {

            Row rdn = EntryUtil.getRdn(secondDn);
            Filter filter = FilterTool.createFilter(rdn);
/*
            for (Iterator iterator=rdn.getNames().iterator(); iterator.hasNext(); ) {
                String name = (String)iterator.next();
                String value = (String)rdn.get(name);

                SimpleFilter sf = new SimpleFilter(name, "=", value);
                filter = FilterTool.appendAndFilter(filter, sf);
            }
*/
            log.debug("Searching for \""+secondDn+"\" with filter "+filter);

            Collection entryMappings = partition.findEntryMappings(secondDn);

            if (entryMappings != null) {
                Entry entry = null;
    
                for (Iterator iterator = entryMappings.iterator(); iterator.hasNext(); ) {
                    EntryMapping entryMapping = (EntryMapping) iterator.next();

                    if (partition.isProxy(entryMapping)) {
                        PenroseSearchResults results = new PenroseSearchResults();

                        PenroseSearchControls sc = new PenroseSearchControls();
                        sc.setScope(PenroseSearchControls.SCOPE_BASE);

                        engine.searchProxy(
                                null,
                                partition,
                                entryMapping,
                                dn,
                                "(objectClass=*)",
                                sc,
                                results
                        );

                        if (results.hasNext()) {
                            entry = (Entry)results.next();
                        }

                    } else {
                        String entryDn = entryMapping.getDn();
                        log.debug("Searching entry in \""+entryDn+"\"");

                        Row entryRdn = EntryUtil.getRdn(entryDn);
                        if (!rdn.getNames().equals(entryRdn.getNames())) {
                            log.debug("RDN doesn't match");
                            continue;
                        }

                        AttributeValues parentSourceValues = new AttributeValues();
                        engine.getParentSourceValues(path, entryMapping, parentSourceValues);

                        PenroseSearchResults results = new PenroseSearchResults();
                        PenroseSearchControls sc = new PenroseSearchControls();

                        handler.getEngine().search(
                                path,
                                parentSourceValues,
                                entryMapping,
                                true,
                                filter,
                                sc,
                                results
                        );

                        while (results.hasNext()) {
                            Entry en = (Entry)results.next();

                            if (EntryUtil.match(dn, en.getDn())) {
                                log.debug("Found "+en.getDn()+".");
                                entry = en;
                                break;
                            }
                        }

                        if (entry != null) break;

                        log.debug("Can't find "+dn+" in "+entryMapping.getDn());
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

        AttributeValues parentSourceValues = new AttributeValues();

        PenroseSearchResults results = new PenroseSearchResults();
        PenroseSearchControls sc = new PenroseSearchControls();

        handler.getEngine().search(
                path,
                parentSourceValues,
                entryMapping,
                true,
                null,
                sc,
                results
        );

        if (results.size() == 0) return null;
        if (results.getReturnCode() != LDAPException.SUCCESS) return null;

        Entry entry = (Entry)results.next();
        return entry;
    }
}
