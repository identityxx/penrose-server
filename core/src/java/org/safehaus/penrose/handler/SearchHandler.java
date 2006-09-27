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

import org.safehaus.penrose.session.*;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.*;
import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private Handler handler;

    public SearchHandler(Handler handler) {
        this.handler = handler;
    }

    /**
     *
     * @param session
     * @param entry
     * @param filter
     * @param sc
     * @param results This will be filled with objects of type Entry.
     * @return return code
     * @throws Exception
     */
    public int search(
            final PenroseSession session,
            final Partition partition,
            final AttributeValues parentSourceValues,
            final Entry entry,
            final Filter filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws Exception {

        handler.getEngine().getThreadManager().execute(new Runnable() {
            public void run() {

                int rc = LDAPException.SUCCESS;
                try {
                    rc = searchInBackground(
                            session,
                            partition,
                            parentSourceValues,
                            entry,
                            filter,
                            sc,
                            results
                    );

                } catch (LDAPException e) {
                    rc = e.getResultCode();

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    rc = ExceptionUtil.getReturnCode(e);

                } finally {
                    results.setReturnCode(rc);
                    results.close();

                    if (rc == LDAPException.SUCCESS) {
                        log.warn("Search operation succeded.");
                    } else {
                        log.warn("Search operation failed. RC="+rc);
                    }
                }
            }
        });

        return LDAPException.SUCCESS;
    }

    public String normalize(String dn) {
        String newDn = "";

        SchemaManager schemaManager = handler.getSchemaManager();
        while (dn != null) {
            Row rdn = EntryUtil.getRdn(dn);
            String parentDn = EntryUtil.getParentDn(dn);

            Row newRdn = new Row();
            for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Object value = rdn.get(name);

                newRdn.set(schemaManager.getNormalizedAttributeName(name), value);
            }

            //log.debug("Normalized rdn "+rdn+" => "+newRdn);

            newDn = EntryUtil.append(newDn, newRdn.toString());
            dn = parentDn;
        }

        return newDn;
    }

    public int searchInBackground(
            final PenroseSession session,
            final Partition partition,
            final AttributeValues parentSourceValues,
            final Entry entry,
            final Filter filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();

        final PenroseSearchResults sr = new PenroseSearchResults();
        final EntryCache cache = handler.getEntryCache();

        sr.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    Entry child = (Entry)event.getObject();

                    // check filter
                    if (!handler.getFilterTool().isValid(child, filter)) {
                        log.debug("Entry \""+child.getDn()+"\" doesn't match search filter.");
                        return;
                    }

                    // store in entry cache
                    EntryMapping entryMapping = child.getEntryMapping();
                    cache.add(partition, entryMapping, filter, child.getDn());
                    cache.put(partition, entryMapping, child);

                    results.add(child);

                } catch (Exception e) {
                    log.debug(e.getMessage(), e);
                }
            }

            public void pipelineClosed(PipelineEvent event) {
                results.setReturnCode(sr.getReturnCode());
            }
        });

        sr.addReferralListener(new ReferralAdapter() {
            public void referralAdded(ReferralEvent event) {
                Object referral = event.getReferral();
                //log.debug("Passing referral: "+referral);
                results.addReferral(referral);
            }
        });

        String engineName = "DEFAULT";
        if (partition.isProxy(entryMapping)) engineName = "PROXY";

        Engine engine = handler.getEngine(engineName);

        if (engine == null) {
            log.debug("Engine "+engineName+" not found");
            return LDAPException.OPERATIONS_ERROR;
        }

        engine.search(
                session,
                partition,
                parentSourceValues,
                entry.getEntryMapping(),
                entry,
                entry.getDn(),
                filter,
                sc,
                sr
        );

        if (sc.getScope() == LDAPConnection.SCOPE_ONE || sc.getScope() == LDAPConnection.SCOPE_SUB) { // one level or subtree
            log.debug("Searching children of \""+entry.getEntryMapping().getDn()+"\"");

            Collection children = partition.getChildren(entry.getEntryMapping());

            for (Iterator i = children.iterator(); i.hasNext();) {
                EntryMapping childMapping = (EntryMapping) i.next();

                searchChildren(
                        session,
                        partition,
                        entry,
                        parentSourceValues,
                        childMapping,
                        entry.getDn(),
                        filter,
                        sc,
                        sr
                );
            }
        }

        sr.close();

        return LDAPException.SUCCESS;
	}

    public void searchChildren(
            PenroseSession session,
            final Partition partition,
            Entry parent,
            AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            String baseDn,
            Filter filter,
            PenroseSearchControls sc,
            final PenroseSearchResults results) throws Exception {

        log.info("Search child mapping \""+entryMapping.getDn()+"\":");

        final PenroseSearchResults sr = new PenroseSearchResults();
        final EntryCache cache = handler.getEntryCache();

        sr.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                Entry entry = (Entry)event.getObject();
                results.add(entry);
            }
        });

        sr.addReferralListener(new ReferralAdapter() {
            public void referralAdded(ReferralEvent event) {
                Object referral = event.getReferral();
                log.debug("Passing referral: "+referral);
                results.addReferral(referral);
            }
        });

        boolean cacheFilter = cache.search(partition, entryMapping, baseDn, filter, sr);
        log.debug("Cache filter: "+cacheFilter);

        if (!cacheFilter) {

            String engineName = "DEFAULT";
            if (partition.isProxy(entryMapping)) engineName = "PROXY";

            Engine engine = handler.getEngine(engineName);

            if (engine == null) {
                log.debug("Engine "+engineName+" not found");

            } else {
                engine.expand(
                        session,
                        partition,
                        parent,
                        parentSourceValues,
                        entryMapping,
                        baseDn,
                        filter,
                        sc,
                        sr
                );
            }

            sr.close();
        }

        //log.debug("Waiting for search results from \""+entryMapping.getDn()+"\".");

        int rc = sr.getReturnCode();
        log.debug("RC: "+rc);

        if (rc != LDAPException.SUCCESS) {
            results.setReturnCode(rc);
            return;
        }

        if (sc.getScope() != LDAPConnection.SCOPE_SUB) return;

        log.debug("Searching children of " + entryMapping.getDn());

        Interpreter interpreter = handler.getEngine().getInterpreterManager().newInstance();
        AttributeValues sourceValues = handler.getEngine().computeAttributeValues(entryMapping, interpreter);
        interpreter.clear();

        AttributeValues newParentSourceValues = new AttributeValues();
        newParentSourceValues.add("parent", parentSourceValues);
        newParentSourceValues.add("parent", sourceValues);
        //AttributeValues newParentSourceValues = handler.pushSourceValues(parentSourceValues, sourceValues);

        for (Iterator i =newParentSourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = newParentSourceValues.get(name);
            log.debug(" - "+name+": "+values);
        }

        Collection children = partition.getChildren(entryMapping);

        for (Iterator i = children.iterator(); i.hasNext();) {
            EntryMapping childMapping = (EntryMapping) i.next();

            searchChildren(
                    session,
                    partition,
                    null,
                    newParentSourceValues,
                    childMapping,
                    baseDn,
                    filter,
                    sc,
                    results
            );
        }
    }
}
