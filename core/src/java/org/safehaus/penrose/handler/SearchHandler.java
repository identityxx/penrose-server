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

import org.safehaus.penrose.session.*;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDN;
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

    public String normalize(String dn) {
        String newDn = "";

        SchemaManager schemaManager = handler.getSchemaManager();
        while (dn != null) {
            RDN rdn = EntryUtil.getRdn(dn);
            String parentDn = EntryUtil.getParentDn(dn);

            RDN newRdn = new RDN();
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

    public int search(
            final PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping entryMapping,
            final String baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws Exception {

        log.debug("Searching "+baseDn);
        log.debug("in mapping "+entryMapping.getDn());

        final PenroseSearchResults sr = new PenroseSearchResults();

        sr.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    Entry child = (Entry)event.getObject();
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

        //if (sc.getScope() == LDAPConnection.SCOPE_BASE || sc.getScope() == LDAPConnection.SCOPE_SUB) { // base or subtree
            searchBase(
                    session,
                    partition,
                    sourceValues,
                    entryMapping,
                    baseDn,
                    filter,
                    sc,
                    sr
            );
        //}

        if (sc.getScope() == LDAPConnection.SCOPE_ONE || sc.getScope() == LDAPConnection.SCOPE_SUB) { // one level or subtree
            log.debug("Searching children of \""+entryMapping.getDn()+"\"");

            Collection children = partition.getChildren(entryMapping);

            for (Iterator i = children.iterator(); i.hasNext();) {
                EntryMapping childMapping = (EntryMapping) i.next();

                searchChildren(
                        session,
                        partition,
                        sourceValues,
                        childMapping,
                        baseDn,
                        filter,
                        sc,
                        sr
                );
            }
        }

        sr.close();

        return LDAPException.SUCCESS;
	}

    public void searchBase(
            final PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping entryMapping,
            final String baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws Exception {

        log.info("Search base mapping \""+entryMapping.getDn()+"\":");

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
                //log.debug("Passing referral: "+referral);
                results.addReferral(referral);
            }
        });

        boolean cacheFilter = cache.search(partition, entryMapping, baseDn, filter, sr);
        log.debug("Cache filter: "+cacheFilter);

        if (!cacheFilter) {

            sr.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    Entry entry = (Entry)event.getObject();

                    try {
                        log.debug("Storing "+entry.getDn()+" in cache");
                        cache.put(partition, entryMapping, entry);
                        cache.add(partition, entryMapping, baseDn, filter, entry.getDn());

                    } catch (Exception e) {
                        log.debug(e.getMessage(), e);
                    }
                }
            });

            String engineName = "DEFAULT";
            if (partition.isProxy(entryMapping)) engineName = "PROXY";

            Engine engine = handler.getEngine(engineName);

            if (engine == null) {
                log.debug("Engine "+engineName+" not found");

            } else {
                engine.search(
                        session,
                        partition,
                        sourceValues,
                        entryMapping,
                        baseDn,
                        filter,
                        sc,
                        sr
                );
            }
        }

        sr.close();
    }

    public void searchChildren(
            final PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping entryMapping,
            final String baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws Exception {

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
                //log.debug("Passing referral: "+referral);
                results.addReferral(referral);
            }
        });

        boolean cacheFilter = cache.search(partition, entryMapping, baseDn, filter, sr);
        log.debug("Cache filter: "+cacheFilter);

        if (!cacheFilter) {

            sr.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    Entry entry = (Entry)event.getObject();

                    try {
                        log.debug("Storing "+entry.getDn()+" in cache");
                        cache.put(partition, entryMapping, entry);
                        cache.add(partition, entryMapping, baseDn, filter, entry.getDn());

                    } catch (Exception e) {
                        log.debug(e.getMessage(), e);
                    }
                }
            });

            String engineName = "DEFAULT";
            if (partition.isProxy(entryMapping)) engineName = "PROXY";

            Engine engine = handler.getEngine(engineName);

            if (engine == null) {
                log.debug("Engine "+engineName+" not found");

            } else {
                engine.expand(
                        session,
                        partition,
                        sourceValues,
                        entryMapping,
                        baseDn,
                        filter,
                        sc,
                        sr
                );
            }
        }

        sr.close();

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
        AttributeValues sv = handler.getEngine().computeAttributeValues(entryMapping, interpreter);
        interpreter.clear();

        AttributeValues newSourceValues = new AttributeValues();
        newSourceValues.add("parent", sourceValues);
        newSourceValues.add("parent", sv);
        //AttributeValues newSourceValues = handler.pushSourceValues(sourceValues, sv);

        log.debug("New parent source values:");
        for (Iterator i = newSourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = newSourceValues.get(name);
            log.debug(" - "+name+": "+values);
        }

        Collection children = partition.getChildren(entryMapping);

        for (Iterator i = children.iterator(); i.hasNext();) {
            EntryMapping childMapping = (EntryMapping) i.next();

            searchChildren(
                    session,
                    partition,
                    newSourceValues,
                    childMapping,
                    baseDn,
                    filter,
                    sc,
                    results
            );
        }
    }
}
