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
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.config.PenroseConfig;
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
public class SearchHandler {

    Logger log = Logger.getLogger(getClass());

    private Handler handler;

    public SearchHandler(Handler handler) {
        this.handler = handler;
    }

    /**
     * @param session
     * @param base
     * @param scope
     * @param deref
     * @param filter
     * @param attributeNames
     * @param results
     * @return Entry
     * @throws Exception
     */
    public int search(
            PenroseSession session,
            String base,
            int scope,
            int deref,
            String filter,
            Collection attributeNames,
            PenroseSearchResults results) throws Exception {

        int rc;
        try {
            String s = null;
            switch (scope) {
            case LDAPConnection.SCOPE_BASE:
                s = "base";
                break;
            case LDAPConnection.SCOPE_ONE:
                s = "one level";
                break;
            case LDAPConnection.SCOPE_SUB:
                s = "subtree";
                break;
            }

            String d = null;
            switch (deref) {
            case LDAPSearchConstraints.DEREF_NEVER:
                d = "never";
                break;
            case LDAPSearchConstraints.DEREF_SEARCHING:
                d = "searching";
                break;
            case LDAPSearchConstraints.DEREF_FINDING:
                d = "finding";
                break;
            case LDAPSearchConstraints.DEREF_ALWAYS:
                d = "always";
                break;
            }

            log.debug("----------------------------------------------------------------------------------");
            log.debug("SEARCH:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: " + session.getBindDn());
            log.debug(" - Base DN: " + base);
            log.debug(" - Scope: " + s);
            log.debug(" - Filter: "+filter);
            log.debug(" - Alias Dereferencing: " + d);
            log.debug(" - Attribute Names: " + attributeNames);
            log.debug("");

            if (session != null && session.getBindDn() == null) {
                PenroseConfig penroseConfig = handler.getPenroseConfig();
                ServiceConfig serviceConfig = penroseConfig.getServiceConfig("LDAP");
                s = serviceConfig == null ? null : serviceConfig.getParameter("allowAnonymousAccess");
                boolean allowAnonymousAccess = s == null ? true : new Boolean(s).booleanValue();
                if (!allowAnonymousAccess) {
                    results.setReturnCode(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                    return LDAPException.INSUFFICIENT_ACCESS_RIGHTS;
                }
            }

            rc = performSearch(session, base, scope, deref, filter, attributeNames, results);

        } catch (LDAPException e) {
            rc = e.getResultCode();
            results.setReturnCode(rc);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = LDAPException.OPERATIONS_ERROR;
            results.setReturnCode(rc);

        } finally {
            results.close();
        }

        return rc;
    }

    /**
     * @param attributeNames
     * @param results of Entries
     */
    public int performSearch(
            PenroseSession session,
            String base,
            int scope,
            int deref,
            String filter,
            Collection attributeNames,
            PenroseSearchResults results) throws Exception {

        String nbase;
        try {
            nbase = LDAPDN.normalize(base);
            if (nbase == null) nbase = "";
        } catch (IllegalArgumentException e) {
            results.setReturnCode(LDAPException.INVALID_DN_SYNTAX);
            return LDAPException.INVALID_DN_SYNTAX;
        }

		List path = handler.getFindHandler().findPath(session, nbase);

		if (path == null) {
            log.debug("Entry \""+nbase+"\" not found.");

            if ("".equals(base) && scope == LDAPConnection.SCOPE_BASE) { // finding root DSE
                log.debug("Creating default Root DSE");

                Entry entry = new Entry("", null);
                AttributeValues attributeValues = entry.getAttributeValues();
                attributeValues.set("objectClass", "top");
                attributeValues.add("objectClass", "extensibleObject");
                attributeValues.set("vendorName", Penrose.VENDOR_NAME);
                attributeValues.set("vendorVersion", Penrose.PRODUCT_NAME+" "+Penrose.PRODUCT_VERSION);

                for (Iterator i=handler.getPartitionManager().getPartitions().iterator(); i.hasNext(); ) {
                    Partition partition = (Partition)i.next();
                    for (Iterator j=partition.getRootEntryMappings().iterator(); j.hasNext(); ) {
                        EntryMapping entryMapping = (EntryMapping)j.next();
                        if ("".equals(entryMapping.getDn())) continue;
                        attributeValues.add("namingContexts", entryMapping.getDn());
                    }
                }

                results.add(entry);
                results.close();

                return LDAPException.SUCCESS;
            }

			log.debug("Can't find base entry " + nbase);
			results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
			return LDAPException.NO_SUCH_OBJECT;
		}

        //Map map = (Map)path.iterator().next();
        //Entry baseEntry = (Entry)map.get("entry");
        Entry baseEntry = (Entry)path.iterator().next();

        log.debug("Found base entry: " + baseEntry.getDn());
        EntryMapping entryMapping = baseEntry.getEntryMapping();

        PartitionManager partitionManager = handler.getPartitionManager();
        Partition partition = partitionManager.getPartition(entryMapping);

        Engine engine = handler.getEngine();
        AttributeValues parentSourceValues = new AttributeValues();
        engine.getParentSourceValues(path, entryMapping, parentSourceValues);

        int rc = handler.getACLEngine().checkSearch(session, baseEntry.getDn(), entryMapping);
        if (rc != LDAPException.SUCCESS) {
            log.debug("Checking search permission => FAILED");
            return rc;
        }

        Filter f = FilterTool.parseFilter(filter);
        log.debug("Parsed filter: "+f+" ("+f.getClass().getName()+")");

        if (partition.isProxy(entryMapping)) {

            rc = handler.getACLEngine().checkRead(session, baseEntry.getDn(), entryMapping);
            if (rc != LDAPException.SUCCESS) {
                log.debug("Checking read permission => FAILED");
                return rc;
            }

            if (scope == LDAPConnection.SCOPE_BASE) {
                if (handler.getFilterTool().isValid(baseEntry, f)) {
                    results.add(baseEntry);
                }

            } else {
                handler.getEngine().searchProxy(partition, entryMapping, base, scope, filter, attributeNames, results);
            }

        } else { // not a proxy

            if (scope == LDAPConnection.SCOPE_BASE || scope == LDAPConnection.SCOPE_SUB) { // base or subtree
                if (handler.getFilterTool().isValid(baseEntry, f)) {

                    rc = handler.getACLEngine().checkRead(session, baseEntry.getDn(), entryMapping);
                    if (rc == LDAPException.SUCCESS) {

                        Entry e = baseEntry;

                        if (attributeNames != null && !attributeNames.isEmpty()) {
                            AttributeValues av = new AttributeValues();
                            av.add(baseEntry.getAttributeValues());
                            av.retain(attributeNames);

                            e = new Entry(baseEntry.getDn(), entryMapping, baseEntry.getSourceValues(), av);
                        }

                        results.add(e);
                    }
                }
            }

            if (scope == LDAPConnection.SCOPE_ONE || scope == LDAPConnection.SCOPE_SUB) { // one level or subtree
                log.debug("Searching children of \""+entryMapping.getDn()+"\"");
                searchChildren(session, path, entryMapping, parentSourceValues, scope, f, attributeNames, results, true);
            }
        }

		//results.setReturnCode(LDAPException.SUCCESS);
		return LDAPException.SUCCESS;
	}

    public void searchChildren(
            PenroseSession session,
            Collection path,
            EntryMapping entryMapping,
            AttributeValues parentSourceValues,
            int scope,
            Filter filter,
            Collection attributeNames,
            PenroseSearchResults results,
            boolean first) throws Exception {

        PartitionManager partitionManager = handler.getPartitionManager();
        Partition partition = partitionManager.getPartition(entryMapping);
        Collection children = partition.getChildren(entryMapping);

        for (Iterator i = children.iterator(); i.hasNext();) {
            EntryMapping childMapping = (EntryMapping) i.next();
            log.debug("Searching \""+childMapping.getDn()+"\"");

            if (partition.isProxy(childMapping)) {
                if (scope == LDAPConnection.SCOPE_ONE) {
                    scope = LDAPConnection.SCOPE_BASE;
                }
                handler.getEngine().searchProxy(partition, childMapping, childMapping.getDn(), scope, filter.toString(), attributeNames, results);
                continue;
            }

            if (handler.getFilterTool().isValid(childMapping, filter)) {

                PenroseSearchResults sr = search(
                        path,
                        parentSourceValues,
                        childMapping,
                        false,
                        filter,
                        attributeNames
                );

                while (sr.hasNext()) {
                    Entry child = (Entry)sr.next();

                    int rc = handler.getACLEngine().checkSearch(session, child.getDn(), child.getEntryMapping());
                    if (rc != LDAPException.SUCCESS) continue;

                    if (!handler.getFilterTool().isValid(child, filter)) continue;

                    rc = handler.getACLEngine().checkRead(session, child.getDn(), child.getEntryMapping());
                    if (rc != LDAPException.SUCCESS) continue;

                    Entry e = child;

                    if (attributeNames != null && !attributeNames.isEmpty()) {
                        AttributeValues av = new AttributeValues();
                        av.add(child.getAttributeValues());
                        av.retain(attributeNames);

                        e = new Entry(child.getDn(), entryMapping, child.getSourceValues(), av);
                    }

                    results.add(e);
                }

                int rc = sr.getReturnCode();

                if (rc != LDAPException.SUCCESS) {
                    log.debug("RC: "+rc);
                    results.setReturnCode(rc);
                    continue;
                }
            }

            if (scope == LDAPConnection.SCOPE_SUB) {
                log.debug("Searching children of " + childMapping.getDn());

                AttributeValues newParentSourceValues = new AttributeValues();
                for (Iterator j=parentSourceValues.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = parentSourceValues.get(name);

                    if (name.startsWith("parent.")) name = "parent."+name;

                    newParentSourceValues.add(name, values);
                }

                Engine engine = handler.getEngine();
                Interpreter interpreter = engine.getInterpreterFactory().newInstance();

                AttributeValues av = engine.computeAttributeValues(childMapping, interpreter);
                for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = av.get(name);

                    name = "parent."+name;
                    newParentSourceValues.add(name, values);
                }

                interpreter.clear();

                for (Iterator j=newParentSourceValues.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = newParentSourceValues.get(name);
                    log.debug(" - "+name+": "+values);
                }

                //Map map = new HashMap();
                //map.put("dn", childMapping.getDn());
                //map.put("entry", null);
                //map.put("entryDefinition", childMapping);

                Collection newPath = new ArrayList();
                newPath.add(null);
                newPath.addAll(path);

                searchChildren(session, newPath, childMapping, newParentSourceValues, scope, filter, attributeNames, results, false);
            }
        }
    }

    public PenroseSearchResults search(
            final Collection path,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            boolean single,
            final Filter filter,
            Collection attributeNames) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Entry: \""+entryMapping.getDn()+"\"", 80));
            log.debug(Formatter.displayLine("Filter: "+filter, 80));
            log.debug(Formatter.displayLine("Parents:", 80));

            if (parentSourceValues != null) {
                for (Iterator i = parentSourceValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = parentSourceValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        final PenroseSearchResults results = new PenroseSearchResults();
        final PenroseSearchResults dns = new PenroseSearchResults();
        final PenroseSearchResults entriesToLoad = new PenroseSearchResults();
        final PenroseSearchResults loadedEntries = new PenroseSearchResults();
        final PenroseSearchResults newEntries = new PenroseSearchResults();

        Collection attributeDefinitions = entryMapping.getAttributeMappings(attributeNames);

        // check if client only requests the dn to be returned
        final boolean dnOnly = attributeNames != null && attributeNames.contains("dn")
                && attributeDefinitions.isEmpty()
                && "(objectclass=*)".equals(filter.toString().toLowerCase());

        log.debug("Search DNs only: "+dnOnly);

        dns.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    EntryData map = (EntryData)event.getObject();
                    String dn = map.getDn();

                    Entry entry = (Entry)handler.getEngine().getEntryCache().get(dn);
                    log.debug("Entry cache for "+dn+": "+(entry == null ? "not found." : "found."));

                    if (entry == null) {

                        if (dnOnly) {
                            AttributeValues sv = map.getMergedValues();
                            //AttributeValues attributeValues = handler.getEngine().computeAttributeValues(entryMapping, sv, interpreter);
                            entry = new Entry(dn, entryMapping, sv, null);

                            results.add(entry);

                        } else {
                            entriesToLoad.add(map);
                        }

                    } else {
                        results.add(entry);
                    }

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

            public void pipelineClosed(PipelineEvent event) {
                int rc = dns.getReturnCode();
                //log.debug("RC: "+rc);

                if (dnOnly) {
                    results.setReturnCode(rc);
                    results.close();
                } else {
                    entriesToLoad.setReturnCode(rc);
                    entriesToLoad.close();
                }
            }
        });

        Entry parent = null;
        if (path != null && path.size() > 0) {
            parent = (Entry)path.iterator().next();
        }

        log.debug("Parent: "+(parent == null ? null : parent.getDn()));
        String parentDn = parent == null ? null : parent.getDn();

        boolean cacheFilter = handler.getEngine().getEntryCache().contains(entryMapping, parentDn, filter);

        if (!cacheFilter) {

            log.debug("Filter cache for "+filter+" not found.");

            dns.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        EntryData map = (EntryData)event.getObject();
                        String dn = map.getDn();

                        log.info("Storing "+dn+" in filter cache.");

                        handler.getEngine().getEntryCache().add(entryMapping, filter, dn);

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            });

            handler.getEngine().search(parent, parentSourceValues, entryMapping, filter, dns);

        } else {
            log.debug("Filter cache for "+filter+" found.");

            PenroseSearchResults list = new PenroseSearchResults();

            list.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        String dn = (String)event.getObject();
                        log.info("Loading "+dn+" from filter cache.");

                        EntryData map = new EntryData();
                        map.setDn(dn);
                        map.setMergedValues(new AttributeValues());
                        map.setRows(new ArrayList());
                        dns.add(map);

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }

                public void pipelineClosed(PipelineEvent event) {
                    dns.close();
                }
            });

            handler.getEngine().getEntryCache().search(entryMapping, parentDn, filter, list);
        }

        if (dnOnly) return results;

        handler.getEngine().load(entryMapping, entriesToLoad, loadedEntries);

        newEntries.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    Entry entry = (Entry)event.getObject();

                    log.info("Storing "+entry.getDn()+" in entry cache.");

                    handler.getEngine().getEntryCache().put(entry);
                    results.add(entry);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

            public void pipelineClosed(PipelineEvent event) {
                int rc = newEntries.getReturnCode();
                //log.debug("RC: "+rc);

                results.setReturnCode(rc);
                results.close();
            }
        });

        handler.getEngine().merge(entryMapping, loadedEntries, newEntries);

        return results;
    }

}
