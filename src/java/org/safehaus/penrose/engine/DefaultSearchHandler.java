/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;


import java.util.*;

import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPEntry;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.OrFilter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.cache.SourceCacheConfig;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class DefaultSearchHandler implements SearchHandler {

    public Logger log = Logger.getLogger(Penrose.SEARCH_LOGGER);

    public DefaultEngine engine;
	public EngineContext engineContext;
    public Config config;

	public void init(Engine engine, EngineContext engineContext) throws Exception {
        this.engine = ((DefaultEngine)engine);
		this.engineContext = engineContext;
        config = engineContext.getConfig();
	}

    public int search(
            PenroseConnection connection,
            String base,
            int scope,
            int deref,
            String filter,
            Collection attributeNames,
            SearchResults results) throws Exception {

		String nbase;
		try {
			nbase = LDAPDN.normalize(base);
		} catch (IllegalArgumentException e) {
			results.setReturnCode(LDAPException.INVALID_DN_SYNTAX);
			return LDAPException.INVALID_DN_SYNTAX;
		}

		Filter f = engineContext.getFilterTool().parseFilter(filter);
		log.debug("Parsed filter: " + f);

		Entry baseEntry;
		try {
			baseEntry = getEntry(connection, nbase, attributeNames);
            
        } catch (LDAPException e) {
            log.debug(e.getMessage());
            results.setReturnCode(e.getResultCode());
            return e.getResultCode();

		} catch (Exception e) {
			log.debug(e.getMessage(), e);
			results.setReturnCode(LDAPException.OPERATIONS_ERROR);
			return LDAPException.OPERATIONS_ERROR;
		}

		if (baseEntry == null) {
			log.debug("Can't find " + nbase);
			results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
			return LDAPException.NO_SUCH_OBJECT;
		}

		log.debug("Search base: " + baseEntry.getDn());

		if (scope == 0 || scope == 2) { // base or subtree
			if (engineContext.getFilterTool().isValidEntry(baseEntry, f)) {
                LDAPEntry entry = baseEntry.toLDAPEntry();
				results.add(Entry.filterAttributes(entry, attributeNames));
			}
		}

		if (scope == 1 || scope == 2) { // one level or subtree
			log.debug("Searching children of " + baseEntry.getDn());
			searchChildren(connection, baseEntry, scope, f, attributeNames, results);
		}
		/*
		 * log.debug("-------------------------------------------------------------------------------");
		 * log.debug("CHECKING FILTER: "+filter);
		 * 
		 * for (Iterator i=entries.iterator(); i.hasNext(); ) { Entry r =
		 * (Entry)i.next();
		 * 
		 * if (penrose.filterTool.isValidEntry(r, f)) {
		 * results.put(penrose.entryTool.toLDAPEntry(connection, r,
		 * attributeNames)); log.debug(" - "+r.getDn()+" ok"); }
		 * else { log.debug(" - "+r.getDn()+" failed"); } }
		 * 
		 * log.debug("-------------------------------------------------------------------------------");
		 */
		results.setReturnCode(LDAPException.SUCCESS);
		return LDAPException.SUCCESS;
	}

	/**
	 * Find an real entry given a dn. If not found it will search for a virtual
	 * entry from all possible mappings.
	 * 
	 * @param dn
	 * @return virtual entry
	 * @throws Exception
	 */
	public Entry getEntry(
            PenroseConnection connection,
            String dn,
			Collection attributeNames) throws Exception {

		log.debug("----------------------------------------------------------------------------------");
		log.debug("Getting entry: " + dn);

        // search the entry directly
        EntryDefinition entry = config.getEntryDefinition(dn);

        if (entry != null) {
            log.debug("Found static entry: " + dn);

            AttributeValues values = entry.getAttributeValues(engineContext.newInterpreter());
            return new Entry(entry, values);
        }

		int i = dn.indexOf(",");
		String rdn;
		String parentDn;

		if (i < 0) {
			rdn = dn;
			parentDn = null;
		} else {
			rdn = dn.substring(0, i);
			parentDn = dn.substring(i + 1);
		}

        // search the parent entry
		EntryDefinition parentEntry = config.getEntryDefinition(parentDn);

		if (parentEntry == null) {
			log.debug("Parent not found: " + dn);

			throw new LDAPException("Can't find " + dn + ".",
					LDAPException.NO_SUCH_OBJECT, "Can't find virtual entry "
							+ dn + ".");
		}

		log.debug("Found parent entry: " + parentDn);
		Collection children = parentEntry.getChildren();

        Filter filter = engineContext.getFilterTool().parseFilter("(" + rdn + ")");
        log.debug("Parsed filter: " + filter);

        int j = rdn.indexOf("=");
        String rdnAttribute = rdn.substring(0, j);
        String rdnValue = rdn.substring(j + 1);

        // Find in each dynamic children
		for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
			EntryDefinition child = (EntryDefinition) iterator.next();

            String childRdn = child.getRdn();
            log.debug("Checking child: "+childRdn);

            int k = childRdn.indexOf("=");
            String childRdnAttribute = childRdn.substring(0, k);
            String childRdnValue = childRdn.substring(k+1);

            // the rdn attribute types must match
            if (!rdnAttribute.equals(childRdnAttribute)) continue;

			if (child.isDynamic()) {

                log.debug("Found entry definition: " + child.getDn());

                try {
                    Entry sr = searchVirtualEntry(connection, child, rdn);
                    log.debug("Found virtual entry: " + sr.getDn());

                    return sr;

                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    // not found, continue to next entry definition
                }

            } else {
                if (!rdnValue.toLowerCase().equals(childRdnValue.toLowerCase())) continue;

                log.debug("Found static entry: " + child.getDn());

                AttributeValues values = child.getAttributeValues(engineContext.newInterpreter());
                return new Entry(child, values);
            }
		}

		throw new LDAPException("Can't find " + dn + ".",
				LDAPException.NO_SUCH_OBJECT, "Can't find virtual entry " + dn + ".");
	}

	/**
	 * Find entries given a parent entry. It could return real entries and/or
	 * virtual entries.
	 * 
	 * @param parent
	 * @param scope
	 * @throws Exception
	 */
	public void searchChildren(
            PenroseConnection connection,
			Entry parent,
            int scope,
            Filter filter,
			Collection attributeNames,
            SearchResults results) throws Exception {

		EntryDefinition parentEntry = parent.getEntryDefinition();
		Collection children = parentEntry.getChildren();
		log.debug("Total children: " + children.size());

		for (Iterator i = children.iterator(); i.hasNext();) {
			EntryDefinition entryDefinition = (EntryDefinition) i.next();
			if (entryDefinition.isDynamic()) continue;

			log.debug("Static children: " + entryDefinition.getDn());

			AttributeValues values = entryDefinition.getAttributeValues(engineContext.newInterpreter());
			Entry sr = new Entry(entryDefinition, values);

			if (engineContext.getFilterTool().isValidEntry(sr, filter)) {
                LDAPEntry en = sr.toLDAPEntry();
				results.add(Entry.filterAttributes(en, attributeNames));
			}

			if (scope == 2) {
				searchChildren(connection, sr, scope, filter, attributeNames, results);
			}
		}

		for (Iterator i = children.iterator(); i.hasNext();) {
			EntryDefinition entryDefinition = (EntryDefinition) i.next();
			if (!entryDefinition.isDynamic()) continue;

			log.debug("Virtual children: " + entryDefinition.getDn());

			SearchResults results2 = searchVirtualEntries(parent, entryDefinition, filter);

            for (Iterator j=results2.iterator(); j.hasNext(); ) {
                Entry sr = (Entry)j.next();

                if (engineContext.getFilterTool().isValidEntry(sr, filter)) {
                    LDAPEntry en = sr.toLDAPEntry();
                    results.add(Entry.filterAttributes(en, attributeNames));
                }
            }
		}
	}

	/**
	 * Find a virtual entry given an rdn and a mapping entry.
	 * 
	 * @param rdn
	 * @param entryDefinition
	 * @return the entry
	 * @throws Exception
	 */
	public Entry searchVirtualEntry(
            PenroseConnection connection,
			EntryDefinition entryDefinition,
            String rdn)
			throws Exception {

        int i = rdn.indexOf("=");
        String attr = rdn.substring(0, i);
        String value = rdn.substring(i + 1);

        Row pk = new Row();
        pk.set(attr, value);

        List pks = new ArrayList();
        pks.add(pk);

        // find entry using rdn as primary key
        loadObject(entryDefinition, pk);
        SearchResults results = getEntries(entryDefinition, pks);

        if (results.size() == 0) return null;

        // there should be only one entry
        Entry entry = (Entry)results.iterator().next();

        log.debug("Checking " + entry.getDn() + ": " + entry.getAttributeValues());

		Map rdnValues = Entry.parseRdn(rdn);
        AttributeValues values = entry.getAttributeValues();

        // check all values in rdn
        for (Iterator j = rdnValues.keySet().iterator(); j.hasNext();) {
            String rdnName = (String) j.next();

            String rdnValue = ((String)rdnValues.get(rdnName)).toLowerCase();
            Collection v = values.get(rdnName);

            // convert to lower cases
            List v2 = new ArrayList();
            for (Iterator k=v.iterator(); k.hasNext(); ) {
                String val = (String)k.next();
                v2.add(val.toLowerCase());
            }

            log.debug("Checking "+rdnName+"="+rdnValue+" with "+v2);
            if (!v2.contains(rdnValue)) {
                return null;
            }
        }

		return entry;
	}

    public SearchResults searchVirtualEntries(
            Entry parent,
            EntryDefinition entryDefinition,
            Filter filter
            ) throws Exception {

        Graph graph = createGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);;

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Searching for entry "+entryDefinition.getDn()+" with filter "+filter);

        try {
            Calendar calendar = Calendar.getInstance();

            // find the primary keys of the entries to be loaded
            Collection pks = getPrimaryKeys(parent, entryDefinition, graph, primarySource, filter, calendar);
            log.debug("Keys: "+pks);

            // find the primary keys of entries that has been loaded
            Collection loadedPks = engine.getEntryCache().findPrimaryKeys(entryDefinition, filter);
            log.debug("Loaded Keys: "+loadedPks);

            if (!pks.isEmpty() && loadedPks.isEmpty()) { // never been loaded -> load everything at once

                Filter f = createFilter(pks);
                loadSources(entryDefinition, graph, primarySource, pks, f, calendar);
                joinSources(entryDefinition, graph, primarySource, f, calendar);

            } else { // some has been loaded -> load each entry individually

                for (Iterator i=pks.iterator(); i.hasNext(); ) {
                    Row pk = (Row)i.next();
                    if (loadedPks.contains(pk)) continue;

                    loadObject(entryDefinition, pk);
                }
            }

            return getEntries(entryDefinition, pks);

        } finally {
        }
    }

    public void loadObject(
            EntryDefinition entryDefinition,
            Row pk) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Loading entry "+entryDefinition.getDn()+" with pk "+pk);

        Graph graph = createGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);;

        Calendar calendar = Calendar.getInstance();

        String s = engine.getSourceCache().getParameter(SourceCacheConfig.CACHE_EXPIRATION);
        int cacheExpiration = s == null ? 0 : Integer.parseInt(s);
        log.debug("Expiration: "+cacheExpiration);
        if (cacheExpiration < 0) cacheExpiration = Integer.MAX_VALUE;

        Calendar c = (Calendar) calendar.clone();
        c.add(Calendar.MINUTE, -cacheExpiration);

        Date modifyTime = engine.getEntryCache().getModifyTime(entryDefinition, pk);
        boolean expired = modifyTime == null || modifyTime.before(c.getTime());

        log.debug("Comparing "+modifyTime+" with "+c.getTime()+" => "+(expired ? "expired" : "not expired"));

        if (expired) { // if cache expired => load this entry only
            Filter filter = createFilter(pk);
            List pks = new ArrayList();
            pks.add(pk);
            loadSources(entryDefinition, graph, primarySource, pks, filter, calendar);
            joinSources(entryDefinition, graph, primarySource, filter, calendar);
        }
    }

    public SearchResults getEntries(
            EntryDefinition entryDefinition,
            Collection keys) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Getting entries from cache with pks "+keys);

        MRSWLock lock = engine.getLock(entryDefinition.getDn());
        lock.getReadLock(Penrose.WAIT_TIMEOUT);

        SearchResults results = new SearchResults();

        try {
            Map entries = engine.getEntryCache().get(entryDefinition, keys);

            for (Iterator i = entries.values().iterator(); i.hasNext();) {
                Entry sr = (Entry) i.next();
                log.debug("Returning "+sr.getDn());
                results.add(sr);
            }

        } finally {
            lock.releaseReadLock(Penrose.WAIT_TIMEOUT);
            results.close();
        }

        return results;
    }

    public Graph createGraph(EntryDefinition entryDefinition) throws Exception {

        Graph graph = new Graph();

        Collection sources = entryDefinition.getEffectiveSources();
        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();
            graph.addNode(source);
        }

        Collection relationships = entryDefinition.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            // System.out.println("Checking ["+relationship.getExpression()+"]");

            String lhs = relationship.getLhs();
            int li = lhs.indexOf(".");
            String lsourceName = lhs.substring(0, li);

            String rhs = relationship.getRhs();
            int ri = rhs.indexOf(".");
            String rsourceName = rhs.substring(0, ri);

            Source lsource = entryDefinition.getEffectiveSource(lsourceName);
            Source rsource = entryDefinition.getEffectiveSource(rsourceName);
            graph.addEdge(lsource, rsource, relationship);
        }

        System.out.println("Graph: "+graph);

        return graph;
    }
/*
    public Set traverseGraph(
            EntryDefinition entryDefinition,
            String start, String dest,
            Map sources, Set keys,
            Filter filter,
            Source primarySource,
            Set visited) throws Exception {

        Collection c = (Collection)sources.get(dest);
        if (c == null) return null;

        for (Iterator i=c.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            if (visited.contains(relationship)) continue;
            visited.add(relationship);

            String lhs = relationship.getLhs();
            int li = lhs.indexOf(".");
            String lsource = lhs.substring(0, li);
            String lexp = lhs.substring(li+1);

            String rhs = relationship.getRhs();
            int ri = rhs.indexOf(".");
            String rsource = rhs.substring(0, ri);
            String rexp = rhs.substring(ri+1);

            if (!dest.equals(lsource)) {
                Set result = traverseGraph(
                        entryDefinition,
                        rhs, lhs,
                        dest, lsource,
                        rexp, lexp,
                        sources, keys, filter, primarySource, visited);
                if (result != null) return result;
            }

            if (!dest.equals(rsource)) {
                Set result = traverseGraph(
                        entryDefinition,
                        lhs, rhs,
                        dest, rsource,
                        lexp, rexp,
                        sources, keys, filter, primarySource, visited);
                if (result != null) return result;
            }
        }

        return null;
    }

    public Set traverseGraph(
            EntryDefinition entryDefinition,
            String lhs, String rhs,
            String lsource, String rsource,
            String lfield, String rfield,
            Map sources, Set keys, Filter filter, Source primarySource,
            Set visited) throws Exception {

        log.debug("Evaluating "+lhs+" => "+rhs);
        log.debug("Old keys: "+keys);
        Source source = entryDefinition.getSource(rsource);

        Set newKeys = new HashSet();
        for (Iterator j=keys.iterator(); j.hasNext(); ) {
            Row pk = (Row)j.next();
            Object value = pk.get(lfield);

            Row newPk = new Row();
            newPk.set(rfield, value);

            newKeys.add(newPk);
        }
        log.debug("New keys: "+newKeys);

        Filter newFilter = createFilter(newKeys);
        log.debug("New filter "+newFilter);

        log.debug("Searching source "+source.getName()+" for "+filter);
        SearchResults results = source.search(newFilter);

        newKeys = new HashSet();
        for (Iterator j=results.iterator(); j.hasNext(); ) {
            Row row = (Row)j.next();
            //log.debug(" - "+row);
            newKeys.add(row);
        }

        if (source.getName().equals(primarySource.getName())) {
            return newKeys;
        }

        return traverseGraph(
                entryDefinition,
                lsource, rsource,
                sources, newKeys, filter, primarySource, visited);
    }

    public Collection traverseGraph(EntryDefinition entryDefinition, Row row, Map sources, Filter filter, Source primarySource) throws Exception {

        String exp = null;
        String sourceName = null;
        String fieldName = null;

        Collection relationships = entryDefinition.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            String lhs = relationship.getLhs();
            int li = lhs.indexOf(".");
            String lsource = lhs.substring(0, li);
            String lfield = lhs.substring(li+1);

            String rhs = relationship.getRhs();
            int ri = rhs.indexOf(".");
            String rsource = rhs.substring(0, ri);
            String rfield = rhs.substring(ri+1);

            Source ls = entryDefinition.getSource(lsource);
            if (ls == null) {
                exp = lhs;
                sourceName = lsource;
                fieldName = lfield;
                break;
            }

            Source rs = entryDefinition.getSource(lsource);
            if (rs == null) {
                exp = rhs;
                sourceName = rsource;
                fieldName = rfield;
                break;
            }

        }

        if (sourceName == null) {
            Source source = (Source)entryDefinition.getSources().iterator().next();
            sourceName = source.getName();
        }

        log.debug("Converting "+filter+" for " + sourceName+" with "+row);
        Source source = entryDefinition.getSource(sourceName);
        Filter sqlFilter = engine.getSourceCache().getCacheFilterTool().toSourceFilter(row, entryDefinition, source, filter);

        Object value = row.get(exp);
        log.debug("Adding "+exp+"="+ value);
        Set keys = new HashSet();

        if (value != null) {
            Row pk = new Row();
            pk.set(fieldName, value);
            keys.add(pk);

            SimpleFilter sf = new SimpleFilter(fieldName, "=", value.toString());
            if (sqlFilter == null) {
                sqlFilter = sf;
            } else if (sqlFilter instanceof AndFilter) {
                AndFilter andFilter = (AndFilter)sqlFilter;
                andFilter.addFilterList(sf);
            } else {
                AndFilter andFilter = new AndFilter();
                andFilter.addFilterList(sf);
                andFilter.addFilterList(sqlFilter);
                sqlFilter = andFilter;
            }
        }

        Set visited = new LinkedHashSet();
        Set result = traverseGraph(entryDefinition, null, sourceName, sources, keys, sqlFilter, primarySource, visited);
        return result;
    }
*/
    public String getStartingSourceName(EntryDefinition entryDefinition) {

        Collection relationships = entryDefinition.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            String lhs = relationship.getLhs();
            int li = lhs.indexOf(".");
            String lsource = lhs.substring(0, li);
            Source ls = entryDefinition.getSource(lsource);
            if (ls == null) return lsource;

            String rhs = relationship.getRhs();
            int ri = rhs.indexOf(".");
            String rsource = rhs.substring(0, ri);
            Source rs = entryDefinition.getSource(lsource);
            if (rs == null) return rsource;

        }

        Source source = (Source)entryDefinition.getSources().iterator().next();
        return source.getName();
    }

    public Source getPrimarySource(EntryDefinition entryDefinition) {

        Collection rdnAttributes = entryDefinition.getRdnAttributes();

        // TODO need to handle multiple rdn attributes
        AttributeDefinition rdnAttribute = (AttributeDefinition)rdnAttributes.iterator().next();
        String exp = rdnAttribute.getExpression();

        // TODO need to handle complex expression
        int index = exp.indexOf(".");
        String primarySourceName = exp.substring(0, index);

        for (Iterator i = entryDefinition.getSources().iterator(); i.hasNext();) {
            Source source = (Source) i.next();
            if (source.getName().equals(primarySourceName)) return source;
        }

        return null;
    }

    public Collection getPrimaryKeys(
            Entry parent,
            EntryDefinition entryDefinition,
            Graph graph,
            Source primarySource,
            Filter filter,
            Calendar calendar
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");

        if (parent != null && parent.isDynamic()) {

            AttributeValues values = parent.getAttributeValues();
            Collection rows = engineContext.getTransformEngine().convert(values);
            Row row = (Row)rows.iterator().next();

            Interpreter interpreter = engineContext.newInterpreter();
            interpreter.set(row);

            Row initialRow = new Row();

            for (Iterator i=parent.getSources().iterator(); i.hasNext(); ) {
                Source s = (Source)i.next();

                for (Iterator j=s.getFields().iterator(); j.hasNext(); ) {
                    Field f = (Field)j.next();
                    String expression = f.getExpression();
                    Object v = interpreter.eval(expression);

                    System.out.println("Setting parent's value "+s.getName()+"."+f.getName()+": "+v);
                    initialRow.set(s.getName()+"."+f.getName(), v);
                }
            }

            String startingSourceName = getStartingSourceName(entryDefinition);
            Source startingSource = entryDefinition.getEffectiveSource(startingSourceName);

            PrimaryKeyGraphVisitor visitor = new PrimaryKeyGraphVisitor(engine, entryDefinition, initialRow);
            graph.traverse(visitor, startingSource);
            return visitor.getKeys();

        } else {

            String primarySourceName = primarySource.getName();
            log.debug("Primary source: "+primarySourceName);

            Filter f = engine.getSourceCache().getCacheFilterTool().toSourceFilter(null, entryDefinition, primarySource, filter);

            log.debug("Searching source "+primarySourceName+" for "+f);
            SearchResults results = primarySource.search(f);

            Set keys = new HashSet();

            for (Iterator j=results.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();

                Interpreter interpreter = engineContext.newInterpreter();
                for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    Object value = row.get(name);
                    interpreter.set(primarySourceName+"."+name, value);
                }

                Collection rdnAttributes = entryDefinition.getRdnAttributes();

                Row pk = new Row();
                boolean valid = true;

                for (Iterator k=rdnAttributes.iterator(); k.hasNext(); ) {
                    AttributeDefinition attr = (AttributeDefinition)k.next();
                    String name = attr.getName();
                    String expression = attr.getExpression();
                    Object value = interpreter.eval(expression);

                    if (value == null) {
                        valid = false;
                        break;
                    }

                    pk.set(name, value);
                }

                if (!valid) continue;
                keys.add(pk);
            }

            return keys;
        }
/*
        List list = new ArrayList();
        List nullList = new ArrayList();
        Map filters = new HashMap();

        for (Iterator i = entryDefinition.getSources().iterator(); i.hasNext();) {
            Source source = (Source) i.next();

            String sourceName = source.getName();

            Filter sqlFilter = engine.getSourceCache().getCacheFilterTool().toSourceFilter(initialRow, entryDefinition, source, filter);
            filters.put(sourceName, sqlFilter);

            log.debug("Filter for " + sourceName+": "+sqlFilter);

            if (sqlFilter == null) { // filter is not applicable to this source
                nullList.add(source);
            } else { // filter is applicable to this source
                list.add(source);
            }
        }

        log.debug("--------------------------------------------------------------------------------------");

        Collection rows = traverseGraph(entryDefinition, initialRow, sourceGraph, filter, primarySource);

        Set keys = new HashSet();

        if (rows != null) {
            log.debug("Found: ");
            for (Iterator j=rows.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();

                Interpreter interpreter = engineContext.newInterpreter();
                for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    Object value = row.get(name);
                    interpreter.set(primarySourceName+"."+name, value);
                }

                Collection rdnAttributes = entryDefinition.getRdnAttributes();

                Row pk = new Row();
                for (Iterator k=rdnAttributes.iterator(); k.hasNext(); ) {
                    AttributeDefinition attr = (AttributeDefinition)k.next();
                    String name = attr.getName();
                    String expression = attr.getExpression();
                    Object value = interpreter.eval(expression);
                    pk.set(name, value);
                }

                log.debug(" - "+row+" => "+pk);

                keys.add(pk);
            }

        } else {
            Filter primaryFilter;

            Collection relationships = entryDefinition.getRelationships();
            Relationship relationship = null;
            if (relationships.size() > 0) {
                relationship = (Relationship)relationships.iterator().next();
            }

            // TODO convert this into a loop
            if (!list.isEmpty() && !nullList.isEmpty()) {

                Source source = (Source)list.iterator().next();
                String sourceName = source.getName();

                String lhs = relationship.getLhs();
                String rhs = relationship.getRhs();

                String keyName;
                String nextKeyName;

                if (lhs.startsWith(sourceName+".")) {
                    keyName = lhs.substring(lhs.indexOf(".")+1);
                    nextKeyName = rhs.substring(rhs.indexOf(".")+1);
                } else {
                    keyName = rhs.substring(rhs.indexOf(".")+1);
                    nextKeyName = lhs.substring(lhs.indexOf(".")+1);
                }

                Filter sourceFilter = (Filter)filters.get(sourceName);

                log.debug("Searching source "+sourceName+" for "+sourceFilter);
                SearchResults results = source.search(sourceFilter);

                Set pks = new HashSet();
                for (Iterator j=results.iterator(); j.hasNext(); ) {
                    Row values = (Row)j.next();
                    Object value = values.get(keyName);

                    Row pk = new Row();
                    pk.set(nextKeyName, value);

                    pks.add(pk);
                }

                primaryFilter = createFilter(pks);

            } else {
                primaryFilter = engine.getSourceCache().getCacheFilterTool().toSourceFilter(initialRow, entryDefinition, primarySource, filter);
            }

            log.debug("Searching source "+primarySourceName+" for "+primaryFilter);
            SearchResults results = primarySource.search(primaryFilter);

            for (Iterator j=results.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();

                Interpreter interpreter = engineContext.newInterpreter();
                for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    Object value = row.get(name);
                    interpreter.set(primarySourceName+"."+name, value);
                }

                Collection rdnAttributes = entryDefinition.getRdnAttributes();

                Row pk = new Row();
                for (Iterator k=rdnAttributes.iterator(); k.hasNext(); ) {
                    AttributeDefinition attr = (AttributeDefinition)k.next();
                    String name = attr.getName();
                    String expression = attr.getExpression();
                    Object value = interpreter.eval(expression);
                    pk.set(name, value);
                }

                keys.add(pk);
            }
        }

        return keys;
*/
    }

    public Filter createFilter(Collection keys) {

        Filter filter = null;

        for (Iterator i=keys.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            Filter f = createFilter(pk);

            if (filter == null) {
                filter = f;

            } else if (!(filter instanceof OrFilter)) {
                OrFilter of = new OrFilter();
                of.addFilterList(filter);
                of.addFilterList(f);
                filter = of;

            } else {
                OrFilter of = (OrFilter)filter;
                of.addFilterList(f);
            }
        }

        return filter;
    }

    public Filter createFilter(Row values) {

        Filter f = null;

        for (Iterator j=values.getNames().iterator(); j.hasNext(); ) {
            String name = (String)j.next();
            Object value = values.get(name);
            if (value == null) continue;

            SimpleFilter sf = new SimpleFilter(name, "=", value == null ? null : value.toString());

            if (f == null) {
                f = sf;

            } else if (!(f instanceof AndFilter)) {
                AndFilter af = new AndFilter();
                af.addFilterList(f);
                af.addFilterList(sf);
                f = af;

            } else {
                AndFilter af = (AndFilter)f;
                af.addFilterList(sf);
            }
        }

        return f;
    }

    /**
     * Load sources of entries matching the filter.
     *
     * @param entryDefinition
     * @param filter
     * @param calendar
     * @throws Exception
     */
    public void loadSources(
            EntryDefinition entryDefinition,
            Graph graph,
            Source primarySource,
            Collection rdns,
            Filter filter,
            Calendar calendar
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Loading all sources with filter " + filter);

        // convert rdns into primary keys
        Collection pks = new HashSet();
        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            Row rdn = (Row)i.next();

            Interpreter interpreter = engineContext.newInterpreter();
            interpreter.set(rdn);

            Collection fields = primarySource.getPrimaryKeyFields();
            Row pk = new Row();

            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();
                String name = field.getName();
                String expression = field.getExpression();

                Object value = interpreter.eval(expression);
                pk.set(name, value);
            }

            pks.add(pk);
        }

        SourceLoaderGraphVisitor visitor = new SourceLoaderGraphVisitor(engine, entryDefinition, pks, calendar.getTime());
        graph.traverse(visitor, primarySource);
        //return visitor.getKeys();
/*
        // get the first relationship
        Collection relationships = entryDefinition.getRelationships();
        Relationship relationship = null;
        if (relationships.size() > 0) {
            relationship = (Relationship)relationships.iterator().next();
        }

        Set keyNames = new HashSet();

        Collection rdnAttributes = entryDefinition.getRdnAttributes();
        for (Iterator i=rdnAttributes.iterator(); i.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)i.next();
            keyNames.add(new String[] { attribute.getName(), attribute.getName() });
        }

        for (Iterator i = entryDefinition.getSources().iterator(); i.hasNext();) {
            Source source = (Source) i.next();
            String sourceName = source.getName();

            Filter f = engine.getSourceCache().getCacheFilterTool().toSourceFilter(null, entryDefinition, source, filter);

            log.debug("--------------------------------------------------------------------------------------");
            log.debug("Loading source " + source.getName() + " with filter " + f);

            if (relationship != null) {

                keyNames = new HashSet();

                String keyName = null;
                String nextKeyName = null;

                String lhs = relationship.getLhs();
                String rhs = relationship.getRhs();

                if (lhs.startsWith(sourceName+".")) {
                    keyName = lhs.substring(sourceName.length()+1);
                    int p = rhs.indexOf(".");
                    nextKeyName = rhs.substring(p+1);

                } else {
                    keyName = rhs.substring(sourceName.length()+1);
                    int p = lhs.indexOf(".");
                    nextKeyName = lhs.substring(p+1);
                }

                keyNames.add(new String[] { keyName, nextKeyName });
            }

            MRSWLock lock = engine.getLock(source);
            lock.getWriteLock(Penrose.WAIT_TIMEOUT);

            try {

                SearchResults results = engine.getSourceCache().loadSource(entryDefinition, source, f, calendar.getTime());

                // update key values
                Set keys = new HashSet();
                for (Iterator j=results.iterator(); j.hasNext(); ) {
                    Row values = (Row)j.next();

                    Row key = new Row();
                    for (Iterator k=keyNames.iterator(); k.hasNext(); ) {
                        String[] names = (String[])k.next();
                        Object keyValue = values.get(names[0]);
                        key.set(names[1], keyValue);
                    }

                    keys.add(key);
                }

                f = createFilter(keys);
                log.debug("new filter " + f);

            } finally {
                lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
            }
       }
*/
    }

    public void joinSources(
            EntryDefinition entryDefinition,
            Graph graph,
            Source primarySource,
            Filter filter,
            Calendar calendar
            ) throws Exception {

        Filter newFilter = engine.getSourceCache().getCacheFilterTool().toSourceFilter(null, entryDefinition, primarySource, filter);
        String sqlFilter = engine.getSourceCache().getCacheFilterTool().toSQLFilter(entryDefinition, newFilter);

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Joining sources with filter "+filter);
        log.debug(" - new filter: "+newFilter);
        log.debug(" - sql filter: "+sqlFilter);

        MRSWLock lock = engine.getLock(entryDefinition.getDn());
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

            // join rows from sources
            Collection rows = engine.getSourceCache().joinSources(entryDefinition, graph, primarySource, sqlFilter);
            log.debug("Joined " + rows.size() + " rows.");

            // merge rows into attribute values
            Map entries = new HashMap();
            for (Iterator i = rows.iterator(); i.hasNext();) {
                Row row = (Row)i.next();
                Map pk = new HashMap();
                Row translatedRow = new Row();

                boolean validPK = engineContext.getTransformEngine().translate(entryDefinition, row, pk, translatedRow);
                if (!validPK) continue;

                AttributeValues values = (AttributeValues)entries.get(pk);
                if (values == null) {
                    values = new AttributeValues();
                    entries.put(pk, values);
                }
                values.add(translatedRow);
            }

            // update attribute values in entry cache
            for (Iterator i=entries.values().iterator(); i.hasNext(); ) {
                AttributeValues values = (AttributeValues)i.next();
                engine.getEntryCache().remove(entryDefinition, values, calendar.getTime());
                engine.getEntryCache().put(entryDefinition, values, calendar.getTime());
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }
    }

}