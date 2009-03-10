package org.safehaus.penrose.nis.directory;

import org.safehaus.penrose.directory.DynamicEntry;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntrySource;
import org.safehaus.penrose.directory.EntrySearchOperation;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.nis.NISMap;
import org.safehaus.penrose.nis.NISObject;
import org.safehaus.penrose.nis.source.NISAutomountsSource;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.Penrose;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class NISAutomountsEntry extends DynamicEntry {

    NISAutomountsSource automountsSource;
    String base;

    public void init() throws Exception {
        EntrySource sourceRef = getSource();
        automountsSource = (NISAutomountsSource)sourceRef.getSource();
        base = automountsSource.getParameter(NISAutomountsSource.BASE);

        super.init();
    }


    public boolean contains(DN dn) throws Exception {

        DN entryDn = getDn();

        if (entryDn.getLength() == dn.getLength()) {
            return entryDn.matches(dn);

        } else if (entryDn.getLength() < dn.getLength()) {
            return entryDn.endsWith(dn);
        }

        return false;
    }

    public Collection<Entry> findEntries(DN dn) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (dn == null) return EMPTY_ENTRIES;

        DN entryDn = getDn();

        if (!dn.endsWith(entryDn)) {
            return EMPTY_ENTRIES;
        }

        Collection<Entry> results = new ArrayList<Entry>();

        if (debug) log.debug("Found entry \""+entryDn+"\".");
        results.add(this);

        return results;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            SearchOperation operation
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        final DN baseDn     = operation.getDn();
        final Filter filter = operation.getFilter();
        final int scope     = operation.getScope();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("AUTOMOUNTS SEARCH", 70));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 70));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 70));
            log.debug(TextUtil.displayLine("Filter : "+filter, 70));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        EntrySearchOperation op = new EntrySearchOperation(operation, this);

        try {
            if (!validate(op)) return;

            expand(op);

        } finally {
            op.close();
        }
    }

    public boolean validateScope(SearchOperation operation) throws Exception {
        return true;
    }

    public boolean validateFilter(SearchOperation operation) throws Exception {
        return true;
    }

    public void expand(
            SearchOperation operation
    ) throws Exception {

        DN baseDn = operation.getDn();
        DN entryDn = getDn();

        int level = baseDn.getLength() - entryDn.getLength();

        if (level < -1 && entryDn.endsWith(baseDn)) {
            searchAncestor(operation);

        } else if (level == -1 && entryDn.getParentDn().matches(baseDn)) {
            searchParent(operation);

        } else if (level == 0 && entryDn.matches(baseDn)) {
            searchEntry(operation);

        } else if (level > 0 && baseDn.endsWith(entryDn)) {
            searchChildren(operation);
        }
    }

    public void searchAncestor(
            SearchOperation operation
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        Session session = operation.getSession();

        if (debug) log.debug("Searching ancestor");

        DN baseDn = operation.getDn();
        int scope = operation.getScope();

        boolean baseSearch     = scope == SearchRequest.SCOPE_BASE;
        boolean oneLevelSearch = scope == SearchRequest.SCOPE_ONE;

        if (baseSearch || oneLevelSearch) return;

        SearchResult automountsSearchResult = createAutomountsSearchResult();
        operation.add(automountsSearchResult);

        Map<String,NISMap> maps = getAutomountMaps(session);

        for (NISMap map : maps.values()) {

            DN automountMapDn = createAutomountMapDn(baseDn, map.getName());

            SearchResult automountMapSearchResult = createAutomountMapSearchResult(automountMapDn, map);
            operation.add(automountMapSearchResult);
        }

        for (NISMap map : maps.values()) {

            DN automountMapDn = createAutomountMapDn(baseDn, map.getName());

            for (NISObject object : map.getObjects()) {

                DN automountMapEntryDn = createAutomountMapEntryDn(automountMapDn, object.getName());

                SearchResult automountMapEntrySearchResult = createAutomountMapEntrySearchResult(automountMapEntryDn, map, object);
                operation.add(automountMapEntrySearchResult);
            }
        }
    }

    public void searchParent(
            SearchOperation operation
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        Session session = operation.getSession();

        if (debug) log.debug("Searching parent");

        DN baseDn = operation.getDn();
        int scope = operation.getScope();

        boolean baseSearch     = scope == SearchRequest.SCOPE_BASE;
        boolean oneLevelSearch = scope == SearchRequest.SCOPE_ONE;

        if (baseSearch) return;

        SearchResult automountsSearchResult = createAutomountsSearchResult();
        operation.add(automountsSearchResult);

        if (oneLevelSearch) return;

        Map<String,NISMap> maps = getAutomountMaps(session);

        for (NISMap map : maps.values()) {

            DN automountMapDn = createAutomountMapDn(baseDn, map.getName());

            SearchResult automountMapSearchResult = createAutomountMapSearchResult(automountMapDn, map);
            operation.add(automountMapSearchResult);
        }

        for (NISMap map : maps.values()) {

            DN automountMapDn = createAutomountMapDn(baseDn, map.getName());

            for (NISObject object : map.getObjects()) {

                DN automountMapEntryDn = createAutomountMapEntryDn(automountMapDn, object.getName());

                SearchResult automountMapEntrySearchResult = createAutomountMapEntrySearchResult(automountMapEntryDn, map, object);
                operation.add(automountMapEntrySearchResult);
            }
        }
    }

    public void searchEntry(
            SearchOperation operation
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        Session session = operation.getSession();

        if (debug) log.debug("Searching entry");

        DN baseDn = operation.getDn();
        int scope = operation.getScope();

        boolean baseSearch     = scope == SearchRequest.SCOPE_BASE;
        boolean oneLevelSearch = scope == SearchRequest.SCOPE_ONE;
        boolean subtreeSearch  = scope == SearchRequest.SCOPE_SUB;

        if (baseSearch || subtreeSearch) {
            SearchResult automountsSearchResult = createAutomountsSearchResult();
            operation.add(automountsSearchResult);

            if (baseSearch) return;
        }

        Map<String,NISMap> maps = getAutomountMaps(session);

        if (oneLevelSearch || subtreeSearch) {

            for (NISMap map : maps.values()) {

                DN automountMapDn = createAutomountMapDn(baseDn, map.getName());

                SearchResult automountMapSearchResult = createAutomountMapSearchResult(automountMapDn, map);
                operation.add(automountMapSearchResult);
            }

            if (oneLevelSearch) return;
        }

        for (NISMap map : maps.values()) {

            DN automountMapDn = createAutomountMapDn(baseDn, map.getName());

            for (NISObject object : map.getObjects()) {

                DN automountMapEntryDn = createAutomountMapEntryDn(automountMapDn, object.getName());

                SearchResult automountMapEntrySearchResult = createAutomountMapEntrySearchResult(automountMapEntryDn, map, object);
                operation.add(automountMapEntrySearchResult);
            }
        }
    }

    public void searchChildren(
            SearchOperation operation
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Searching children");

        DN baseDn = operation.getDn();
        DN automountsDn = getDn();

        DN automountMapDn = createAutomountMapDn(automountsDn, "...");

        if (baseDn.matches(automountMapDn)) {
            searchAutomountMap(operation);
            return;
        }

        DN automountEntryDn = createAutomountMapEntryDn(automountMapDn, "...");

        if (baseDn.matches(automountEntryDn)) {
            searchAutomountMapEntry(operation);
            return;
        }

        if (baseDn.endsWith(automountEntryDn)) {
            Penrose.errorLog.error("Entry "+baseDn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }
    }

    public DN createAutomountMapDn(
            DN automountsDn,
            String automountMapName
    ) throws Exception {

        RDNBuilder rb = new RDNBuilder();
        rb.set("nisMapName", automountMapName);
        RDN rdn = rb.toRdn();

        DNBuilder db = new DNBuilder();
        db.append(rdn);
        db.append(automountsDn);

        return db.toDn();
    }

    public DN createAutomountMapEntryDn(
            DN automountMapDn,
            String name
    ) throws Exception {

        String mapName = (String)automountMapDn.getRdn().get("nisMapName");
        if (!base.equals(mapName)) {
            if ("*".equals(name)) name = "/";
        }

        RDNBuilder rb = new RDNBuilder();
        rb.set("cn", name);
        RDN rdn = rb.toRdn();

        DNBuilder db = new DNBuilder();
        db.append(rdn);
        db.append(automountMapDn);

        return db.toDn();
    }

    public Map<String,NISMap> getAutomountMaps(Session session) throws Exception {

        boolean debug = log.isDebugEnabled();

        Map<String,NISMap> maps = new LinkedHashMap<String,NISMap>();

        NISMap autoMaster = automountsSource.getAutomountMap(session, base);
        if (autoMaster == null) return maps;

        maps.put(autoMaster.getName(), autoMaster);

        for (NISObject entry : autoMaster.getObjects()) {

            String name = entry.getName();
            String value = entry.getValue();
            String description = entry.getDescription();

            if (debug) log.debug(" - "+name+": "+value);
            if (value == null) continue;

            if (value.startsWith("-")) continue;

            StringTokenizer st = new StringTokenizer(value, " \t");
            if (!st.hasMoreTokens()) continue;

            String mapName = st.nextToken();

            if (mapName.indexOf(":") > 0) continue;

            if (mapName.startsWith("/")) {
                int i = mapName.lastIndexOf('/');
                mapName = mapName.substring(i+1);
            }

            if (mapName.startsWith("auto_")) {
                mapName = "auto." + mapName.substring(5);
            }

            NISMap map = automountsSource.getAutomountMap(session, mapName);
            if (map == null) continue;

            map.setDescription(description);

            maps.put(mapName, map);
        }

        return maps;
    }

    public void searchAutomountMap(
            SearchOperation operation
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        Session session = operation.getSession();
        SearchResponse response = operation.getSearchResponse();

        DN automountMapDn = operation.getDn();
        RDN automountMapRdn = automountMapDn.getRdn();

        String automountMapName = (String)automountMapRdn.get("nisMapName");

        if (debug) log.debug("Searching automount map "+automountMapName);

        NISMap map = automountsSource.getAutomountMap(session, automountMapName);

        if (map == null) {
            Penrose.errorLog.error("Automount map "+automountMapName+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        int scope = operation.getScope();

        if (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB) {
            SearchResult result = createAutomountMapSearchResult(automountMapDn, map);
            response.add(result);

            if (scope == SearchRequest.SCOPE_BASE) return;
        }

        if (scope == SearchRequest.SCOPE_ONE || scope == SearchRequest.SCOPE_SUB) {
            for (NISObject object : map.getObjects()) {

                DN automountMapEntryDn = createAutomountMapEntryDn(automountMapDn, object.getName());

                SearchResult result = createAutomountMapEntrySearchResult(automountMapEntryDn, map, object);
                response.add(result);
            }
        }
    }

    public void searchAutomountMapEntry(
            SearchOperation operation
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        Session session = operation.getSession();

        DN automountMapEntryDn = operation.getDn();
        RDN automountMapEntryRdn = automountMapEntryDn.getRdn();

        String name = (String)automountMapEntryRdn.get("cn");

        DN automountMapDn = automountMapEntryDn.getParentDn();
        RDN automountMapRdn = automountMapDn.getRdn();

        String mapName = (String)automountMapRdn.get("nisMapName");

        if (debug) log.debug("Searching automount "+name+" in map "+mapName);

        NISMap map = automountsSource.getAutomountMap(session, mapName);

        if (map == null) {
            Penrose.errorLog.error("Automount map "+mapName+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        if (!base.equals(mapName)) {
            if ("/".equals(name)) name = "*";
        }

        NISObject object = map.getObject(name);

        if (object == null) {
            Penrose.errorLog.error("Automount map entry "+name+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        int scope = operation.getScope();

        if (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB) {
            SearchResult result = createAutomountMapEntrySearchResult(automountMapEntryDn, map, object);
            operation.add(result);
        }
    }

    public SearchResult createAutomountsSearchResult() throws Exception {

        Interpreter interpreter = partition.newInterpreter();

        DN dn = computeDn(interpreter);
        Attributes attributes = computeAttributes(interpreter);

        SearchResult result = new SearchResult(dn, attributes);
        result.setEntryName(getName());

        return result;
    }

    public SearchResult createAutomountMapSearchResult(
            DN automountMapDn,
            NISMap map
    ) throws Exception {

        Attributes attributes = new Attributes();
        attributes.setValue("objectClass", "nisMap");
        attributes.setValue("nisMapName", map.getName());
        if (map.getDescription() != null) attributes.setValue("description", map.getDescription());

        SearchResult result = new SearchResult(automountMapDn, attributes);
        result.setEntryName(getName());

        return result;
    }

    public SearchResult createAutomountMapEntrySearchResult(
            DN automountMapEntryDn,
            NISMap map,
            NISObject object
    ) throws Exception {

        String name = object.getName();
        String value = object.getValue();

        DN automountsDn = getDn();

        if (base.equals(map.getName())) {

            if (!value.startsWith("-")) {

                StringTokenizer st = new StringTokenizer(value, " \t");
                if (st.hasMoreTokens()) {
                    String mapName = st.nextToken();
                    String remainder = value.substring(mapName.length()).trim();

                    if (mapName.indexOf(":") < 0) {

                        if (mapName.startsWith("/")) {
                            int i = mapName.lastIndexOf('/');
                            mapName = mapName.substring(i+1);
                        }

                        if (mapName.startsWith("auto_")) {
                            mapName = "auto." + mapName.substring(5);
                        }

                        DN automountMapDn = createAutomountMapDn(automountsDn, mapName);

                        value = "ldap:"+automountMapDn;
                        if (remainder.length() > 0) value += " "+remainder;
                    }
                }
            }

        } else {
            if ("*".equals(name)) name = "/";
        }

        Attributes attributes = new Attributes();
        attributes.setValue("objectClass", "nisObject");
        attributes.setValue("cn", name);
        attributes.setValue("nisMapEntry", value);
        attributes.setValue("nisMapName", map.getName());
        if (object.getDescription() != null) attributes.setValue("description", object.getDescription());

        SearchResult result = new SearchResult(automountMapEntryDn, attributes);
        result.setEntryName(getName());

        return result;
    }
}
