package org.safehaus.penrose.jdbc.scheduler;

import org.safehaus.penrose.scheduler.Job;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.directory.EntrySource;
import org.safehaus.penrose.directory.Directory;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.jdbc.source.JDBCSource;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.session.Session;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class JDBCSyncJob extends Job {

    private Map<String,Source> sources = new LinkedHashMap<String,Source>();

    private Map<String,Source> targets = new LinkedHashMap<String,Source>();
    Map<String,Source> tmpTargets = new LinkedHashMap<String,Source>();

    private Map<String,Collection<String>> relations = new LinkedHashMap<String,Collection<String>>();

    private Map<String,Entry> entries = new LinkedHashMap<String,Entry>();
    Map<String,Entry> tmpEntries = new LinkedHashMap<String,Entry>();

    private String baseDn;
    private int scope;

    private Source changelog;

    public void init() throws Exception {

        log.debug("Initializing "+this.getName()+" job...");

        SourceManager sourceManager = partition.getSourceManager();

         log.debug("Sources:");

        String sourceNames = jobConfig.getParameter("source");
        StringTokenizer st = new StringTokenizer(sourceNames, "; ");
        while (st.hasMoreTokens()) {
            String sourceName = st.nextToken();
            log.debug(" - "+sourceName);

            Source source = sourceManager.getSource(sourceName);
            sources.put(sourceName, source);
        }

        Iterator<String> i = sources.keySet().iterator();

        log.debug("Targets:");

        String targetNames = jobConfig.getParameter("target");
        st = new StringTokenizer(targetNames, "; ");

        while (i.hasNext() && st.hasMoreTokens()) {
            String sourceName = i.next();
            String groups = st.nextToken();

            Collection<String> list = new ArrayList<String>();
            relations.put(sourceName, list);

            st = new StringTokenizer(groups, ", ");
            while (st.hasMoreTokens()) {
                String targetName = st.nextToken();
                list.add(targetName);

                Source target = sourceManager.getSource(targetName);
                targets.put(targetName, target);

                Source tmp = createTmpTarget(target);
                tmpTargets.put(tmp.getName(), tmp);

                log.debug(" - "+sourceName+": "+target.getParameter(JDBCSource.TABLE)+" => "+tmp.getParameter(JDBCSource.TABLE));
            }
        }

        baseDn = jobConfig.getParameter("baseDn");

        String scope = jobConfig.getParameter("scope");
        if ("OBJECT".equals(scope)) {
            this.scope = SearchRequest.SCOPE_BASE;

        } else if ("ONELEVEL".equals(scope)) {
            this.scope = SearchRequest.SCOPE_ONE;

        } else { // if ("SUBTREE".equals(scope))
            this.scope = SearchRequest.SCOPE_SUB;
        }

        Directory directory = partition.getDirectory();
        for (Entry entry : directory.findEntries(baseDn)) {
            entries.put(entry.getId(), entry);

            Entry tmpEntry = createTmpEntry(entry);
            tmpEntries.put(tmpEntry.getId(), tmpEntry);
        }

        String changeLogName = jobConfig.getParameter("changelog");
        changelog = sourceManager.getSource(changeLogName);
    }

    public Source createTmpTarget(Source target) throws Exception {
        String tableName = target.getParameter(JDBCSource.TABLE);

        Source tmp = (Source)target.clone();
        tmp.setParameter(JDBCSource.TABLE, tableName+"_tmp");

        return tmp;
    }

    public Entry createTmpEntry(Entry entry) throws Exception {
        Entry tmpEntry = (Entry)entry.clone();

        for (EntrySource sourceRef : tmpEntry.getLocalSources()) {
            Source source = sourceRef.getSource();
            String name = source.getName();

            if (!targets.containsKey(name)) continue;

            Source tmpSource = tmpTargets.get(name);
            sourceRef.setSource(tmpSource);
        }

        log.debug("Old entry "+entry.getDn());

        for (EntrySource sourceRef : entry.getLocalSources()) {
            Source source = sourceRef.getSource();
            log.debug(" - "+sourceRef.getAlias()+": "+source.getParameter(JDBCSource.TABLE));
        }

        log.debug("New entry "+tmpEntry.getDn());

        for (EntrySource sourceRef : tmpEntry.getLocalSources()) {
            Source source = sourceRef.getSource();
            log.debug(" - "+sourceRef.getAlias()+": "+source.getParameter(JDBCSource.TABLE));
        }

        tmpEntry.removeChildren();

        for (Entry child : entry.getChildren()) {
            Entry tmpChild = createTmpEntry(child);
            tmpChild.setParent(tmpEntry);
            tmpEntry.addChild(tmpChild);
        }

        return tmpEntry;
    }

    public void execute() throws Exception {
        synchronize();
    }

    public void create() throws Exception {

        log.debug("============================================================================================");
        log.debug("Creating "+targets.keySet());

        for (Source target : targets.values()) {
            try {
                target.create();
            } catch (Exception e) {
                log.error("Failed to create " + target.getName() + ": " + e.getMessage());
            }
        }
    }

    public void load() throws Exception {
        log.debug("============================================================================================");
        log.debug("Loading "+targets.keySet());

        initTmpEntries();
        loadSources();
        switchSources();
    }

    public void synchronize() throws Exception {

        log.debug("============================================================================================");
        log.debug("Synchronizing "+targets.keySet());

        initTmpEntries();
        loadSources();

        if (changelog != null) {
            generateChangeLogs();
        }

        switchSources();
    }

    public void clear() throws Exception {

        log.debug("============================================================================================");
        log.debug("Clearing "+targets.keySet());

        Session session = createAdminSession();

        try {
            for (Source target : targets.values()) {
                try {
                    target.clear(session);
                } catch (Exception e) {
                    log.error("Failed to create " + target.getName() + ": " + e.getMessage());
                }
            }

        } finally {
            session.close();
        }
    }

    public void drop() throws Exception {

        log.debug("============================================================================================");
        log.debug("Dropping "+targets.keySet());

        for (Source target : targets.values()) {
            try {
                target.drop();
            } catch (Exception e) {
                log.error("Failed to create " + target.getName() + ": " + e.getMessage());
            }
        }
    }

    public void initTmpEntries() throws Exception {

        for (Source tmp : tmpTargets.values()) {
            try {
                tmp.create();
            } catch (Exception e) {
                log.error("Failed to create " + tmp.getName() + ": " + e.getMessage());
                tmp.drop();
                tmp.create();
            }
        }
    }

    public List<Collection<EntrySource>> getGroupsOfSources(Collection<Source> sources) throws Exception {

        List<Collection<EntrySource>> results = new ArrayList<Collection<EntrySource>>();

        Collection<EntrySource> list = new ArrayList<EntrySource>();
        Connection lastConnection = null;

        for (Source source : sources) {

            EntrySource sourceRef = new EntrySource(source);

            Connection connection = source.getConnection();

            if (lastConnection == null) {
                lastConnection = connection;

            } else if (lastConnection != connection || !connection.isJoinSupported()) {
                results.add(list);
                list = new ArrayList<EntrySource>();
                lastConnection = connection;
            }

            list.add(sourceRef);
        }

        if (!list.isEmpty()) results.add(list);

        return results;
    }

    public void loadSources() throws Exception {

        Session session = createAdminSession();

        try {
            List<Collection<EntrySource>> groupsOfSources = getGroupsOfSources(sources.values());

            for (Collection<EntrySource> sourceRefs : groupsOfSources) {
                try {

                    EntrySource sourceRef = sourceRefs.iterator().next();

                    //Collection<SourceRef> primarySourceRefs = new ArrayList<SourceRef>();
                    //primarySourceRefs.add(sourceRef);

                    Collection<EntrySource> localSourceRefs = new ArrayList<EntrySource>();
                    localSourceRefs.addAll(sourceRefs);

                    SearchRequest request = new SearchRequest();

                    Interpreter interpreter = partition.newInterpreter();
                    SearchResponse response = new SplitSearchResponse(
                            session,
                            tmpTargets.values(),
                            interpreter
                    );

                    SearchResponse sr = new MergeSearchResponse(
                            response
                    );

                    SourceAttributes sourceValues = new SourceAttributes();

                    Source source = sourceRef.getSource();

                    source.search(
                            null,
                            //primarySourceRefs,
                            localSourceRefs,
                            sourceRefs,
                            sourceValues,
                            request,
                            sr
                    );

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

        } finally {
            session.close();
        }
    }

    public void generateChangeLogs() throws Exception {

        Session session = createAdminSession();

        try {
            for (Entry entry : entries.values()) {
                Entry tmpEntry = tmpEntries.get(entry.getId());

                log.debug("=============================================================================");
                log.debug("Searching old snapshot.");

                SearchResponse response1 = search(entry, session);

                log.debug("=============================================================================");
                log.debug("Searching new snapshot.");

                SearchResponse response2 = search(tmpEntry, session);

                generateChangeLogs(session, response1, response2);
            }

        } finally {
            session.close();
        }
    }

    public SearchResponse search(Entry entry, Session session) throws Exception {
        
        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);
        request.setScope(scope);

        SearchResponse response = new SearchResponse();

        entry.search(
                session,
                request,
                response
        );

        return response;
    }

    public void generateChangeLogs(
            Session session,
            SearchResponse response1,
            SearchResponse response2
    ) throws Exception {

        boolean b1 = response1.hasNext();
        boolean b2 = response2.hasNext();

        SearchResult result1 = b1 ? response1.next() : null;
        SearchResult result2 = b2 ? response2.next() : null;

        while (b1 && b2) {

            DN dn1 = result1.getDn();
            DN dn2 = result2.getDn();
            
            if (debug) log.debug("Comparing ["+dn1+"] with ["+dn2+"]");

            int c = dn1.compareTo(dn2);

            if (c < 0) { // delete old entry
                DeleteRequest request = new DeleteRequest();
                request.setDn(result1.getDn());
                recordDeleteOperation(session, request);

                b1 = response1.hasNext();
                if (b1) result1 = response1.next();

            } else if (c > 0) { // add new entry
                AddRequest request = new AddRequest();
                request.setDn(result2.getDn());
                request.setAttributes(result2.getAttributes());
                recordAddOperation(session, request);

                b2 = response2.hasNext();
                if (b2) result2 = response2.next();

            } else {

                Collection<Modification> modifications = createModifications(
                        result1.getAttributes(),
                        result2.getAttributes()
                );

                if (!modifications.isEmpty()) { // modify entry
                    ModifyRequest request = new ModifyRequest();
                    request.setDn(result1.getDn());
                    request.setModifications(modifications);
                    recordModifyOperation(session, request);
                }

                b1 = response1.hasNext();
                if (b1) result1 = response1.next();

                b2 = response2.hasNext();
                if (b2) result2 = response2.next();
            }
        }

        while (b1) { // delete old entries
            DeleteRequest request = new DeleteRequest();
            request.setDn(result1.getDn());
            recordDeleteOperation(session, request);

            b1 = response1.hasNext();
            if (b1) result1 = response1.next();
        }

        while (b2) { // add new entries
            AddRequest request = new AddRequest();
            request.setDn(result2.getDn());
            request.setAttributes(result2.getAttributes());
            recordAddOperation(session, request);

            b2 = response2.hasNext();
            if (b2) result2 = response2.next();
        }
    }

    public Collection<Modification> createModifications(
            Attributes attributes1,
            Attributes attributes2
    ) {
        Collection<Modification> modifications = new ArrayList<Modification>();

        Collection<String> oldAttributes = new ArrayList<String>();
        oldAttributes.addAll(attributes1.getNormalizedNames());
        oldAttributes.removeAll(attributes2.getNormalizedNames());

        for (String name : oldAttributes) {
            Attribute oldAttribute = attributes1.get(name);
            modifications.add(new Modification(Modification.DELETE, oldAttribute));
        }

        Collection<String> newAttributes = new ArrayList<String>();
        newAttributes.addAll(attributes2.getNormalizedNames());
        newAttributes.removeAll(attributes1.getNormalizedNames());

        for (String name : newAttributes) {
            Attribute newAttribute = attributes2.get(name);
            modifications.add(new Modification(Modification.ADD, newAttribute));
        }

        for (Attribute attribute1 : attributes1.getAll()) {
            Attribute attribute2 = attributes2.get(attribute1.getName());
            if (attribute2 == null) continue;

            Collection<Modification> mods = createModifications(
                    attribute1,
                    attribute2
            );

            if (mods.isEmpty()) continue;

            modifications.addAll(mods);
        }

        return modifications;
    }

    public Collection<Modification> createModifications(
            Attribute attribute1,
            Attribute attribute2
    ) {
        Collection<Modification> modifications = new ArrayList<Modification>();

        Collection<Object> oldValues = new ArrayList<Object>();
        oldValues.addAll(attribute1.getValues());
        oldValues.removeAll(attribute2.getValues());

        if (!oldValues.isEmpty()) {
            Attribute oldAttribute = new Attribute(attribute1.getName(), oldValues);
            modifications.add(new Modification(Modification.DELETE, oldAttribute));
        }

        Collection<Object> newValues = new ArrayList<Object>();
        newValues.addAll(attribute2.getValues());
        newValues.removeAll(attribute1.getValues());

        if (!newValues.isEmpty()) {
            Attribute newAttribute = new Attribute(attribute2.getName(), newValues);
            modifications.add(new Modification(Modification.ADD, newAttribute));
        }

        return modifications;
    }

    public void recordAddOperation(Session session, AddRequest request) throws Exception {

        log.debug("Recording add operation "+request.getDn());

        Attributes attrs = request.getAttributes();

        DN dn = new DN();

        Attributes attributes = new Attributes();
        attributes.setValue("targetDN", request.getDn().toString());
        attributes.setValue("changeType", "add");
        attributes.setValue("changes", attrs.toString());

        changelog.add(session, dn, attributes);
    }

    public void recordModifyOperation(Session session, ModifyRequest request) throws Exception {

        log.debug("Recording modify operation "+request.getDn());

        StringBuilder sb = new StringBuilder();

        for (Modification modification : request.getModifications()) {
            Attribute attr = modification.getAttribute();

            sb.append(LDAP.getModificationOperation(modification.getType()));
            sb.append(": ");
            sb.append(attr.getName());
            sb.append("\n");

            sb.append(attr);

            sb.append("-");
            sb.append("\n");
        }

        DN dn = new DN();

        Attributes attributes = new Attributes();
        attributes.setValue("targetDN", request.getDn().toString());
        attributes.setValue("changeType", "modify");
        attributes.setValue("changes", sb.toString());

        changelog.add(session, dn, attributes);
    }

    public void recordModRdnOperation(Session session, ModRdnRequest request) throws Exception {

        log.debug("Recording modrdn operation "+request.getDn());

        DN dn = new DN();

        Attributes attributes = new Attributes();
        attributes.setValue("targetDN", request.getDn().toString());
        attributes.setValue("changeType", "modrdn");
        attributes.setValue("newRDN", request.getNewRdn().toString());
        attributes.setValue("deleteOldRDN", request.getDeleteOldRdn());

        changelog.add(session, dn, attributes);
    }

    public void recordDeleteOperation(Session session, DeleteRequest request) throws Exception {

        log.debug("Recording delete operation "+request.getDn());

        DN dn = new DN();

        Attributes attributes = new Attributes();
        attributes.setValue("targetDN", request.getDn().toString());
        attributes.setValue("changeType", "delete");

        changelog.add(session, dn, attributes);
    }

    public void switchSources() throws Exception {

        Iterator i = targets.values().iterator();
        Iterator j = tmpTargets.values().iterator();

        while (i.hasNext() && j.hasNext()) {
            Source target = (Source)i.next();
            Source tmp = (Source)j.next();

            try {
                target.drop();
            } catch (Exception e) {
                log.debug("Failed to drop "+target.getName()+": "+e.getMessage());
            }

            try {
                tmp.rename(target);
            } catch (Exception e) {
                log.debug("Failed to rename "+tmp.getName()+": "+e.getMessage());
            }
        }
    }

    public Map<String, Source> getSources() {
        return sources;
    }

    public void setSources(Map<String, Source> sources) {
        this.sources = sources;
    }

    public Map<String, Source> getTargets() {
        return targets;
    }

    public void setTargets(Map<String, Source> targets) {
        this.targets = targets;
    }

    public Map<String, Collection<String>> getRelations() {
        return relations;
    }

    public void setRelations(Map<String, Collection<String>> relations) {
        this.relations = relations;
    }

    public Map<String, Entry> getEntries() {
        return entries;
    }

    public void setEntries(Map<String, Entry> entries) {
        this.entries = entries;
    }

    public String getBaseDn() {
        return baseDn;
    }

    public void setBaseDn(String baseDn) {
        this.baseDn = baseDn;
    }

    public int getScope() {
        return scope;
    }

    public void setScope(int scope) {
        this.scope = scope;
    }

    public Source getChangelog() {
        return changelog;
    }

    public void setChangelog(Source changelog) {
        this.changelog = changelog;
    }
}
