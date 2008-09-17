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
package org.safehaus.penrose.directory;

import org.safehaus.penrose.acl.ACI;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterEvaluator;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.mapping.Mapping;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.util.TextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Entry implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();
    public boolean info = log.isInfoEnabled();
    public boolean warn = log.isWarnEnabled();

    public final static Collection<Entry> EMPTY_ENTRIES = new ArrayList<Entry>();

    protected EntryConfig entryConfig;
    protected EntryContext entryContext;

    protected Entry parent;
    protected Collection<Entry> children = new LinkedHashSet<Entry>();

    protected Map<String, EntrySource> localSources = new LinkedHashMap<String, EntrySource>();
    //protected Map<String,SourceRef> localPrimarySourceRefs = new LinkedHashMap<String,SourceRef>();

    protected List<EntrySource> sources = new ArrayList<EntrySource>();
    protected Map<String, EntrySource> sourcesByAlias = new LinkedHashMap<String, EntrySource>();
    //protected Map<String,SourceRef> primarySourceRefs = new LinkedHashMap<String,SourceRef>();

    protected Directory directory;
    protected Partition partition;

    protected SchemaManager schemaManager;
    protected FilterEvaluator filterEvaluator;
    protected EntryFilterEvaluator entryFilterEvaluator;

    public void init(EntryConfig entryConfig, EntryContext entryContext) throws Exception {
        this.entryConfig = entryConfig;
        this.entryContext = entryContext;

        parent = entryContext.getParent();
        if (parent != null) parent.addChild(this);

        directory = entryContext.getDirectory();
        partition = directory.getPartition();
        schemaManager = partition.getSchemaManager();

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();
        filterEvaluator = penroseContext.getFilterEvaluator();

        entryFilterEvaluator = new EntryFilterEvaluator(this);

        // create source references
        
        //String primarySourceName = getPrimarySourceAlias();

        for (EntrySourceConfig sourceMapping : entryConfig.getSourceConfigs()) {

            EntrySource entrySource = createSource(sourceMapping);
            String alias = entrySource.getAlias();

            addSource(entrySource);
            localSources.put(alias, entrySource);
/*
            if (alias.equals(primarySourceName)) {
                localPrimarySourceRefs.put(alias, entrySource);
                primarySourceRefs.put(alias, entrySource);
            }
*/
        }

        // inherit source referencess from the parent entries

        Entry p = parent;

        while (p != null) {

            //String psn = p.getPrimarySourceAlias();

            for (EntrySource entrySource : p.getLocalSources()) {
                //String alias = entrySource.getAlias();

                addSource(entrySource);
/*
                if (alias.equals(psn)) {
                    primarySourceRefs.put(alias, entrySource);
                }
*/
            }

            p = p.getParent();
        }

        init();
    }

    public void init() throws Exception {

        String initScript = entryConfig.getInitScript();

        if (initScript != null) {
            Interpreter interpreter = partition.newInterpreter();
            interpreter.set("entry", this);
            interpreter.eval(initScript);
        }
    }

    public void destroy() throws Exception {

        String destroyScript = entryConfig.getDestroyScript();

        if (destroyScript != null) {
            Interpreter interpreter = partition.newInterpreter();
            interpreter.set("entry", this);
            interpreter.eval(destroyScript);
        }
    }

    public EntrySource createSource(EntrySourceConfig sourceConfig) throws Exception {

        String partitionName = sourceConfig.getPartitionName();
        String sourceName = sourceConfig.getSourceName();

        log.debug("Initializing source reference "+sourceConfig.getAlias()+": "+partitionName+"."+sourceName);

        Partition sourcePartition;

        if (partitionName == null) {
            sourcePartition = partition;

        } else {
            sourcePartition = partition.getPartitionContext().getPartition(partitionName);
            if (sourcePartition == null) throw new Exception("Unknown partition "+partitionName+".");
        }

        SourceManager sourceManager = sourcePartition.getSourceManager();
        Source source = sourceManager.getSource(sourceName);
        if (source == null) throw new Exception("Unknown source "+sourceName+".");

        return new EntrySource(this, sourceConfig, source);
    }

    public String getId() {
        return entryConfig.getId();
    }

    public String getParentId() {

        return parent == null ? null : parent.getId();
    }

    public DN getDn() {
        return entryConfig.getDn();
    }

    public DN getParentDn() throws Exception {
        return entryConfig.getParentDn();
    }

    public RDN getRdn() throws Exception {
        return entryConfig.getRdn();
    }

    public EntryConfig getEntryConfig() {
        return entryConfig;
    }

    public Directory getDirectory() {
        return directory;
    }

    public Partition getPartition() {
        return partition;
    }

    public Mapping getMapping() {
        String mappingName = entryConfig.getMappingName();
        if (mappingName == null) return null;
        
        return partition.getMappingManager().getMapping(mappingName);
    }

    public Collection<String> getLocalSourceNames() {
        return localSources.keySet();
    }

    public Collection<EntrySource> getLocalSources() {
        return localSources.values();
    }
/*
    public Collection<SourceRef> getLocalPrimarySourceRefs() {
        return localPrimarySourceRefs.values();
    }
*/
    public Collection<EntrySource> getSources() {
        return sources;
    }

    public int getSourceCount() {
        return sources.size();
    }

    public EntrySource getSource(String name) {
        return sourcesByAlias.get(name);
    }

    public EntrySource getSource(int index) {
        return sources.get(index);
    }

    public EntrySource getSource() {
        return sources.get(0);
    }
/*
    public Collection<SourceRef> getPrimarySourceRefs() {
        return primarySourceRefs.values();
    }

    public void setPrimarySourceRefs(Map<String, SourceRef> primarySourceRefs) {
        this.primarySourceRefs = primarySourceRefs;
    }
*/
    public Collection<Entry> getChildren() {
        return children;
    }

    public void addChild(Entry child) throws Exception {
        children.add(child);
    }

    public void addChildren(Collection<Entry> children) throws Exception {
        for (Entry child : children) {
            addChild(child);
        }
    }

    public void removeChild(Entry child) throws Exception {
        children.remove(child);
    }

    public void removeChildren() {
        children.clear();
    }

    public Entry getParent() {
        return parent;
    }

    public void setParent(Entry parent) {
        this.parent = parent;
    }

    public String getPrimarySourceAlias() {

        for (EntryAttributeConfig rdnAttributeMapping : getRdnAttributeConfigs()) {

            String variable = rdnAttributeMapping.getVariable();
            if (variable == null) continue;

            int i = variable.indexOf('.');
            if (i < 0) continue;

            return variable.substring(0, i);
        }

        Collection<EntrySourceConfig> sourceMappings = getSourceMappings();
        if (!sourceMappings.isEmpty()) {
            EntrySourceConfig sourceMapping = sourceMappings.iterator().next();
            return sourceMapping.getAlias();
        }
        
        return null;
/*
        Entry entry = this;
        String psn = null;

        do {
            Collection<SourceMapping> sourceMappings = entry.getSourceMappings();
            if (sourceMappings.size() > 0) {
                SourceMapping sourceMapping = sourceMappings.iterator().next();
                psn = sourceMapping.getName();
            }

            entry = directory.getEntry(entry.getParentId());
            //entry = directory.getParent(entry);

        } while (entry != null);

        //return entryConfig.getPrimarySourceAlias();
        return psn;
*/
    }

    public Collection<String> getObjectClasses() {
        return entryConfig.getObjectClasses();
    }

    public String getParameter(String name) {
        return entryConfig.getParameter(name);
    }

    public Collection<String> getParameterNames() {
        return entryConfig.getParameterNames();
    }

    public List<Entry> getPath() {
        List<Entry> path = new ArrayList<Entry>();

        Entry entry = this;
        do {
            path.add(0, entry);
            entry = entry.getParent();
        } while (entry != null);

        return path;
    }

    public List<Entry> getRelativePath(DN baseDn) {

        List<Entry> path = new ArrayList<Entry>();

        Entry entry = this;
        do {
            path.add(0, entry);
            // if (entry == base) break;
            if (baseDn.getSize() == entry.getDn().getSize()) break;
            entry = entry.getParent();
        } while (entry != null);

        return path;
    }

    public boolean containsObjectClass(String objectClass) {
        return entryConfig.containsObjectClass(objectClass);
    }

    public Collection<EntryAttributeConfig> getAttributeConfigs() {
        return entryConfig.getAttributeConfigs();
    }

    public EntryAttributeConfig getAttributeConfig(String attributeName) {
        return entryConfig.getAttributeConfig(attributeName);
    }

    public Collection<EntryAttributeConfig> getRdnAttributeConfigs() {
        return entryConfig.getRdnAttributeConfigs();
    }

    public Collection<ACI> getACL() {
        return entryConfig.getACL();
    }

    public Collection<EntrySourceConfig> getSourceMappings() {
        return entryConfig.getSourceConfigs();
    }

    public EntrySourceConfig getSourceMapping(int index) {
        return entryConfig.getSourceConfig(index);
    }
    
    public EntrySourceConfig getSourceMapping(String alias) {
        return entryConfig.getSourceConfig(alias);
    }

    public Session createAdminSession() throws Exception {
        SessionManager sessionManager = getPartition().getPartitionContext().getSessionManager();
        return sessionManager.createAdminSession();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ACL
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validatePermission(Session session, AddRequest request) throws Exception {
        partition.validatePermission(session, request, this);
    }

    public void validatePermission(Session session, CompareRequest request) throws Exception {
        partition.validatePermission(session, request, this);
    }

    public void validatePermission(Session session, DeleteRequest request) throws Exception {
        partition.validatePermission(session, request, this);
    }

    public void validatePermission(Session session, ModifyRequest request) throws Exception {
        partition.validatePermission(session, request, this);
    }

    public void validatePermission(Session session, ModRdnRequest request) throws Exception {
        partition.validatePermission(session, request, this);
    }

    public void validatePermission(Session session, SearchRequest request) throws Exception {
        partition.validatePermission(session, request, this);
    }

    public void validatePermission(Session session, SearchResult result) throws Exception {
        partition.validatePermission(session, result);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Schema
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validateSchema(AddRequest request) throws Exception {
        partition.validateSchema(request, this);
    }

    public void validateSchema(ModifyRequest request) throws Exception {
        partition.validateSchema(request, this);
    }

    public void validateSchema(ModRdnRequest request) throws Exception {
        partition.validateSchema(request, this);
    }

    public AttributeType getAttributeType(String attributeName) throws Exception {
        return schemaManager.getAttributeType(attributeName);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Scope
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validateScope(SearchRequest request) throws Exception {

        DN dn = request.getDn();
        int scope = request.getScope();

        if (debug) log.debug("Checking search scope "+LDAP.getScope(scope)+".");

        if (scope == SearchRequest.SCOPE_ONE && !getParentDn().matches(dn)) {
            log.debug("Entry \""+getDn()+"\" is out of scope.");
            throw LDAP.createException(LDAP.UNWILLING_TO_PERFORM);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Filter
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validateFilter(Filter filter) throws Exception {

        if (debug) log.debug("Checking search filter "+filter+".");

        if (!entryFilterEvaluator.eval(filter)) {
            if (debug) log.debug("Entry \""+getDn()+"\" doesn't match search filter.");
            throw LDAP.createException(LDAP.UNWILLING_TO_PERFORM);
        }
    }

    public boolean validateSearchResult(SearchRequest request, SearchResult result) throws Exception {

        Filter filter = request.getFilter();
        Attributes attributes = result.getAttributes();

        if (debug) log.debug("Checking search filter "+filter+".");

        boolean b = filterEvaluator.eval(attributes, filter);

        if (debug) {
            if (!b) log.debug("Entry \""+result.getDn()+"\" doesn't match search filter.");
        }

        return b;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("ADD", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        String addScript = entryConfig.getAddScript();

        if (addScript != null) {
            Interpreter interpreter = partition.newInterpreter();
            interpreter.set("entry", this);
            interpreter.set("session", session);
            interpreter.set("request", request);
            interpreter.set("response", response);
            interpreter.eval(addScript);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("BIND", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        byte[] password = request.getPassword();

        SearchResult searchResult = find(dn);

        Attributes attributes = searchResult.getAttributes();
        Attribute attribute = attributes.get("userPassword");

        if (attribute == null) {
            log.debug("Attribute userPassword not found");
            throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
        }

        Collection<Object> userPasswords = attribute.getValues();
        for (Object userPassword : userPasswords) {
            if (debug) log.debug("userPassword: " + userPassword);
            if (PasswordUtil.comparePassword(password, userPassword)) return;
        }

        throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("COMPARE", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);

        String attributeName = request.getAttributeName();
        Object attributeValue = request.getAttributeValue();

        SearchResult searchResult = find(session, dn);

        Attributes attributes = searchResult.getAttributes();
        Attribute attribute = attributes.get(attributeName);
        if (attribute == null) {
            if (debug) log.debug("Attribute "+attributeName+" not found.");

            response.setReturnCode(LDAP.COMPARE_FALSE);
            return;
        }

        Collection<Object> values = attribute.getValues();
        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        String equality = attributeType == null ? null : attributeType.getEquality();
        EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

        if (debug) log.debug("Comparing values:");
        for (Object value : values) {
            boolean b = equalityMatchingRule.compare(value, attributeValue);
            if (debug) log.debug(" - [" + value + "] => " + b);

            if (b) {
                response.setReturnCode(LDAP.COMPARE_TRUE);
                return;
            }
        }

        response.setReturnCode(LDAP.COMPARE_FALSE);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("DELETE", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);

        throw LDAP.createException(LDAP.UNWILLING_TO_PERFORM);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean contains(DN dn) throws Exception {

        DN entryDn = getDn();

        int entryDnSize = entryDn.getSize();
        int dnSize      = dn.getSize();

        if (dnSize < entryDnSize) {
            return false;
        }

        if (entryDnSize == dnSize) {
            return dn.matches(entryDn);
        }

        if (!dn.endsWith(entryDn)) {
            return false;
        }

        for (Entry child : getChildren()) {
            boolean b = child.contains(dn);
            if (b) return true;
        }

        return false;
    }

    public Collection<Entry> findEntries(DN dn) throws Exception {

        if (dn == null) return EMPTY_ENTRIES;

        DN entryDn        = getDn();
        if (debug) log.debug("Finding matching entries for \""+dn+"\" in \""+entryDn+"\".");

        int entryDnLength = entryDn.getSize();
        int dnLength      = dn.getSize();

        if (dnLength == 0 && entryDnLength == 0) { // Root DSE
            Collection<Entry> results = new ArrayList<Entry>();
            results.add(this);
            return results;
        }

        if (!dn.endsWith(entryDn)) {
            if (debug) log.debug("Doesn't match "+entryDn);
            return EMPTY_ENTRIES;
        }

        if (debug) log.debug("Searching children of \""+entryDn+"\".");

        Collection<Entry> results = new ArrayList<Entry>();

        if (dnLength > entryDnLength) { // children has priority
            for (Entry child : getChildren()) {
                Collection<Entry> list = child.findEntries(dn);
                results.addAll(list);
            }
            return results;
        }

        results.add(this);
        
        if (debug) log.debug("Found entry \""+entryDn+"\".");

        return results;
    }
/*
    public Collection<Entry> findEntries(DN dn, int level) throws Exception {

        if (debug) log.debug("Finding matching entries for "+dn+":");

        if (dn == null) return EMPTY_ENTRIES;

        DN entryDn        = getDn();

        int entryDnLength = entryDn.getSize();
        int dnLength      = dn.getSize();

        RDN entryRdn      = getRdn();
        RDN rdn           = dn.get(dnLength - entryDnLength - 1);

        if (!entryRdn.matches(rdn)) {
            if (debug) log.debug("Doesn't match with "+entryDn);
            return EMPTY_ENTRIES;
        }

        Collection<Entry> results = new ArrayList<Entry>();

        if (dnLength > entryDnLength) { // children has priority
            for (Entry child : getChildren()) {
                Collection<Entry> list = child.findEntries(dn, entryDnLength);
                results.addAll(list);
            }
            return results;
        }

        results.add(this);
        
        if (debug) log.debug("Found entry "+entryDn);

        return results;
    }
*/
    public SearchResult find(DN dn) throws Exception {

        Session session = createAdminSession();

        try {
            return find(session, dn);

        } finally {
            session.close();
        }
    }

    public SearchResult find(
            Session session,
            DN dn
    ) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse response = new SearchResponse();

        search(
                session,
                request,
                response
        );

        response.close();

        if (response.getReturnCode() != LDAP.SUCCESS) {
            if (debug) log.debug("Entry "+dn+" not found: "+response.getErrorMessage());
            throw LDAP.createException(response.getReturnCode());
        }

        if (!response.hasNext()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        return response.next();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("MODIFY", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);
        validateSchema(request);

        throw LDAP.createException(LDAP.UNWILLING_TO_PERFORM);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("MODRDN", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);
        validateSchema(request);

        throw LDAP.createException(LDAP.UNWILLING_TO_PERFORM);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        DN baseDn     = request.getDn();
        Filter filter = request.getFilter();
        int scope     = request.getScope();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("SEARCH", 80));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 80));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 80));
            log.debug(TextUtil.displayLine("Filter : "+filter, 80));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        try {
            validateSearchRequest(session, request, response);

        } catch (Exception e) {
            response.close();
            return;
        }

        response = createSearchResponse(session, request, response);

        try {
            Interpreter interpreter = partition.newInterpreter();
            interpreter.set("session", session);
            interpreter.set("request", request);
            interpreter.set("response", response);
            interpreter.set("entry", this);

            for (EntrySearchConfig searchConfig : entryConfig.getSearchConfigs()) {
                Filter searchFilter = searchConfig.getFilter();
                if (debug) log.debug("Checking "+searchFilter+" with "+filter+".");

                if (!searchFilter.matches(filter)) continue;

                if (debug) log.debug("Executing search script with filter "+searchFilter+".");
                interpreter.eval(searchConfig.getScript());

                return;
            }

            expand(session, request, response);

        } finally {
            response.close();
        }
    }

    public void validateSearchRequest(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        validateScope(request);
        validatePermission(session, request);

        Filter filter = request.getFilter();
        validateFilter(filter);
    }

    public SearchResponse createSearchResponse(
            final Session session,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {
        return new EntrySearchResponse(session, request, response, this);
    }

    public void expand(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        Interpreter interpreter = partition.newInterpreter();

        DN dn = computeDn(interpreter);
        Attributes attributes = computeAttributes(interpreter);

        SearchResult result = new SearchResult(dn, attributes);
        result.setEntryId(getId());

        response.add(result);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        DN dn = session.getBindDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("UNBIND", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }
    }

    public SearchResult createSearchResult(SourceAttributes sourceAttributes) throws Exception {
        
        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceAttributes);

        DN dn = computeDn(interpreter);
        Attributes attributes = computeAttributes(interpreter);

        SearchResult result = new SearchResult(dn, attributes);
        result.setEntryId(entryConfig.getId());
        result.setSourceAttributes(sourceAttributes);

        return result;
    }

    public DN createDn(SourceAttributes sourceAttributes) throws Exception {

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceAttributes);

        return computeDn(interpreter);
    }

    public DN computeDn(
            Interpreter interpreter
    ) throws Exception {

        DNBuilder db = new DNBuilder();

        RDN rdn = computeRdn(interpreter);
        db.set(rdn);

        Entry parent = getParent();
        if (parent == null) {
            db.append(entryConfig.getParentDn());

        } else {
            db.append(parent.computeDn(interpreter));
        }

        DN dn = db.toDn();
        if (debug) log.debug("DN: "+dn);

        return dn;
    }

    public RDN computeRdn(
            Interpreter interpreter
    ) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        for (EntryAttributeConfig attributeMapping : entryConfig.getRdnAttributeConfigs()) {
            String name = attributeMapping.getName();

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            rb.set(name, value);
        }

        return rb.toRdn();
    }

    public Attributes computeAttributes(
            Interpreter interpreter
    ) throws Exception {

        Attributes attributes = new Attributes();

        Mapping mapping = getMapping();
        if (mapping != null) {
            mapping.map(interpreter, attributes);
            return attributes;
        }

        for (EntryAttributeConfig attributeConfig : getAttributeConfigs()) {

            String name = attributeConfig.getName();
            if ("dn".equals(name)) continue;

            Object value = interpreter.eval(attributeConfig);
            //if (debug) log.debug("Attribute "+name+": "+value);
            if (value == null || value.toString().trim().equals("")) continue;

            if (value instanceof Collection) {
                attributes.addValues(name, (Collection<Object>) value);
            } else {
                attributes.addValue(name, value);
            }
        }

        for (String objectClass : entryConfig.getObjectClasses()) {
            //if (debug) log.debug("Object class: "+objectClass);
            attributes.addValue("objectClass", objectClass);
        }

        return attributes;
    }

    public void extractSourceAttributes(DN dn, Interpreter interpreter, SourceAttributes output) throws Exception {

        Entry parent = getParent();
        if (getDn().getSize() > dn.getSize()) {
            if (parent == null) return;
            parent.extractSourceAttributes(dn, interpreter, output);
            return;
        }

        DN parentDn = dn.getParentDn();
        if (parentDn != null && parent != null) {
            parent.extractSourceAttributes(parentDn, interpreter, output);
        }

        RDN rdn = dn.getRdn();
        interpreter.set(rdn);

        if (debug) log.debug(" - "+rdn+" => "+getDn());

        computeSourceAttributes(interpreter, output);

        interpreter.clear();
    }

    public SourceAttributes extractSourceValues(
            DN dn,
            Attributes attributes
    ) throws Exception {

        if (debug) log.debug("Extracting entry "+dn+":");

        Interpreter interpreter = partition.newInterpreter();
        SourceAttributes sourceValues = new SourceAttributes();

        extractSourceAttributes(
                dn,
                attributes,
                interpreter,
                sourceValues
        );

        return sourceValues;
    }

    public void extractSourceAttributes(
            DN dn,
            Attributes attributes,
            Interpreter interpreter,
            SourceAttributes sourceAttributes
    ) throws Exception {

        DN parentDn = dn.getParentDn();

        Entry parent = getParent();
        if (parentDn != null && parent != null) {
            parent.extractSourceAttributes(parentDn, interpreter, sourceAttributes);
        }

        RDN rdn = dn.getRdn();
        interpreter.set(rdn);
        interpreter.set("rdn", rdn);
        interpreter.set(attributes);

        computeSourceAttributes(interpreter, sourceAttributes);

        interpreter.clear();
    }

    public void computeSourceAttributes(
            Interpreter interpreter,
            SourceAttributes sourceAttributes
    ) throws Exception {

        for (EntrySource entrySource : getLocalSources()) {

            if (debug) log.debug("Extracting source "+entrySource.getAlias()+":");

            Attributes attributes = sourceAttributes.get(entrySource.getAlias());

            String mappingName = entrySource.getMappingName();
            if (mappingName != null) {
                Mapping mapping = partition.getMappingManager().getMapping(mappingName);
                mapping.map(interpreter, attributes);
                
            } else {
                for (EntryField field : entrySource.getFields()) {

                    Object value = interpreter.eval(field);
                    if (value == null) continue;

                    if (FieldConfig.TYPE_INTEGER.equals(field.getType()) && value instanceof String) {
                        value = Integer.parseInt((String)value);
                    }

                    attributes.addValue(field.getName(), value);

                    String fieldName = entrySource.getAlias() + "." + field.getName();
                    if (debug) log.debug(" - " + fieldName + ": " + value);
                }
            }
        }
    }

    public int hashCode() {
        return entryConfig.hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        Entry entry = (Entry)object;
        if (!equals(entryConfig, entry.entryConfig)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {

        Entry entry = (Entry)super.clone();

        try {
            entry.entryConfig = (EntryConfig) entryConfig.clone();

            entry.localSources = new LinkedHashMap<String, EntrySource>();
            //entry.localPrimarySourceRefs = new LinkedHashMap<String,SourceRef>();

            entry.sources = new ArrayList<EntrySource>();
            entry.sourcesByAlias = new LinkedHashMap<String, EntrySource>();
            //entry.primarySourceRefs = new LinkedHashMap<String,SourceRef>();

            for (EntrySource origEntrySource : sources) {
                EntrySource entrySource = (EntrySource)origEntrySource.clone();

                String alias = entrySource.getAlias();

                entry.addSource(entrySource);
/*
                if (primarySourceRefs.containsKey(alias)) {
                    entry.primarySourceRefs.put(alias, entrySource);
                }
*/
                if (localSources.containsKey(alias)) {
                    entry.localSources.put(alias, entrySource);
                }
/*
                if (localPrimarySourceRefs.containsKey(alias)) {
                    entry.localPrimarySourceRefs.put(alias, entrySource);
                }
*/
            }

            entry.partition = partition;

            return entry;

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void addSource(EntrySource entrySource) {
        sources.add(entrySource);
        sourcesByAlias.put(entrySource.getAlias(), entrySource);
    }
}