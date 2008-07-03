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

    protected Map<String,SourceRef> localSourceRefs = new LinkedHashMap<String,SourceRef>();
    //protected Map<String,SourceRef> localPrimarySourceRefs = new LinkedHashMap<String,SourceRef>();

    protected List<SourceRef> sourceRefs              = new ArrayList<SourceRef>();
    protected Map<String,SourceRef> sourceRefsByName  = new LinkedHashMap<String,SourceRef>();
    //protected Map<String,SourceRef> primarySourceRefs = new LinkedHashMap<String,SourceRef>();

    protected Directory directory;
    protected Partition partition;

    protected SchemaManager schemaManager;
    protected FilterEvaluator filterEvaluator;

    public void init(EntryConfig entryConfig, EntryContext entryContext) throws Exception {
        this.entryConfig = entryConfig;
        this.entryContext = entryContext;

        directory = entryContext.getDirectory();
        partition = directory.getPartition();
        schemaManager = partition.getSchemaManager();

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();
        filterEvaluator = penroseContext.getFilterEvaluator();

        // create source references
        
        //String primarySourceName = getPrimarySourceName();

        for (SourceMapping sourceMapping : entryConfig.getSourceMappings()) {

            SourceRef sourceRef = createSourceRef(sourceMapping);
            String alias = sourceRef.getAlias();

            sourceRefs.add(sourceRef);
            localSourceRefs.put(alias, sourceRef);
            sourceRefsByName.put(alias, sourceRef);
/*
            if (alias.equals(primarySourceName)) {
                localPrimarySourceRefs.put(alias, sourceRef);
                primarySourceRefs.put(alias, sourceRef);
            }
*/
        }

        // inherit source referencess from the parent entries

        Entry parent = directory.getEntry(entryConfig.getParentId());

        while (parent != null) {

            //String psn = parent.getPrimarySourceName();

            for (SourceRef sourceRef : parent.getLocalSourceRefs()) {
                String alias = sourceRef.getAlias();

                sourceRefs.add(sourceRef);
                sourceRefsByName.put(alias, sourceRef);
/*
                if (alias.equals(psn)) {
                    primarySourceRefs.put(alias, sourceRef);
                }
*/
            }

            parent = parent.getParent();
        }

        init();
    }

    public void init() throws Exception {
    }

    public void destroy() throws Exception {
    }

    public SourceRef createSourceRef(SourceMapping sourceMapping) throws Exception {

        log.debug("Initializing source reference "+sourceMapping.getName()+".");

        Partition partition = getPartition();

        SourceManager sourceManager = partition.getSourceManager();
        Source source = sourceManager.getSource(sourceMapping.getSourceName());
        if (source == null) throw new Exception("Unknown source "+sourceMapping.getSourceName()+".");

        return new SourceRef(this, source, sourceMapping);
    }

    public String getId() {
        return entryConfig.getId();
    }

    public String getParentId() {
        return entryConfig.getParentId();
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

    public Collection<SourceRef> getLocalSourceRefs() {
        return localSourceRefs.values();
    }
/*
    public Collection<SourceRef> getLocalPrimarySourceRefs() {
        return localPrimarySourceRefs.values();
    }
*/
    public Collection<SourceRef> getSourceRefs() {
        return sourceRefs;
    }

    public int getSourceRefsCount() {
        return sourceRefs.size();
    }

    public SourceRef getSourceRef(String name) {
        return sourceRefsByName.get(name);
    }

    public SourceRef getSourceRef(int index) {
        return sourceRefs.get(index);
    }

    public SourceRef getSourceRef() {
        return sourceRefs.get(0);
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
        return directory.getChildren(this);
    }

    public void addChild(Entry child) throws Exception {
        directory.addChild(this, child);
    }

    public void addChildren(Collection<Entry> children) throws Exception {
        for (Entry child : children) {
            addChild(child);
        }
    }

    public void removeChild(Entry child) throws Exception {
        directory.removeChild(this, child);
    }

    public void removeChildren() {
        directory.removeChildren(this);
    }

    public Entry getParent() {
        return directory.getParent(this);
    }

    public void setParent(Entry parent) {
        directory.setParent(this, parent);
    }

    public String getPrimarySourceName() {

        for (AttributeMapping rdnAttributeMapping : getRdnAttributeMappings()) {

            String variable = rdnAttributeMapping.getVariable();
            if (variable == null) continue;

            int i = variable.indexOf('.');
            if (i < 0) continue;

            return variable.substring(0, i);
        }

        Collection<SourceMapping> sourceMappings = getSourceMappings();
        if (!sourceMappings.isEmpty()) {
            SourceMapping sourceMapping = sourceMappings.iterator().next();
            return sourceMapping.getName();
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

        //return entryConfig.getPrimarySourceName();
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

    public Collection<AttributeMapping> getAttributeMappings() {
        return entryConfig.getAttributeMappings();
    }

    public AttributeMapping getAttributeMapping(String attributeName) {
        return entryConfig.getAttributeMapping(attributeName);
    }

    public Collection<AttributeMapping> getRdnAttributeMappings() {
        return entryConfig.getRdnAttributeMappings();
    }

    public Collection<ACI> getACL() {
        return entryConfig.getACL();
    }

    public Collection<SourceMapping> getSourceMappings() {
        return entryConfig.getSourceMappings();
    }

    public SourceMapping getSourceMapping(int index) {
        return entryConfig.getSourceMapping(index);
    }
    
    public SourceMapping getSourceMapping(String alias) {
        return entryConfig.getSourceMapping(alias);
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

        if (!filterEvaluator.eval(this, filter)) {
            if (debug) log.debug("Entry \""+getDn()+"\" doesn't match search filter.");
            throw LDAP.createException(LDAP.UNWILLING_TO_PERFORM);
        }
    }

    public void validateSearchResult(SearchRequest request, SearchResult result) throws Exception {

        Filter filter = request.getFilter();
        Attributes attributes = result.getAttributes();

        if (debug) log.debug("Checking search filter "+filter+".");

        if (!filterEvaluator.eval(attributes, filter)) {
            if (debug) log.debug("Entry \""+result.getDn()+"\" doesn't match search filter.");
            throw LDAP.createException(LDAP.UNWILLING_TO_PERFORM);
        }
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

        validatePermission(session, request);
        validateSchema(request);

        throw LDAP.createException(LDAP.UNWILLING_TO_PERFORM);
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

        final DN baseDn     = request.getDn();
        final Filter filter = request.getFilter();
        final int scope     = request.getScope();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("SEARCH", 80));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 80));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 80));
            log.debug(TextUtil.displayLine("Filter : "+filter, 80));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        response = createSearchResponse(session, request, response);

        try {
            validateScope(request);
            validatePermission(session, request);
            validateFilter(filter);

        } catch (Exception e) {
            response.close();
            return;
        }

        try {
            generateSearchResults(session, request, response);

        } finally {
            response.close();
        }
    }

    public SearchResponse createSearchResponse(
            final Session session,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {
        return new EntrySearchResponse(session, request, response, this);
    }

    public void generateSearchResults(
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

    public DN computeDn(
            Interpreter interpreter
    ) throws Exception {

        DNBuilder db = new DNBuilder();

        RDN rdn = computeRdn(interpreter);
/*
        if (rdn.isEmpty()) {
            log.error("RDN is empty: "+rdn);
            throw LDAP.createException(LDAP.OPERATIONS_ERROR);
        }
*/
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

        for (AttributeMapping attributeMapping : entryConfig.getRdnAttributeMappings()) {
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

        for (AttributeMapping attributeMapping : entryConfig.getAttributeMappings()) {

            Object value = interpreter.eval(attributeMapping);
            if (debug) log.debug("Attribute "+attributeMapping.getName()+": "+value);
            if (value == null) continue;

            if (value instanceof Collection) {
                attributes.addValues(attributeMapping.getName(), (Collection<Object>) value);
            } else {
                attributes.addValue(attributeMapping.getName(), value);
            }
        }

        for (String objectClass : entryConfig.getObjectClasses()) {
            if (debug) log.debug("Object class: "+objectClass);
            attributes.addValue("objectClass", objectClass);
        }

        return attributes;
    }

    public SourceValues extractSourceValues(DN dn) throws Exception {

        if (debug) log.debug("Extracting dn "+dn+":");

        Interpreter interpreter = partition.newInterpreter();
        SourceValues sourceValues = new SourceValues();

        extractSourceValues(
                dn,
                this,
                interpreter,
                sourceValues
        );

        return sourceValues;
    }

    public void extractSourceValues(
            DN dn,
            Entry entry,
            Interpreter interpreter,
            SourceValues sourceValues
    ) throws Exception {

        DN parentDn = dn.getParentDn();
        Entry parent = entry.getParent();

        if (parentDn != null && parent != null) {
            extractSourceValues(parentDn, parent, interpreter, sourceValues);
        }

        RDN rdn = dn.getRdn();
        interpreter.set(rdn);

        entry.computeSources(interpreter, sourceValues);

        interpreter.clear();
    }

    public SourceValues extractSourceValues(
            DN dn,
            Attributes attributes
    ) throws Exception {

        if (debug) log.debug("Extracting entry "+dn+":");

        Interpreter interpreter = partition.newInterpreter();
        SourceValues sourceValues = new SourceValues();

        extractSourceValues(
                dn,
                attributes,
                interpreter,
                sourceValues
        );

        return sourceValues;
    }

    public void extractSourceValues(
            DN dn,
            Attributes attributes,
            Interpreter interpreter,
            SourceValues sourceValues
    ) throws Exception {

        DN parentDn = dn.getParentDn();

        Entry parent = getParent();
        if (parentDn != null && parent != null) {
            extractSourceValues(parentDn, parent, interpreter, sourceValues);
        }

        RDN rdn = dn.getRdn();
        interpreter.set(rdn);
        interpreter.set("rdn", rdn);
        interpreter.set(attributes);

        computeSources(interpreter, sourceValues);

        interpreter.clear();
    }

    public void computeSources(
            Interpreter interpreter,
            SourceValues sourceValues
    ) throws Exception {

        for (SourceRef sourceRef : localSourceRefs.values()) {

            if (debug) log.debug("Extracting source "+sourceRef.getAlias()+":");

            Attributes attributes = sourceValues.get(sourceRef.getAlias());

            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {

                Object value = interpreter.eval(fieldRef);
                if (value == null) continue;

                if ("INTEGER".equals(fieldRef.getType()) && value instanceof String) {
                    value = Integer.parseInt((String)value);
                }

                attributes.addValue(fieldRef.getName(), value);

                String fieldName = sourceRef.getAlias() + "." + fieldRef.getName();
                if (debug) log.debug(" - " + fieldName + ": " + value);
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

            entry.localSourceRefs = new LinkedHashMap<String,SourceRef>();
            //entry.localPrimarySourceRefs = new LinkedHashMap<String,SourceRef>();

            entry.sourceRefs        = new ArrayList<SourceRef>();
            entry.sourceRefsByName  = new LinkedHashMap<String,SourceRef>();
            //entry.primarySourceRefs = new LinkedHashMap<String,SourceRef>();

            for (SourceRef origSourceRef : sourceRefs) {
                SourceRef sourceRef = (SourceRef)origSourceRef.clone();

                String alias = sourceRef.getAlias();

                entry.sourceRefs.add(sourceRef);
                entry.sourceRefsByName.put(alias, sourceRef);
/*
                if (primarySourceRefs.containsKey(alias)) {
                    entry.primarySourceRefs.put(alias, sourceRef);
                }
*/
                if (localSourceRefs.containsKey(alias)) {
                    entry.localSourceRefs.put(alias, sourceRef);
                }
/*
                if (localPrimarySourceRefs.containsKey(alias)) {
                    entry.localPrimarySourceRefs.put(alias, sourceRef);
                }
*/
            }

            entry.partition = partition;

            return entry;

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}