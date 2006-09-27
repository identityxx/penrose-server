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
import org.safehaus.penrose.acl.ACLEngine;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineManager;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPConnection;

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Handler {

    Logger log = LoggerFactory.getLogger(getClass());

    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";
    public final static String STOPPING = "STOPPING";

    private PenroseConfig penroseConfig;

    private Penrose penrose;

    private AddHandler addHandler;
    private BindHandler bindHandler;
    private CompareHandler compareHandler;
    private DeleteHandler deleteHandler;
    private ModifyHandler modifyHandler;
    private ModRdnHandler modRdnHandler;
    private FindHandler findHandler;
    private SearchHandler searchHandler;

    private SchemaManager schemaManager;
    private EngineManager engineManager;

    private SessionManager sessionManager;
    private PartitionManager partitionManager;

    private InterpreterManager interpreterManager;
    private ACLEngine aclEngine;
    private FilterTool filterTool;
    private EntryCache entryCache;

    private String status = STOPPED;

    public Handler(Penrose penrose) throws Exception {
        this.penrose = penrose;

        aclEngine = new ACLEngine(penrose);

        addHandler = createAddHandler();
        bindHandler = createBindHandler();
        compareHandler = createCompareHandler();
        deleteHandler = createDeleteHandler();
        modifyHandler = createModifyHandler();
        modRdnHandler = createModRdnHandler();
        findHandler = createFindHandler();
        searchHandler = createSearchHandler();

        PenroseConfig penroseConfig = penrose.getPenroseConfig();
        CacheConfig cacheConfig = penroseConfig.getEntryCacheConfig();
        String cacheClass = cacheConfig.getCacheClass() == null ? EntryCache.class.getName() : cacheConfig.getCacheClass();

        log.debug("Initializing entry cache "+cacheClass);
        Class clazz = Class.forName(cacheClass);
        entryCache = (EntryCache)clazz.newInstance();

        entryCache.setCacheConfig(cacheConfig);
        entryCache.setPenrose(penrose);

        entryCache.init();
    }

    public AddHandler createAddHandler() {
        return new AddHandler(this);
    }

    public BindHandler createBindHandler() {
        return new BindHandler(this);
    }

    public CompareHandler createCompareHandler() {
        return new CompareHandler(this);
    }

    public DeleteHandler createDeleteHandler() {
        return new DeleteHandler(this);
    }

    public ModifyHandler createModifyHandler() {
        return new ModifyHandler(this);
    }

    public ModRdnHandler createModRdnHandler() {
        return new ModRdnHandler(this);
    }

    public FindHandler createFindHandler() {
        return new FindHandler(this);
    }

    public SearchHandler createSearchHandler() {
        return new SearchHandler(this);
    }

    public void start() throws Exception {

        if (status != STOPPED) return;

        //log.debug("Starting SessionHandler...");

        try {
            status = STARTING;

            filterTool = new FilterTool();
            filterTool.setSchemaManager(schemaManager);

            status = STARTED;

            //log.debug("SessionHandler started.");

        } catch (Exception e) {
            status = STOPPED;
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void stop() throws Exception {

        if (status != STARTED) return;

        try {
            status = STOPPING;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        status = STOPPED;
    }

    public int bind(
            PenroseSession session,
            String dn,
            String password
    ) throws Exception {

        PenroseConfig penroseConfig = penrose.getPenroseConfig();
        String rootDn = schemaManager.normalize(penroseConfig.getRootDn());
        dn = schemaManager.normalize(dn);

        int rc;

        if (dn.equals(rootDn)) {
            if (!PasswordUtil.comparePassword(password, penroseConfig.getRootPassword())) {
                log.debug("Password doesn't match => BIND FAILED");
                rc = LDAPException.INVALID_CREDENTIALS;
            } else {
                rc = LDAPException.SUCCESS;
            }

        } else {

            Partition partition = partitionManager.findPartition(dn);

            if (partition == null) {
                log.debug("Entry "+dn+" not found");
                return LDAPException.NO_SUCH_OBJECT;
            }

            rc = getBindHandler().bind(session, partition, dn, password);
        }

        if (rc == LDAPException.SUCCESS) {
            session.setBindDn(dn);
            session.setBindPassword(password);
        }

        return rc;
    }

    public int unbind(PenroseSession session) throws Exception {

        if (session == null) return LDAPException.SUCCESS;

        session.setBindDn(null);
        session.setBindPassword(null);

        return LDAPException.SUCCESS;
    }

    public int add(
            PenroseSession session,
            String dn,
            Attributes attributes
    ) throws Exception {

        attributes = normalize(attributes);

        log.warn("Add entry \""+dn+"\".");
        log.debug("-------------------------------------------------");
        log.debug("ADD:");
        if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: "+session.getBindDn());
        log.debug(" - Entry: "+dn);
        log.debug("");

        String parentDn = EntryUtil.getParentDn(dn);

        Partition partition = partitionManager.findPartition(parentDn);

        if (partition == null) {
            log.debug("Parent entry "+parentDn+" not found");
            return LDAPException.NO_SUCH_OBJECT;
        }

        Collection path = findHandler.find(partition, parentDn);

        if (path == null || path.isEmpty()) {
            log.debug("Parent entry "+parentDn+" not found");
            return LDAPException.NO_SUCH_OBJECT;
        }

        Entry parent = (Entry)path.iterator().next();

        int rc = aclEngine.checkAdd(session, partition, parent.getEntryMapping(), parentDn);

        if (rc != LDAPException.SUCCESS) {
            log.debug("Not allowed to add "+dn);
            return rc;
        }

        return getAddHandler().add(session, partition, parent, dn, attributes);
    }

    public int compare(
            PenroseSession session,
            String dn,
            String attributeName,
            Object attributeValue
    ) throws Exception {

        log.warn("Compare attribute "+attributeName+" in \""+dn+"\" with \""+attributeValue+"\".");

        log.debug("-------------------------------------------------------------------------------");
        log.debug("COMPARE:");
        if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: " + session.getBindDn());
        log.debug(" - DN: " + dn);
        log.debug(" - Attribute Name: " + attributeName);
        if (attributeValue instanceof byte[]) {
            log.debug(" - Attribute Value: " + BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[])attributeValue));
        } else {
            log.debug(" - Attribute Value: " + attributeValue);
        }
        log.debug("-------------------------------------------------------------------------------");

        Partition partition = partitionManager.findPartition(dn);

        if (partition == null) {
            log.debug("Entry "+dn+" not found");
            return LDAPException.NO_SUCH_OBJECT;
        }

        Collection path = findHandler.find(partition, dn);

        if (path == null || path.isEmpty()) {
            log.debug("Entry "+dn+" not found");
            return LDAPException.NO_SUCH_OBJECT;
        }

        Entry entry = (Entry)path.iterator().next();

        int rc = aclEngine.checkRead(session, partition, entry.getEntryMapping(), dn);

        if (rc != LDAPException.SUCCESS) {
            log.debug("Not allowed to compare "+dn);
            return rc;
        }

        return getCompareHandler().compare(session, partition, entry, attributeName, attributeValue);
    }

    public int delete(
            PenroseSession session,
            String dn
    ) throws Exception {

        log.warn("Delete entry \""+dn+"\".");

        log.debug("-------------------------------------------------");
        log.debug("DELETE:");
        if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: "+session.getBindDn());
        log.debug(" - DN: "+dn);
        log.debug("");

        Partition partition = partitionManager.findPartition(dn);

        if (partition == null) {
            log.debug("Entry "+dn+" not found");
            return LDAPException.NO_SUCH_OBJECT;
        }

        Collection path = findHandler.find(partition, dn);

        if (path == null || path.isEmpty()) {
            log.debug("Entry "+dn+" not found");
            return LDAPException.NO_SUCH_OBJECT;
        }

        Entry entry = (Entry)path.iterator().next();

        int rc = aclEngine.checkDelete(session, partition, entry.getEntryMapping(), dn);

        if (rc != LDAPException.SUCCESS) {
            log.debug("Not allowed to delete "+dn);
            return rc;
        }

        return getDeleteHandler().delete(session, partition, entry);
    }

    public int modify(
            PenroseSession session,
            String dn,
            Collection modifications
    ) throws Exception {

        log.warn("Modify entry \""+dn+"\".");

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("MODIFY:", 80));
        if (session != null && session.getBindDn() != null) {
            log.debug(Formatter.displayLine(" - Bind DN: " + session.getBindDn(), 80));
        }
        log.debug(Formatter.displayLine(" - DN: " + dn, 80));

        log.debug(Formatter.displayLine(" - Attributes: ", 80));
        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            ModificationItem mi = (ModificationItem)i.next();
            Attribute attribute = mi.getAttribute();
            String op = "replace";
            switch (mi.getModificationOp()) {
                case DirContext.ADD_ATTRIBUTE:
                    op = "add";
                    break;
                case DirContext.REMOVE_ATTRIBUTE:
                    op = "delete";
                    break;
                case DirContext.REPLACE_ATTRIBUTE:
                    op = "replace";
                    break;
            }

            log.debug(Formatter.displayLine("   - "+op+": "+attribute.getID()+" => "+attribute.get(), 80));
        }

        log.debug(Formatter.displaySeparator(80));

        Partition partition = partitionManager.findPartition(dn);

        if (partition == null) {
            log.debug("Entry "+dn+" not found");
            return LDAPException.NO_SUCH_OBJECT;
        }

        Collection path = findHandler.find(partition, dn);

        if (path == null || path.isEmpty()) {
            log.debug("Entry "+dn+" not found");
            return LDAPException.NO_SUCH_OBJECT;
        }

        Entry entry = (Entry)path.iterator().next();

        int rc = aclEngine.checkModify(session, partition, entry.getEntryMapping(), dn);

        if (rc != LDAPException.SUCCESS) {
            log.debug("Not allowed to modify "+dn);
            return rc;
        }

        return getModifyHandler().modify(session, partition, entry, modifications);
    }

    public int modrdn(
            PenroseSession session,
            String dn,
            String newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        log.warn("ModRDN \""+dn+"\" to \""+newRdn+"\".");

        log.debug("-------------------------------------------------------------------------------");
        log.debug("MODRDN:");
        if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: " + session.getBindDn());
        log.debug(" - DN: " + dn);
        log.debug(" - New RDN: " + newRdn);

        Partition partition = partitionManager.findPartition(dn);

        if (partition == null) {
            log.debug("Entry "+dn+" not found");
            return LDAPException.NO_SUCH_OBJECT;
        }

        Collection path = findHandler.find(partition, dn);

        if (path == null || path.isEmpty()) {
            log.debug("Entry "+dn+" not found");
            return LDAPException.NO_SUCH_OBJECT;
        }

        Entry entry = (Entry)path.iterator().next();

        int rc = aclEngine.checkModify(session, partition, entry.getEntryMapping(), dn);

        if (rc != LDAPException.SUCCESS) {
            log.debug("Not allowed to rename "+dn);
            return rc;
        }

        return getModRdnHandler().modrdn(session, partition, entry, newRdn, deleteOldRdn);
    }

    /**
     * @param session
     * @param baseDn
     * @param filter
     * @param sc
     * @param results The results will be filled with objects of type SearchResult.
     * @return return code
     * @throws Exception
     */
    public int search(
            final PenroseSession session,
            final String baseDn,
            final String filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws Exception {

        String scope = LDAPUtil.getScope(sc.getScope());

        Collection attributeNames = sc.getAttributes();
        attributeNames = normalize(attributeNames);
        sc.setAttributes(attributeNames);

        log.warn("Search \""+baseDn +"\" with scope "+scope+" and filter \""+filter+"\"");

        log.debug("----------------------------------------------------------------------------------");
        log.debug("SEARCH:");
        if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: " + session.getBindDn());
        log.debug(" - Base DN: "+baseDn);
        log.debug(" - Scope: "+scope);
        log.debug(" - Filter: "+filter);
        log.debug(" - Attribute Names: "+attributeNames);
        log.debug("");

        final Partition partition = partitionManager.findPartition(baseDn);
        final PenroseSearchResults sr = new PenroseSearchResults();
        final Filter f = FilterTool.parseFilter(filter);

        sr.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    Entry entry = (Entry)event.getObject();
                    String dn = entry.getDn();
                    EntryMapping entryMapping = entry.getEntryMapping();

                    // check read permission
                    int rc = aclEngine.checkRead(session, partition, entryMapping, dn);
                    if (rc != LDAPException.SUCCESS) {
                        log.debug("Entry \""+entry.getDn()+"\" is not readable.");
                        return;
                    }

                    log.debug("Returning entry "+dn);
                    SearchResult searchResult = EntryUtil.toSearchResult(entry);
                    filterAttributes(session, partition, entryMapping, searchResult, sc);

                    results.add(searchResult);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

            public void pipelineClosed(PipelineEvent event) {
                results.setReturnCode(sr.getReturnCode());
                results.close();
            }
        });

        sr.addReferralListener(new ReferralAdapter() {
            public void referralAdded(ReferralEvent event) {
                Object referral = event.getReferral();
                //log.debug("Passing referral: "+referral);
                results.addReferral(referral);
            }
        });

        if ("".equals(baseDn) && sc.getScope() == LDAPConnection.SCOPE_BASE) {
            Entry rootDSE = createRootDSE();
            sr.add(rootDSE);
            sr.close();
            return LDAPException.SUCCESS;
        }

        if (partition == null) {
            log.debug("Entry "+baseDn+" not found");
            results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
            return LDAPException.NO_SUCH_OBJECT;
        }

        Collection path = findHandler.find(partition, baseDn);

        if (path == null || path.isEmpty()) {
            log.debug("Entry "+baseDn+" not found");
            results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
            return LDAPException.NO_SUCH_OBJECT;
        }

        Entry entry = (Entry)path.iterator().next();

        if (entry == null) {
            log.debug("Entry "+baseDn+" not found");
            results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
            return LDAPException.NO_SUCH_OBJECT;
        }

        int rc = aclEngine.checkSearch(session, partition, entry.getEntryMapping(), baseDn);

        if (rc != LDAPException.SUCCESS) {
            log.debug("Not allowed to search "+baseDn);
            return rc;
        }

        return getSearchHandler().search(session, partition, path, entry, f, sc, sr);
    }

    public Entry createRootDSE() throws Exception {

        Entry entry = new Entry("", null);

        AttributeValues attributeValues = entry.getAttributeValues();
        attributeValues.set("objectClass", "top");
        attributeValues.add("objectClass", "extensibleObject");
        attributeValues.set("vendorName", Penrose.VENDOR_NAME);
        attributeValues.set("vendorVersion", Penrose.PRODUCT_NAME+" "+Penrose.PRODUCT_VERSION);

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition p = (Partition)i.next();
            for (Iterator j=p.getRootEntryMappings().iterator(); j.hasNext(); ) {
                EntryMapping e = (EntryMapping)j.next();
                if ("".equals(e.getDn())) continue;
                attributeValues.add("namingContexts", e.getDn());
            }
        }

        return entry;
    }

    public void filterAttributes(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            SearchResult searchResult,
            PenroseSearchControls sc
    ) throws Exception {

        Attributes attributes = searchResult.getAttributes();

        Set grants = new HashSet();
        Set denies = new HashSet();

        Collection attributeNames = new ArrayList();

        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            attributeNames.add(attribute.getID());
        }

        String targetDn = schemaManager.normalize(searchResult.getName());
        aclEngine.getReadableAttributes(session, partition, entryMapping, targetDn, attributeNames, grants, denies);

        //log.debug("Readable attributes: "+grants);
        //log.debug("Unreadable attributes: "+denies);

        Collection list = new ArrayList();
        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            if (denies.contains(attribute.getID())) list.add(attribute);
        }

        Collection requestedAttributeNames = sc.getAttributes();
        if (requestedAttributeNames != null && !requestedAttributeNames.isEmpty() && !requestedAttributeNames.contains("*")) {
            for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
                Attribute attribute = (Attribute)i.next();
                if (!requestedAttributeNames.contains(attribute.getID())) list.add(attribute);
            }
        }

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();
            log.debug("Removing attribute "+attribute.getID());
            attributes.remove(attribute.getID());
        }
    }

    public BindHandler getBindHandler() {
        return bindHandler;
    }

    public void setBindHandler(BindHandler bindHandler) {
        this.bindHandler = bindHandler;
    }

    public SearchHandler getSearchHandler() {
        return searchHandler;
    }

    public void setSearchHandler(SearchHandler searchHandler) {
        this.searchHandler = searchHandler;
    }

    public AddHandler getAddHandler() {
        return addHandler;
    }

    public void setAddHandler(AddHandler addHandler) {
        this.addHandler = addHandler;
    }

    public ModifyHandler getModifyHandler() {
        return modifyHandler;
    }

    public void setModifyHandler(ModifyHandler modifyHandler) {
        this.modifyHandler = modifyHandler;
    }

    public DeleteHandler getDeleteHandler() {
        return deleteHandler;
    }

    public void setDeleteHandler(DeleteHandler deleteHandler) {
        this.deleteHandler = deleteHandler;
    }

    public CompareHandler getCompareHandler() {
        return compareHandler;
    }

    public void setCompareHandler(CompareHandler compareHandler) {
        this.compareHandler = compareHandler;
    }

    public ModRdnHandler getModRdnHandler() {
        return modRdnHandler;
    }

    public void setModRdnHandler(ModRdnHandler modRdnHandler) {
        this.modRdnHandler = modRdnHandler;
    }

    public InterpreterManager getInterpreterFactory() {
        return interpreterManager;
    }

    public void setInterpreterFactory(InterpreterManager interpreterManager) {
        this.interpreterManager = interpreterManager;
    }

    public Engine getEngine() {
        return engineManager.getEngine("DEFAULT");
    }

    public Engine getEngine(String name) {
        return engineManager.getEngine(name);
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    // ------------------------------------------------
    // Listeners
    // ------------------------------------------------

    public void addConnectionListener(ConnectionListener l) {
    }

    public void removeConnectionListener(ConnectionListener l) {
    }

    public void addBindListener(BindListener l) {
    }

    public void removeBindListener(BindListener l) {
    }

    public void addSearchListener(SearchListener l) {
    }

    public void removeSearchListener(SearchListener l) {
    }

    public void addCompareListener(CompareListener l) {
    }

    public void removeCompareListener(CompareListener l) {
    }

    public void addAddListener(AddListener l) {
    }

    public void removeAddListener(AddListener l) {
    }

    public void addDeleteListener(DeleteListener l) {
    }

    public void removeDeleteListener(DeleteListener l) {
    }

    public void addModifyListener(ModifyListener l) {
    }

    public void removeModifyListener(ModifyListener l) {
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
        aclEngine.setSchemaManager(schemaManager);
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public FindHandler getFindHandler() {
        return findHandler;
    }

    public void setFindHandler(FindHandler findHandler) {
        this.findHandler = findHandler;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }

    public EngineManager getEngineManager() {
        return engineManager;
    }

    public void setEngineManager(EngineManager engineManager) {
        this.engineManager = engineManager;
    }

    public Attributes normalize(Attributes attributes) throws Exception{

        BasicAttributes newAttributes = new BasicAttributes();

        for (NamingEnumeration e=attributes.getAll(); e.hasMore(); ) {
            Attribute attribute = (Attribute)e.next();
            String attributeName = schemaManager.getNormalizedAttributeName(attribute.getID());

            BasicAttribute newAttribute = new BasicAttribute(attributeName);
            for (NamingEnumeration e2=attribute.getAll(); e2.hasMore(); ) {
                Object value = e2.next();
                newAttribute.add(value);
            }

            newAttributes.put(newAttribute);
        }

        return newAttributes;
    }

    public Collection normalize(Collection attributeNames) {
        if (attributeNames == null) return null;

        Collection list = new ArrayList();
        for (Iterator i = attributeNames.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            list.add(schemaManager.getNormalizedAttributeName(name));
        }

        return list;
    }

    public FilterTool getFilterTool() {
        return filterTool;
    }

    public void setFilterTool(FilterTool filterTool) {
        this.filterTool = filterTool;
    }

    public EntryCache getEntryCache() {
        return entryCache;
    }

    public void setEntryCache(EntryCache entryCache) {
        this.entryCache = entryCache;
    }
}

