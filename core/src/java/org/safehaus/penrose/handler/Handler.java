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
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.cache.EntryCacheManager;
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
    private EntryCacheManager entryCacheManager;

    private InterpreterManager interpreterManager;
    private ACLEngine aclEngine;
    private FilterTool filterTool;

    ThreadManager threadManager;

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

    public void bind(
            PenroseSession session,
            String dn,
            String password
    ) throws LDAPException {

        try {
            PenroseConfig penroseConfig = penrose.getPenroseConfig();
            String rootDn = schemaManager.normalize(penroseConfig.getRootDn());
            dn = schemaManager.normalize(dn);

            int rc;

            if (dn.equals(rootDn)) {
                if (!PasswordUtil.comparePassword(password, penroseConfig.getRootPassword())) {
                    log.debug("Password doesn't match => BIND FAILED");
                    rc = LDAPException.INVALID_CREDENTIALS;
                    throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
                }

            } else {

                Partition partition = partitionManager.findPartition(dn);

                if (partition == null) {
                    log.debug("Entry "+dn+" not found");
                    rc = LDAPException.NO_SUCH_OBJECT;
                    throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
                }

                getBindHandler().bind(session, partition, dn, password);
                session.setBindDn(dn);
                session.setBindPassword(password);
            }

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
        }
    }

    public void unbind(PenroseSession session) throws LDAPException {

        if (session == null) return;

        session.setBindDn(null);
        session.setBindPassword(null);
    }

    public void add(
            PenroseSession session,
            String dn,
            Attributes attributes
    ) throws LDAPException {

        try {
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
                int rc = LDAPException.NO_SUCH_OBJECT;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            Entry parent = findHandler.find(partition, parentDn);

            if (parent == null) {
                log.debug("Parent entry "+dn+" not found");
                int rc = LDAPException.NO_SUCH_OBJECT;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            int rc = aclEngine.checkAdd(session, partition, parent.getEntryMapping(), parentDn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to add "+dn);
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            getAddHandler().add(session, partition, parent, dn, attributes);

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
        }
    }

    public boolean compare(
            PenroseSession session,
            String dn,
            String attributeName,
            Object attributeValue
    ) throws LDAPException {

        try {
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
                int rc = LDAPException.NO_SUCH_OBJECT;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            Entry entry = findHandler.find(partition, dn);

            if (entry == null) {
                log.debug("Entry "+dn+" not found");
                int rc = LDAPException.NO_SUCH_OBJECT;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            int rc = aclEngine.checkRead(session, partition, entry.getEntryMapping(), dn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to compare "+dn);
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            return getCompareHandler().compare(session, partition, entry, attributeName, attributeValue);

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
        }
    }

    public void delete(
            PenroseSession session,
            String dn
    ) throws LDAPException {

        try {
            log.warn("Delete entry \""+dn+"\".");

            log.debug("-------------------------------------------------");
            log.debug("DELETE:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: "+session.getBindDn());
            log.debug(" - DN: "+dn);
            log.debug("");

            Partition partition = partitionManager.findPartition(dn);

            if (partition == null) {
                int rc = LDAPException.NO_SUCH_OBJECT;
                String message = "Entry "+dn+" not found";
                log.debug(message);
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
            }
/*
            List path = new ArrayList();
            AttributeValues sourceValues = new AttributeValues();

            findHandler.find(partition, dn, path, sourceValues);

            if (path.isEmpty()) {
                log.debug("Entry "+dn+" not found");
                return LDAPException.NO_SUCH_OBJECT;
            }

            Entry entry = (Entry)path.iterator().next();
*/
            Entry entry = findHandler.find(partition, dn);

            if (entry == null) {
                int rc = LDAPException.NO_SUCH_OBJECT;
                String message = "Entry "+dn+" not found";
                log.debug(message);
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
            }

            int rc = aclEngine.checkDelete(session, partition, entry.getEntryMapping(), dn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to delete "+dn);
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            getDeleteHandler().delete(session, partition, entry);

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
        }
    }

    public void modify(
            PenroseSession session,
            String dn,
            Collection modifications
    ) throws LDAPException {

        try {
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
                int rc = LDAPException.NO_SUCH_OBJECT;
                String message = "Entry "+dn+" not found";
                log.debug(message);
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
            }
/*
            List path = new ArrayList();
            AttributeValues sourceValues = new AttributeValues();

            findHandler.find(partition, dn, path, sourceValues);

            if (path.isEmpty()) {
                log.debug("Entry "+dn+" not found");
                return LDAPException.NO_SUCH_OBJECT;
            }

            Entry entry = (Entry)path.iterator().next();
*/
            Entry entry = findHandler.find(partition, dn);

            if (entry == null) {
                int rc = LDAPException.NO_SUCH_OBJECT;
                String message = "Entry "+dn+" not found";
                log.debug(message);
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
            }

            int rc = aclEngine.checkModify(session, partition, entry.getEntryMapping(), dn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to modify "+dn);
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            getModifyHandler().modify(session, partition, entry, modifications);

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
        }
    }

    public void modrdn(
            PenroseSession session,
            String dn,
            String newRdn,
            boolean deleteOldRdn
    ) throws LDAPException {

        try {
            log.warn("ModRDN \""+dn+"\" to \""+newRdn+"\".");

            log.debug("-------------------------------------------------------------------------------");
            log.debug("MODRDN:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: " + session.getBindDn());
            log.debug(" - DN: " + dn);
            log.debug(" - New RDN: " + newRdn);

            Partition partition = partitionManager.findPartition(dn);

            if (partition == null) {
                int rc = LDAPException.NO_SUCH_OBJECT;
                String message = "Entry "+dn+" not found";
                log.debug(message);
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
            }
/*
            List path = new ArrayList();
            AttributeValues sourceValues = new AttributeValues();

            findHandler.find(partition, dn, path, sourceValues);

            if (path.isEmpty()) {
                log.debug("Entry "+dn+" not found");
                return LDAPException.NO_SUCH_OBJECT;
            }

            Entry entry = (Entry)path.iterator().next();
*/
            Entry entry = findHandler.find(partition, dn);

            if (entry == null) {
                int rc = LDAPException.NO_SUCH_OBJECT;
                String message = "Entry "+dn+" not found";
                log.debug(message);
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
            }

            int rc = aclEngine.checkModify(session, partition, entry.getEntryMapping(), dn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to rename "+dn);
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            getModRdnHandler().modrdn(session, partition, entry, newRdn, deleteOldRdn);

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
        }
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

        if ("".equals(baseDn) && sc.getScope() == LDAPConnection.SCOPE_BASE) {
            Entry rootDSE = createRootDSE();
            SearchResult searchResult = EntryUtil.toSearchResult(rootDSE);
            filterAttributes(session, searchResult, sc);
            results.add(searchResult);
            results.close();
            return LDAPException.SUCCESS;
        }

        final Partition partition = partitionManager.findPartition(baseDn);

        if (partition == null) {
            log.debug("Entry "+baseDn+" not found");
            results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
            return LDAPException.NO_SUCH_OBJECT;
        }

        threadManager.execute(new Runnable() {
            public void run() {

                int rc = LDAPException.SUCCESS;
                try {
                    rc = searchInBackground(
                            session,
                            partition,
                            baseDn,
                            filter,
                            sc,
                            results
                    );
                    results.setReturnCode(rc);

                } catch (LDAPException e) {
                    rc = e.getResultCode();
                    results.setReturnCode(rc);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    rc = ExceptionUtil.getReturnCode(e);
                    results.setReturnCode(rc);

                } finally {
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

    public int searchInBackground(
            final PenroseSession session,
            final Partition partition,
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

        final PenroseSearchResults sr = new PenroseSearchResults();
        final Filter f = FilterTool.parseFilter(filter);

        sr.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    Entry entry = (Entry)event.getObject();
                    String dn = entry.getDn();
                    EntryMapping entryMapping = entry.getEntryMapping();

                    // check read permission
                    log.debug("Checking read permission on "+dn);
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
            }
        });

        sr.addReferralListener(new ReferralAdapter() {
            public void referralAdded(ReferralEvent event) {
                Object referral = event.getReferral();
                //log.debug("Passing referral: "+referral);
                results.addReferral(referral);
            }
        });

/*
        List path = new ArrayList();
        findHandler.find(partition, baseDn, path, sourceValues);

        if (path.isEmpty()) {
            log.debug("Entry "+baseDn+" not found");
            results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
            return LDAPException.NO_SUCH_OBJECT;
        }

        final Entry entry = (Entry)path.iterator().next();

        if (entry == null) {
            log.debug("Entry "+baseDn+" not found");
            results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
            return LDAPException.NO_SUCH_OBJECT;
        }
*/
        Collection entryMappings = partition.findEntryMappings(baseDn);

        int rc = LDAPException.SUCCESS;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            final EntryMapping entryMapping = (EntryMapping)i.next();

            rc = aclEngine.checkSearch(session, partition, entryMapping, baseDn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to search "+baseDn);
                continue;
            }

            AttributeValues sourceValues = new AttributeValues();

            rc = getSearchHandler().search(
                    session,
                    partition,
                    sourceValues,
                    entryMapping,
                    baseDn,
                    f,
                    sc,
                    sr
            );
        }

        return rc;
    }

    public Entry createRootDSE() throws Exception {

        Entry entry = new Entry("", null);

        AttributeValues attributeValues = entry.getAttributeValues();
        attributeValues.set("objectClass", "top");
        attributeValues.add("objectClass", "extensibleObject");
        attributeValues.set("vendorName", Penrose.PRODUCT_VENDOR);
        attributeValues.set("vendorVersion", Penrose.PRODUCT_NAME+" Server "+Penrose.PRODUCT_VERSION);

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

        Collection requestedAttributeNames = new HashSet();

        boolean allRegularAttributes = false;
        boolean opAttributes = false;

        if (sc.getAttributes() != null && !sc.getAttributes().isEmpty()) {

            for (Iterator i=sc.getAttributes().iterator(); i.hasNext(); ) {
                String attributeName = (String)i.next();
                requestedAttributeNames.add(attributeName.toLowerCase());
            }

            allRegularAttributes = requestedAttributeNames.contains("*");
            opAttributes = requestedAttributeNames.contains("+");
        }

        log.debug("Requested: "+requestedAttributeNames);

        Collection attributeNames = new ArrayList();
        Attributes attributes = searchResult.getAttributes();

        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            attributeNames.add(attribute.getID().toLowerCase());
        }

        log.debug("Returned: "+attributeNames);

        Set grants = new HashSet();
        Set denies = new HashSet();
        denies.addAll(attributeNames);

        String targetDn = schemaManager.normalize(searchResult.getName());
        aclEngine.getReadableAttributes(session, partition, entryMapping, targetDn, attributeNames, grants, denies);

        log.debug("Granted: "+grants);
        log.debug("Denied: "+denies);

        Collection opAtNames = new HashSet();
        for (Iterator i=entryMapping.getOperationalAttributeNames().iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            opAtNames.add(attributeName.toLowerCase());
        }
        
        Collection list = new HashSet();

        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String attributeName = attribute.getID();
            String normalizedName = attributeName.toLowerCase();

            if (denies.contains(normalizedName)) {
                log.debug("Remove denied attribute "+normalizedName);
                list.add(attributeName);
                continue;
            }

            if (partition.isProxy(entryMapping)) {
                log.debug("Keep proxied attribute "+normalizedName);
                continue;
            }

            if (requestedAttributeNames.isEmpty()) {
                if (opAtNames.contains(normalizedName)) {
                    log.debug("Remove operational attribute "+normalizedName);
                    list.add(attributeName);
                } else {
                    log.debug("Keep regular attribute "+normalizedName);
                }
                continue;
            }

            if (opAttributes && opAtNames.contains(normalizedName)) {
                log.debug("Keep operational attribute "+normalizedName);
                continue;
            }

            if (allRegularAttributes && !opAtNames.contains(normalizedName)) {
                log.debug("Keep all regular attribute "+normalizedName);
                continue;
            }

            if (requestedAttributeNames.contains(normalizedName)) {
                log.debug("Keep requested attribute "+normalizedName);
                continue;
            }

            log.debug("Remove unrequested attribute "+normalizedName);
            list.add(attributeName);
        }

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            attributes.remove(attributeName);
        }
    }

    public void filterAttributes(
            PenroseSession session,
            SearchResult searchResult,
            PenroseSearchControls sc
    ) throws Exception {

        Collection requestedAttributeNames = new HashSet();

        boolean allRegularAttributes = false;
        boolean opAttributes = false;

        if (sc.getAttributes() != null && !sc.getAttributes().isEmpty()) {

            for (Iterator i=sc.getAttributes().iterator(); i.hasNext(); ) {
                String attributeName = (String)i.next();
                requestedAttributeNames.add(attributeName.toLowerCase());
            }

            allRegularAttributes = requestedAttributeNames.contains("*");
            opAttributes = requestedAttributeNames.contains("+");
        }

        if (allRegularAttributes || opAttributes) return;

        log.debug("Requested: "+requestedAttributeNames);

        Collection attributeNames = new ArrayList();
        Attributes attributes = searchResult.getAttributes();

        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            attributeNames.add(attribute.getID().toLowerCase());
        }

        log.debug("Returned: "+attributeNames);

        Collection list = new HashSet();

        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String attributeName = attribute.getID();
            String normalizedName = attributeName.toLowerCase();

            if (requestedAttributeNames.contains(normalizedName)) {
                log.debug("Keep requested attribute "+normalizedName);
                continue;
            }
        }

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            attributes.remove(attributeName);
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

    public ThreadManager getThreadManager() {
        return threadManager;
    }

    public void setThreadManager(ThreadManager threadManager) {
        this.threadManager = threadManager;
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

    public EntryCacheManager getEntryCacheManager() {
        return entryCacheManager;
    }

    public void setEntryCacheManager(EntryCacheManager entryCacheManager) {
        this.entryCacheManager = entryCacheManager;
    }

    public AttributeValues pushSourceValues(
            AttributeValues oldSourceValues,
            AttributeValues newSourceValues
    ) {
        AttributeValues av = new AttributeValues();

        for (Iterator i=oldSourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = oldSourceValues.get(name);

            if (name.startsWith("parent.")) name = "parent."+name;
            av.add(name, values);
        }

        if (newSourceValues != null) {
            for (Iterator i=newSourceValues.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Collection values = newSourceValues.get(name);

                av.add("parent."+name, values);
            }
        }

        return av;
    }

}

