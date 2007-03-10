package org.safehaus.penrose.handler;

import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.EntryMapping;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPConnection;

import javax.naming.directory.*;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DefaultHandler extends Handler {

    private AddHandler addHandler;
    private BindHandler bindHandler;
    private CompareHandler compareHandler;
    private DeleteHandler deleteHandler;
    private ModifyHandler modifyHandler;
    private ModRdnHandler modRdnHandler;
    private FindHandler findHandler;
    private SearchHandler searchHandler;

    public DefaultHandler() throws Exception {
    }

    public void init(HandlerConfig handlerConfig) throws Exception {
        super.init(handlerConfig);
        
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

    public void bind(
            PenroseSession session,
            Partition partition,
            DN dn,
            String password
    ) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug("BIND:");
            log.debug(" - Bind DN: "+dn);
            log.debug(" - Bind Password: "+password);
            log.debug("");
        }

        getBindHandler().bind(session, partition, dn, password);
    }

    public void unbind(PenroseSession session) throws Exception {
    }

    public void add(
            PenroseSession session,
            Partition partition,
            DN dn,
            Attributes attributes
    ) throws Exception {

        if (log.isWarnEnabled()) {
            log.warn("Add entry \""+dn+"\".");
        }
        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug("ADD:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: "+session.getBindDn());
            log.debug(" - Entry: "+dn);
            log.debug("");
        }

        DN parentDn = dn.getParentDn();
        attributes = normalize(attributes);

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            EntryMapping parentMapping = partition.getParent(entryMapping);
            int rc = aclEngine.checkAdd(session, partition, parentMapping, parentDn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to add "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                getAddHandler().add(session, partition, entryMapping, dn, attributes);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    public boolean compare(
            PenroseSession session,
            Partition partition,
            DN dn,
            String attributeName,
            Object attributeValue
    ) throws Exception {

        if (log.isWarnEnabled()) {
            log.warn("Compare attribute "+attributeName+" in \""+dn+"\" with \""+attributeValue+"\".");
        }
        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug("COMPARE:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: " + session.getBindDn());
            log.debug(" - DN: " + dn);
            log.debug(" - Attribute Name: " + attributeName);
            if (attributeValue instanceof byte[]) {
                log.debug(" - Attribute Value: " + BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[])attributeValue));
            } else {
                log.debug(" - Attribute Value: " + attributeValue);
            }
        }

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            int rc = aclEngine.checkRead(session, partition, entryMapping, dn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to compare "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                return getCompareHandler().compare(session, partition, entryMapping, dn, attributeName, attributeValue);
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    public void delete(
            PenroseSession session,
            Partition partition,
            DN dn
    ) throws Exception {

        if (log.isWarnEnabled()) {
            log.warn("Delete entry \""+dn+"\".");
        }
        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug("DELETE:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: "+session.getBindDn());
            log.debug(" - DN: "+dn);
            log.debug("");
        }

        Entry entry = null; //findHandler.find(partition, dn);

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            int rc = aclEngine.checkDelete(session, partition, entryMapping, dn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to delete "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                getDeleteHandler().delete(session, partition, entryMapping, dn);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    public void modify(
            PenroseSession session,
            Partition partition,
            DN dn,
            Collection modifications
    ) throws Exception {

        dn = normalize(dn);

        if (log.isWarnEnabled()) {
            log.warn("Modify entry \""+dn+"\".");
        }
        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------------------------------------------------");
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

                if (attribute.size() == 0) {
                    log.debug(Formatter.displayLine("   - "+op+": "+attribute.getID()+" => "+null, 80));
                } else {
                    log.debug(Formatter.displayLine("   - "+op+": "+attribute.getID()+" => "+attribute.get(), 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        Entry entry = null; //findHandler.find(partition, dn);

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            int rc = aclEngine.checkModify(session, partition, entryMapping, dn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to modify "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                getModifyHandler().modify(session, partition, entry, entryMapping, dn, modifications);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    public void modrdn(
            PenroseSession session,
            Partition partition,
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        if (log.isWarnEnabled()) {
            log.warn("ModRDN \""+dn+"\" to \""+newRdn+"\".");
        }
        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODRDN:", 80));
            if (session != null && session.getBindDn() != null) {
                log.debug(Formatter.displayLine(" - Bind DN: " + session.getBindDn(), 80));
            }
            log.debug(Formatter.displayLine(" - DN: " + dn, 80));
            log.debug(Formatter.displayLine(" - New RDN: " + newRdn, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Entry entry = null; //findHandler.find(partition, dn);

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            int rc = aclEngine.checkModify(session, partition, entryMapping, dn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to modify "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                getModRdnHandler().modrdn(session, partition, entry, entryMapping, dn, newRdn, deleteOldRdn);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    /**
     * @param session
     * @param dn
     * @param filter
     * @param sc
     * @param results The results will be filled with objects of type SearchResult.
     * @return return code
     * @throws Exception
     */
    public void search(
            final PenroseSession session,
            final Partition partition,
            final DN dn,
            final String filter,
            final PenroseSearchControls sc,
            final Results results
    ) throws Exception {

        final DN baseDn = normalize(dn);

        String scope = LDAPUtil.getScope(sc.getScope());

        Collection attributeNames = sc.getAttributes();
        attributeNames = normalize(attributeNames);
        sc.setAttributes(attributeNames);

        if (log.isWarnEnabled()) {
            log.warn("Search \""+baseDn +"\" with scope "+scope+" and filter \""+filter+"\"");
        }

        final boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug("SEARCH:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: " + session.getBindDn());
            log.debug(" - Base DN: "+baseDn);
            log.debug(" - Scope: "+scope);
            log.debug(" - Filter: "+filter);
            log.debug(" - Attribute Names: "+attributeNames);
            log.debug("");
        }

        final Filter f = FilterTool.parseFilter(filter);

        final Set requestedAttributes = new HashSet();
        if (sc.getAttributes() != null) requestedAttributes.addAll(sc.getAttributes());

        final boolean allRegularAttributes = sc.getAttributes() == null || sc.getAttributes().isEmpty() || sc.getAttributes().contains("*");
        final boolean allOpAttributes = sc.getAttributes() != null && sc.getAttributes().contains("+");

        if (debug) log.debug("Requested: "+sc.getAttributes());

        if (baseDn.isEmpty()) {
            if (sc.getScope() == LDAPConnection.SCOPE_BASE) {
                Entry entry = createRootDSE();

                if (debug) {
                    log.debug("Before: "+entry.getDn());
                    entry.getAttributeValues().print();
                }

                Collection list = filterAttributes(session, partition, entry, requestedAttributes, allRegularAttributes, allOpAttributes);
                removeAttributes(entry, list);

                if (debug) {
                    log.debug("After: "+entry.getDn());
                    entry.getAttributeValues().print();
                }

                results.add(entry);
            }
            results.close();
            return;
        }

        Collection entryMappings = partition.findEntryMappings(baseDn);

        if (entryMappings.isEmpty()) {
            if (debug) log.debug("Base DN "+baseDn+" not found.");
            throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
        }

        final SearchPipeline sr = new SearchPipeline(
                results,
                session,
                partition,
                this,
                aclEngine,
                requestedAttributes,
                allRegularAttributes,
                allOpAttributes,
                entryMappings
        );

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            final EntryMapping entryMapping = (EntryMapping)i.next();

            int rc = aclEngine.checkSearch(session, partition, entryMapping, baseDn);

            if (rc != LDAPException.SUCCESS) {
                if (debug) log.debug("Not allowed to search "+baseDn);
                continue;
            }

            Runnable runnable = new Runnable() {
                public void run() {
                    try {

                        SearchHandler searchHandler = getSearchHandler();
                        searchHandler.search(
                                session,
                                partition,
                                entryMapping,
                                baseDn,
                                f,
                                sc,
                                sr
                        );

                        sr.setResult(entryMapping, ExceptionUtil.createLDAPException(LDAPException.SUCCESS));

                    } catch (Exception e) {
                        sr.setResult(entryMapping, ExceptionUtil.createLDAPException(e));

                    } finally {
                        try { sr.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
                    }
                }
            };

            threadManager.execute(runnable);
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

    public FindHandler getFindHandler() {
        return findHandler;
    }

    public void setFindHandler(FindHandler findHandler) {
        this.findHandler = findHandler;
    }

}
