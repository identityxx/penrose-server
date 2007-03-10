package org.safehaus.penrose.handler;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.interpreter.Interpreter;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPConnection;

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

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

    public void bind(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            String password
    ) throws Exception {

        String engineName = entryMapping.getEngineName();
        Engine engine = getEngine(engineName);

        if (engine == null) {
            log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        engine.bind(session, partition, entryMapping, dn, password);
    }

    public void unbind(PenroseSession session, Partition partition, EntryMapping entryMapping, DN bindDn) throws Exception {

        String engineName = entryMapping.getEngineName();
        Engine engine = getEngine(engineName);

        if (engine == null) {
            log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        //engine.unbind(session, partition, entryMapping, bindDn);
    }

    public void add(PenroseSession session, Partition partition, EntryMapping entryMapping, DN dn, Attributes attributes) throws Exception {
        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Adding entry "+dn);

        Attributes normalizedAttributes = new BasicAttributes();

        for (NamingEnumeration ne = attributes.getAll(); ne.hasMore(); ) {
            Attribute attribute = (Attribute)ne.next();

            String attributeName = attribute.getID();
            String normalizedAttributeName = attributeName;

            AttributeType at = schemaManager.getAttributeType(attributeName);
            if (debug) log.debug("Attribute "+attributeName+": "+at);
            if (at != null) {
                normalizedAttributeName = at.getName();
            }

            Attribute normalizedAttribute = new BasicAttribute(normalizedAttributeName);
            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                normalizedAttribute.add(value);
            }

            normalizedAttributes.put(normalizedAttribute);
        }

        if (debug) {
            log.debug("Original attributes:");
            for (NamingEnumeration ne = attributes.getAll(); ne.hasMore(); ) {
                Attribute attribute = (Attribute)ne.next();
                String attributeName = attribute.getID();

                for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                    Object value = j.next();
                    log.debug(" - "+attributeName+": "+value);
                }
            }

            log.debug("Normalized attributes:");
            for (NamingEnumeration ne = normalizedAttributes.getAll(); ne.hasMore(); ) {
                Attribute attribute = (Attribute)ne.next();
                String attributeName = attribute.getID();

                for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                    Object value = j.next();
                    log.debug(" - "+attributeName+": "+value);
                }
            }
        }

        attributes = normalizedAttributes;

        // -dlc- if the objectClass of the add Attributes does
        // not match any of the objectClasses of the entryMapping, there
        // is no sense trying to perform an add on this entryMapping
        Attribute at = attributes.get("objectClass");

        boolean childHasObjectClass = false;
        for (Iterator it2 = entryMapping.getObjectClasses().iterator();
            (!childHasObjectClass) && it2.hasNext();)
        {
            String cObjClass = (String) it2.next();
            for (int i = 0; i < at.size(); i++)
            {
                String objectClass = (String) at.get(i);
                if (childHasObjectClass = cObjClass.equalsIgnoreCase(objectClass))
                {
                    break;
                }
            }
        }
        if (!childHasObjectClass)
        {
            throw ExceptionUtil.createLDAPException(LDAPException.OBJECT_CLASS_VIOLATION);
        }

        String engineName = entryMapping.getEngineName();
        Engine engine = getEngine(engineName);

        if (engine == null) {
            log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        engine.add(session, partition, null, entryMapping, dn, attributes);
    }

    public boolean compare(PenroseSession session, Partition partition, EntryMapping entryMapping, DN dn, String attributeName, Object attributeValue) throws Exception {

        boolean debug = log.isDebugEnabled();
        Entry entry = findHandler.find(session, partition, entryMapping, dn);

        if (entry == null) {
            if (debug) log.debug("Entry "+dn+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
        }

        List attributeNames = new ArrayList();
        attributeNames.add(attributeName);

        AttributeValues attributeValues = entry.getAttributeValues();
        Collection values = attributeValues.get(attributeName);
        if (values == null) {
            if (debug) log.debug("Attribute "+attributeName+" not found.");
            return false;
        }

        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        String equality = attributeType == null ? null : attributeType.getEquality();
        EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

        if (debug) log.debug("Comparing values:");
        for (Iterator i=values.iterator(); i.hasNext(); ) {
            Object value = i.next();

            boolean b = equalityMatchingRule.compare(value, attributeValue);
            if (debug) log.debug(" - ["+value+"] => "+b);

            if (b) return true;

        }

        return false;
    }

    public void delete(PenroseSession session, Partition partition, EntryMapping entryMapping, DN dn) throws Exception {

        String engineName = entryMapping.getEngineName();
        Engine engine = getEngine(engineName);

        if (engine == null) {
            log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        engine.delete(session, partition, null, entryMapping, dn);
    }

    public void modify(PenroseSession session, Partition partition, Entry entry, EntryMapping entryMapping, DN dn, Collection modifications) throws Exception {
        log.debug("Modifying "+dn);

        Collection normalizedModifications = new ArrayList();

        for (Iterator i = modifications.iterator(); i.hasNext();) {
            ModificationItem modification = (ModificationItem) i.next();

            Attribute attribute = modification.getAttribute();
            String attributeName = attribute.getID();

            AttributeType at = schemaManager.getAttributeType(attributeName);
            if (at != null) {
                attributeName = at.getName();
            }

            switch (modification.getModificationOp()) {
                case DirContext.ADD_ATTRIBUTE:
                    log.debug("add: " + attributeName);
                    break;
                case DirContext.REMOVE_ATTRIBUTE:
                    log.debug("delete: " + attributeName);
                    break;
                case DirContext.REPLACE_ATTRIBUTE:
                    log.debug("replace: " + attributeName);
                    break;
            }

            Attribute normalizedAttribute = new BasicAttribute(attributeName);
            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                normalizedAttribute.add(value);
                log.debug(attributeName + ": "+value);
            }

            log.debug("-");

            ModificationItem normalizedModification = new ModificationItem(modification.getModificationOp(), normalizedAttribute);
            normalizedModifications.add(normalizedModification);
        }

        modifications = normalizedModifications;

        log.info("");

        String engineName = entryMapping.getEngineName();
        Engine engine = getEngine(engineName);

        if (engine == null) {
            log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        engine.modify(session, partition, entry, entryMapping, dn, modifications);
    }

    public void modrdn(PenroseSession session, Partition partition, Entry entry, EntryMapping entryMapping, DN dn, RDN newRdn, boolean deleteOldRdn) throws Exception {

        String engineName = entryMapping.getEngineName();
        Engine engine = getEngine(engineName);

        if (engine == null) {
            log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        engine.modrdn(session, partition, entry, entryMapping, dn, newRdn, deleteOldRdn);
    }

    public void search(
            final PenroseSession session,
            final Partition partition,
            final EntryMapping entryMapping,
            final DN baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final Results results
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug("Searching "+baseDn);
            log.debug("in mapping "+entryMapping.getDn());
        }

        AttributeValues sourceValues = new AttributeValues();

        Pipeline sr = new Pipeline(results) {

            // Check LDAP filter
            public void add(Object object) throws Exception {
                Entry child = (Entry)object;

                if (debug) log.debug("Checking filter "+filter+" on "+child.getDn());

                if (!filterTool.isValid(child, filter)) {
                    if (debug) log.debug("Entry \""+child.getDn()+"\" doesn't match search filter.");
                    return;
                }

                super.add(child);
            }
        };

        if (sc.getScope() == LDAPConnection.SCOPE_BASE || sc.getScope() == LDAPConnection.SCOPE_SUB) { // base or subtree
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
        }

        if (sc.getScope() == LDAPConnection.SCOPE_ONE || sc.getScope() == LDAPConnection.SCOPE_SUB) { // one level or subtree

            Collection children = partition.getChildren(entryMapping);

            for (Iterator i = children.iterator(); i.hasNext();) {
                EntryMapping childMapping = (EntryMapping) i.next();

                searchChildren(
                        session,
                        partition,
                        sourceValues,
                        entryMapping,
                        childMapping,
                        baseDn,
                        filter,
                        sc,
                        sr
                );
            }
        }
    }

    public void searchBase(
            final PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping entryMapping,
            final DN baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final Results results
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();

        String engineName = entryMapping.getEngineName();
        Engine engine = getEngine(engineName);

        if (engine == null) {
            if (debug) log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        Pipeline sr = new Pipeline(results);

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

    public void searchChildren(
            final PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final DN baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final Results results
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug("Search mapping \""+entryMapping.getDn()+"\":");
        }

        String engineName = entryMapping.getEngineName();
        Engine engine = getEngine(engineName);

        if (engine == null) {
            if (debug) log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        // use a new pipeline so that results is not closed
        Pipeline sr = new Pipeline(results);

        engine.expand(
                session,
                partition,
                sourceValues,
                baseMapping,
                entryMapping,
                baseDn,
                filter,
                sc,
                sr
        );

        if (sc.getScope() != LDAPConnection.SCOPE_SUB) return;

        Collection children = partition.getChildren(entryMapping);
        if (children.isEmpty()) return;

        Interpreter interpreter = interpreterManager.newInstance();
        AttributeValues attributeValues = engine.computeAttributeValues(entryMapping, sourceValues, interpreter);
        interpreter.clear();

        AttributeValues newSourceValues = new AttributeValues();
        newSourceValues.add("parent", sourceValues);
        newSourceValues.add("parent", attributeValues);
        //AttributeValues newSourceValues = handler.pushSourceValues(sourceValues, attributeValues);

        if (debug) {
            log.debug("New parent source values:");
            newSourceValues.print();
        }

        for (Iterator i = children.iterator(); i.hasNext();) {
            EntryMapping childMapping = (EntryMapping) i.next();

            searchChildren(
                    session,
                    partition,
                    newSourceValues,
                    baseMapping,
                    childMapping,
                    baseDn,
                    filter,
                    sc,
                    results
            );
        }
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
