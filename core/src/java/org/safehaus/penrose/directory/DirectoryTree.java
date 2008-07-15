package org.safehaus.penrose.directory;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class DirectoryTree {

    public final static Collection<RDN> EMPTY_CHILDREN = new ArrayList<RDN>();

    public Set<DN> suffixes = new TreeSet<DN>();

    public Map<DN,Attributes> entries = new LinkedHashMap<DN,Attributes>();
    public Map<DN,Collection<RDN>> children = new LinkedHashMap<DN,Collection<RDN>>();

    public void addSuffix(DN dn) {
        suffixes.add(dn);
    }

    public void removeSuffix(DN dn) {
        suffixes.remove(dn);
    }

    public Collection<DN> getSuffixes() {
        return suffixes;
    }

    public boolean checkSuffix(DN dn) throws Exception {
        for (DN suffix : suffixes) {
            if (dn.endsWith(suffix)) return true;
        }

        return false;
    }

    public boolean isRoot(DN dn) throws Exception {
        for (DN suffix : suffixes) {
            if (dn.equals(suffix)) return true;
        }

        return false;
    }

    public void add(AddRequest request, AddResponse response) throws Exception {

        DN dn = request.getDn();
        Attributes attributes = request.getAttributes();

        if (!checkSuffix(dn)) {
            response.setReturnCode(LDAP.NO_SUCH_OBJECT);
            return;
        }

        if (isRoot(dn)) {
            if (hasEntry(dn)) {
                response.setReturnCode(LDAP.ENTRY_ALREADY_EXISTS);
                return;
            }

            addEntry(dn, attributes);
            return;
        }

        DN parentDn = dn.getParentDn();
        RDN rdn = dn.getRdn();

        if (!hasEntry(parentDn)) {
            response.setReturnCode(LDAP.NO_SUCH_OBJECT);
            return;
        }

        if (hasEntry(dn)) {
            response.setReturnCode(LDAP.ENTRY_ALREADY_EXISTS);
            return;
        }

        if (hasChild(parentDn, rdn)) {
            throw LDAP.createException(LDAP.ENTRY_ALREADY_EXISTS);
        }

        addEntry(dn, attributes);
        addChild(parentDn, rdn);
    }

    public void compare(CompareRequest request, CompareResponse response) throws Exception {

        DN dn = request.getDn();

        Attributes attributes = getEntry(dn);
        if (attributes == null) {
            response.setReturnCode(LDAP.NO_SUCH_OBJECT);
            return;
        }

        Attribute attribute = attributes.get(request.getAttributeName());
        if (attribute == null) {
            response.setReturnCode(LDAP.NO_SUCH_ATTRIBUTE);
            return;
        }

        Object value = request.getAttributeValue();
        for (Object v : attribute.getValues()) {
            if (value.equals(v)) {
                response.setReturnCode(LDAP.COMPARE_TRUE);
                return;
            }
        }

        response.setReturnCode(LDAP.COMPARE_FALSE);
    }

    public void delete(DeleteRequest request, DeleteResponse response) throws Exception {

        DN dn = request.getDn();

        if (hasChildren(dn)) {
            response.setReturnCode(LDAP.NOT_ALLOWED_ON_NONLEAF);
            return;
        }

        if (hasEntry(dn)) {
            response.setReturnCode(LDAP.NO_SUCH_OBJECT);
            return;
        }

        children.remove(dn);
        entries.remove(dn);
    }

    public void modify(ModifyRequest request, ModifyResponse response) throws Exception {

        DN dn = request.getDn();

        Attributes attributes = getEntry(dn);
        if (attributes == null) {
            response.setReturnCode(LDAP.NO_SUCH_OBJECT);
            return;
        }

        for (Modification modification : request.getModifications()) {

            Attribute attribute = modification.getAttribute();

            switch (modification.getType()) {
                case Modification.ADD:
                    attributes.add(attribute);
                    break;

                case Modification.DELETE:
                    attributes.remove(attribute);
                    break;

                case Modification.REPLACE:
                    attributes.set(attribute);
                    break;
            }
        }
    }

    public void modrdn(ModRdnRequest request, ModRdnResponse response) throws Exception {
    }

    public void search(SearchRequest request, SearchResponse response) throws Exception {

        DN dn = request.getDn();

        if (!hasEntry(dn)) {
            response.setReturnCode(LDAP.NO_SUCH_OBJECT);
            return;
        }

        int scope = request.getScope();

        if (scope == SearchRequest.SCOPE_BASE) {

            createSearchResult(request, response, dn);
            response.close();

            return;
        }

        if (scope == SearchRequest.SCOPE_ONE) {
            for (RDN rdn : getChildren(dn)) {
                DN childDn = rdn.append(dn);
                createSearchResult(request, response, childDn);
            }

            response.close();

            return;
        }

        search(request, response, dn);
    }

    void search(SearchRequest request, SearchResponse response, DN dn) throws Exception {

        createSearchResult(request, response, dn);

        for (RDN rdn : getChildren(dn)) {
            DN childDn = rdn.append(dn);
            
            search(request, response, childDn);
        }
    }

    void createSearchResult(SearchRequest request, SearchResponse response, DN dn) throws Exception {

        Attributes attributes = getEntry(dn);

        SearchResult result = new SearchResult();
        result.setDn(dn);
        result.setAttributes(attributes);

        Filter filter = request.getFilter();
        if (filter == null || filter.eval(attributes)) {
            response.add(result);
        }
    }

    void addEntry(DN dn, Attributes attributes) throws Exception {
        entries.put(dn, attributes);
    }

    Attributes getEntry(DN dn) throws Exception {
        return entries.get(dn);
    }

    boolean hasEntry(DN dn) throws Exception {
        return getEntry(dn) != null;
    }

    void addChild(DN parentDn, RDN rdn) throws Exception {
        Collection<RDN> list = getChildren(parentDn);
        if (list == null) {
            list = new LinkedHashSet<RDN>();
            children.put(parentDn, list);
        }

        list.add(rdn);
    }

    Collection<RDN> getChildren(DN dn) throws Exception {
        Collection<RDN> list = children.get(dn);
        if (list == null) return EMPTY_CHILDREN;
        return list;
    }

    boolean hasChildren(DN dn) throws Exception {
        Collection<RDN> list = getChildren(dn);
        return !list.isEmpty();
    }

    boolean hasChild(DN dn, RDN rdn) throws Exception {
        Collection<RDN> list = getChildren(dn);
        return list.contains(rdn);
    }
}
