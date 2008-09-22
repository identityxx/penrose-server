package org.safehaus.penrose.federation;

import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.DN;

import java.util.Collection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.io.Serializable;

/**
 * @author Endi Sukma Dewata
 */
public class LinkingData implements Serializable {

    public final static Collection<Object> EMPTY = new ArrayList<Object>();

    public final static int LOCAL_STORAGE  = 0;
    public final static int GLOBAL_STORAGE = 1;

    private SearchResult entry;

    private int storage;
    private String localAttribute;
    private String globalAttribute;

    private boolean searched;
    private String status;

    public Map<DN,SearchResult> linkedEntries = new LinkedHashMap<DN,SearchResult>();
    public Map<DN,SearchResult> matchedEntries = new LinkedHashMap<DN,SearchResult>();

    public LinkingData(SearchResult entry) {
        this.entry = entry;
    }

    public DN getDn() {
        return entry.getDn();
    }
    
    public SearchResult getEntry() {
        return entry;
    }

    public void setEntry(SearchResult entry) {
        this.entry = entry;
    }

    public boolean isSearched() {
        return searched;
    }

    public void setSearched(boolean searched) {
        this.searched = searched;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public void addLinkedEntry(SearchResult entry) {
        linkedEntries.put(entry.getDn(), entry);
    }

    public Collection<SearchResult> getLinkedEntries() {
        return linkedEntries.values();
    }

    public Collection<DN> getLinkedDNs() {
        return linkedEntries.keySet();
    }
    
    public SearchResult getLinkedEntry(DN dn) {
        return linkedEntries.get(dn);
    }

    public void removeLinkedEntry(DN dn) {
        linkedEntries.remove(dn);
    }

    public void removeLinkedEntries() {
        linkedEntries.clear();
    }

    public void addMatchedEntry(SearchResult entry) {
        matchedEntries.put(entry.getDn(), entry);
    }

    public Collection<SearchResult> getMatchedEntries() {
        return matchedEntries.values();
    }

    public Collection<DN> getMatchedDNs() {
        return matchedEntries.keySet();
    }

    public SearchResult getMatchedEntry(DN dn) {
        return matchedEntries.get(dn);
    }

    public void removeMatchedEntry(DN dn) {
        matchedEntries.remove(dn);
    }

    public void removeMatchedEntries() {
        matchedEntries.clear();
    }

    public int getStorage() {
        return storage;
    }

    public void setStorage(int storage) {
        this.storage = storage;
    }

    public String getLocalAttribute() {
        return localAttribute;
    }

    public void setLocalAttribute(String localAttribute) {
        this.localAttribute = localAttribute;
    }

    public String getGlobalAttribute() {
        return globalAttribute;
    }

    public void setGlobalAttribute(String globalAttribute) {
        this.globalAttribute = globalAttribute;
    }
}
