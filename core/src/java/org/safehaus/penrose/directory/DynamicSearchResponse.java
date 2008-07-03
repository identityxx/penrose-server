package org.safehaus.penrose.directory;

import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * @author Endi Sukma Dewata
 */
public class DynamicSearchResponse extends SearchResponse {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    Session session;
    Partition partition;

    DynamicEntry entry;
    List<Collection<SourceRef>> groupsOfSources;
    SourceValues sourceValues;
    Interpreter interpreter;

    SearchRequest request;
    SearchResponse response;

    DN lastDn;
    Attributes lastAttributes;
    Entry lastEntry;
    SourceValues lastSourceValues;

    public DynamicSearchResponse(
            Session session,
            DynamicEntry entry,
            List<Collection<SourceRef>> groupsOfSources,
            SourceValues sourceValues,
            Interpreter interpreter,
            SearchRequest request,
            SearchResponse response
    ) {
        this.session = session;

        this.entry = entry;
        this.partition = entry.getPartition();

        this.groupsOfSources = groupsOfSources;
        this.sourceValues = sourceValues;
        this.interpreter = interpreter;

        this.request = request;
        this.response = response;
    }

    public void add(SearchResult result) throws Exception {

        SourceValues sv = (SourceValues)sourceValues.clone();
        sv.set(result.getSourceValues());

        if (debug) {
            log.debug("Source values:");
            sv.print();
        }

        interpreter.set(sv);
        entry.propagate(sv, interpreter);
        interpreter.clear();

        if (debug) {
            log.debug("Source values:");
            sv.print();
        }

        boolean complete = searchSecondarySources(sv);

        if (!complete) return;

        interpreter.set(sv);
        Collection<DN> dns = entry.computeDns(interpreter);
        Attributes attributes = entry.computeAttributes(interpreter);
        interpreter.clear();

        if (debug) {
            log.debug("Attributes:");
            attributes.print();
        }

        for (DN dn : dns) {
            if (lastDn == null) {
                if (debug) log.debug("Generating entry "+dn);
                lastDn = dn;
                lastAttributes = attributes;
                lastEntry = entry;
                lastSourceValues = sv;

            } else if (lastDn.equals(dn)) {
                if (debug) log.debug("Merging entry " + dn);
                lastAttributes.add(attributes);
                lastSourceValues.add(sv);

            } else {
                if (debug) log.debug("Returning entry " + lastDn);
                SearchResult searchResult = new SearchResult(lastDn, lastAttributes);
                searchResult.setEntryId(lastEntry.getId());
                searchResult.setSourceValues(lastSourceValues);
                response.add(searchResult);

                if (debug) log.debug("Generating entry "+dn);
                lastDn = dn;
                lastAttributes = attributes;
                lastEntry = entry;
                lastSourceValues = sv;
            }
        }
    }

    public void close() throws Exception {

        if (lastDn != null) {
            if (debug) log.debug("Returning entry " + lastDn);
            SearchResult searchResult = new SearchResult(lastDn, lastAttributes);
            searchResult.setEntryId(lastEntry.getId());
            searchResult.setSourceValues(lastSourceValues);
            response.add(searchResult);
        }

        response.close();
    }

    public boolean searchSecondarySources(
            SourceValues sv
    ) throws Exception {
        if (groupsOfSources.size() <= 1) return true;

        for (int i=1; i<groupsOfSources.size(); i++) {
            Collection<SourceRef> sourceRefs = groupsOfSources.get(i);
            if (debug) log.debug("Processing group " + sourceRefs);

            SourceRef sourceRef = sourceRefs.iterator().next();
            
            String flag = sourceRef.getSearch();
            if (debug) log.debug("Flag: "+flag);

            if (SourceMapping.IGNORE.equals(flag)) {
                continue;
            }

            SearchResponse sr = new SearchResponse();

            //Collection<SourceRef> primarySourceRefs = entry.getPrimarySourceRefs();
            Collection<SourceRef> localSourceRefs = entry.getLocalSourceRefs();

            Source source = sourceRef.getSource();

            try {
                source.search(
                        session,
                        //primarySourceRefs,
                        localSourceRefs,
                        sourceRefs,
                        sv,
                        request,
                        sr
                );
                
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            if (SourceMapping.REQUIRED.equals(flag) && !sr.hasNext()) {
                log.debug("Required entries not found.");
                return false;
            }

            while (sr.hasNext()) {
                SearchResult result = sr.next();
                sv.add(result.getSourceValues());
            }

            interpreter.set(sv);
            entry.propagate(sv, interpreter);
            interpreter.clear();

            if (debug) {
                log.debug("Source values:");
                sv.print();
            }
        }

        return true;
    }
}
