package org.safehaus.penrose.handler;

import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.Link;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterEvaluator;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.ldap.*;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class DefaultHandler extends Handler {

    public DefaultHandler() throws Exception {
    }

    public void init(HandlerConfig handlerConfig) throws Exception {
        super.init(handlerConfig);
    }

    public void add(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        Attributes attributes = request.getAttributes();
        Collection values = attributes.getValues("objectClass");

        Collection objectClasses = entryMapping.getObjectClasses();
        boolean childHasObjectClass = false;

        for (Iterator i = objectClasses.iterator(); !childHasObjectClass && i.hasNext(); ) {
            String oc = (String)i.next();

            for (Iterator j = values.iterator(); j.hasNext(); ) {
                String objectClass = (String)j.next();
                if (childHasObjectClass = oc.equalsIgnoreCase(objectClass)) break;
            }
        }

        if (!childHasObjectClass) {
            throw ExceptionUtil.createLDAPException(LDAPException.OBJECT_CLASS_VIOLATION);
        }

        super.add(session, partition, entryMapping, request, response);
    }

    public void search(
            final Session session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();

        final Filter filter = request.getFilter();
        int scope = request.getScope();

        if (debug) {
            log.debug("Searching "+entryMapping.getDn()+" with scope "+ LDAPUtil.getScope(scope));
        }

        Link link = entryMapping.getLink();

        if (link != null) {

            String partitionName = link.getPartitionName();

            PartitionManager partitionManager = penroseContext.getPartitionManager();
            Partition p = partitionName == null ? partition : partitionManager.getPartition(partitionName);

            DN dn = link.getDn();
            Collection c = p.getEntryMappings(dn == null ? entryMapping.getDn() : dn);

            SearchRequest newRequest = new SearchRequest(request);

            for (Iterator j=c.iterator(); j.hasNext(); ) {
                EntryMapping em = (EntryMapping)j.next();

                Handler handler = handlerManager.getHandler(p, em);

                handler.search(
                        session,
                        p,
                        em,
                        em,
                        newRequest,
                        response
                );
            }

            return;
        }

        final FilterEvaluator filterEvaluator = penroseContext.getFilterEvaluator();

        if (scope == SearchRequest.SCOPE_BASE
                || scope == SearchRequest.SCOPE_SUB
                || scope == SearchRequest.SCOPE_ONE && partition.getParent(entryMapping) == baseMapping
                ) {

            if (filterEvaluator.eval(entryMapping, filter)) { // Check LDAP filter

                SearchResponse<SearchResult> sr = new SearchResponse<SearchResult>() {
                    public void add(SearchResult object) throws Exception {
                        SearchResult searchResult = (SearchResult)object;

                        if (debug) log.debug("Checking filter "+filter);

                        if (!filterEvaluator.eval(searchResult.getAttributes(), filter)) { // Check LDAP filter
                            if (debug) log.debug("Entry \""+searchResult.getDn()+"\" doesn't match search filter.");
                            return;
                        }

                        response.add(searchResult);
                    }
                };

                Engine engine = getEngine(partition, entryMapping);

                SourceValues sourceValues = new SourceValues();

                engine.search(
                        session,
                        partition,
                        baseMapping,
                        entryMapping,
                        sourceValues,
                        request,
                        sr
                );

            } else {
                if (debug) log.debug("Entry \""+entryMapping.getDn()+"\" doesn't match search filter.");
            }
        }

        if (scope == SearchRequest.SCOPE_ONE && entryMapping == baseMapping
                || scope == SearchRequest.SCOPE_SUB) {

            Collection children = partition.getChildren(entryMapping);

            SearchRequest newRequest = new SearchRequest(request);
            if (scope == SearchRequest.SCOPE_ONE) {
                newRequest.setScope(SearchRequest.SCOPE_BASE);
            }

            for (Object aChildren : children) {
                EntryMapping childMapping = (EntryMapping) aChildren;

                Handler handler = handlerManager.getHandler(partition, childMapping);

                handler.search(
                        session,
                        partition,
                        baseMapping,
                        childMapping,
                        newRequest,
                        response
                );

            }
        }
    }
}
