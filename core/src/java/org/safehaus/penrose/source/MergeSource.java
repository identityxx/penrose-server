package org.safehaus.penrose.source;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.PresentFilter;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class MergeSource extends Source {

    public final static String SOURCES = "sources";

    public Collection<String> sourceNames = new ArrayList<String>();

    public void init() throws Exception {
        String value = getParameter(SOURCES);
        String[] s = value.split(",");
        sourceNames.addAll(Arrays.asList(s));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        if (debug) log.debug("Adding "+request.getDn()+".");

        SourceManager sourceManager = partition.getSourceManager();

        for (String sourceName : sourceNames) {
            Source source = sourceManager.getSource(sourceName);

            AddResponse newResponse = new AddResponse();

            try {
                source.add(session, request, newResponse);
            } catch (LDAPException e) {
                log.debug(e.getMessage());
                // ignore
            }
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

        if (debug) log.debug("Binding "+request.getDn()+".");

        SourceManager sourceManager = partition.getSourceManager();

        for (String sourceName : sourceNames) {
            Source source = sourceManager.getSource(sourceName);

            BindResponse newResponse = new BindResponse();

            try {
                source.bind(session, request, newResponse);
                return;

            } catch (LDAPException e) {
                log.debug(e.getMessage());
                // ignore
            }
        }

        response.setReturnCode(LDAP.INVALID_CREDENTIALS);
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

        if (debug) log.debug("Comparing "+request.getDn()+".");

        SourceManager sourceManager = partition.getSourceManager();

        for (String sourceName : sourceNames) {
            Source source = sourceManager.getSource(sourceName);

            CompareResponse newResponse = new CompareResponse();

            try {
                source.compare(session, request, newResponse);

                int rc = newResponse.getReturnCode();
                if (rc == LDAP.COMPARE_TRUE) {
                    response.setReturnCode(rc);
                    return;
                }

            } catch (LDAPException e) {
                log.debug(e.getMessage());
                // ignore
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

        if (debug) log.debug("Deleting "+request.getDn()+".");

        SourceManager sourceManager = partition.getSourceManager();

        for (String sourceName : sourceNames) {
            Source source = sourceManager.getSource(sourceName);

            DeleteResponse newResponse = new DeleteResponse();

            try {
                source.delete(session, request, newResponse);
            } catch (LDAPException e) {
                log.debug(e.getMessage());
                // ignore
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        if (debug) log.debug("Modifying "+request.getDn()+".");

        SourceManager sourceManager = partition.getSourceManager();

        for (String sourceName : sourceNames) {
            Source source = sourceManager.getSource(sourceName);

            ModifyResponse newResponse = new ModifyResponse();
            
            try {
                source.modify(session, request, newResponse);
            } catch (LDAPException e) {
                log.debug(e.getMessage());
                // ignore
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        if (debug) log.debug("Renaming "+request.getDn()+".");

        SourceManager sourceManager = partition.getSourceManager();

        for (String sourceName : sourceNames) {
            Source source = sourceManager.getSource(sourceName);

            ModRdnResponse newResponse = new ModRdnResponse();

            try {
                source.modrdn(session, request, newResponse);
            } catch (LDAPException e) {
                log.debug(e.getMessage());
                // ignore
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        if (debug) log.debug("Searching "+request.getDn()+".");

        Filter filter = request.getFilter();

        Partition partition = getPartition();
        SourceManager sourceManager = partition.getSourceManager();

        final Collection<DN> list = new LinkedHashSet<DN>();
        final Map<String,Map<DN,SearchResult>> maps = new LinkedHashMap<String,Map<DN,SearchResult>>();

        for (String sourceName : sourceNames) {
            Source source = sourceManager.getSource(sourceName);

            final Map<DN,SearchResult> map = new LinkedHashMap<DN,SearchResult>();
            maps.put(sourceName, map);

            SearchResponse newResponse = new SearchResponse() {
                public void add(SearchResult result) throws Exception {
                    DN dn = result.getDn();
                    list.add(dn);
                    map.put(dn, result);
                }
            };

            try {
                source.search(session, request, newResponse);
            } catch (LDAPException e) {
                log.debug(e.getMessage());
                // ignore
            }
        }

        boolean nullFilter = filter == null ||
                filter instanceof PresentFilter && "objectClass".equalsIgnoreCase(((PresentFilter)filter).getAttribute());

        for (DN dn : list) {
            SearchResult newResult = new SearchResult();
            newResult.setDn(dn);
            Attributes newAttributes = newResult.getAttributes();

            for (String sourceName : sourceNames) {
                Map<DN,SearchResult> map = maps.get(sourceName);
                SearchResult result = map.get(dn);

                if (result == null) {
                    if (nullFilter) continue;

                    Source source = sourceManager.getSource(sourceName);
                    try {
                        result = source.find(session, dn);
                    } catch (Exception e) {
                        log.debug(e.getMessage());
                        // ignore
                    }
                    if (result == null) continue;
                }

                newAttributes.add(result.getAttributes());
            }

            response.add(newResult);
        }

        response.close();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        if (debug) log.debug("Unbinding "+request.getDn()+".");

        SourceManager sourceManager = partition.getSourceManager();

        for (String sourceName : sourceNames) {
            Source source = sourceManager.getSource(sourceName);

            UnbindResponse newResponse = new UnbindResponse();

            try {
                source.unbind(session, request, newResponse);
            } catch (LDAPException e) {
                log.debug(e.getMessage());
                // ignore
            }
        }
    }
}
