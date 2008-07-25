package org.safehaus.penrose.federation.directory;

import org.safehaus.penrose.directory.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.source.LDAPSource;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.util.TextUtil;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class FederationEntry extends DynamicEntry {

    private boolean checkAccountDisabled;
    private boolean checkAccountLockout;
    private boolean checkAccountExpires;

    public void init() throws Exception {
        checkAccountDisabled = Boolean.parseBoolean(getParameter("checkAccountDisabled"));
        checkAccountLockout = Boolean.parseBoolean(getParameter("checkAccountLockout"));
        checkAccountExpires = Boolean.parseBoolean(getParameter("checkAccountExpires"));

        super.init();
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

        SearchResult sr = find(dn);
        SourceAttributes sv = sr.getSourceAttributes();

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sv);

        log.debug("Checking Active Directory link.");

        EntrySource adSourceRef = getSource("a");
        Source adSource = adSourceRef.getSource();

        Filter filter = null;

        for (EntryField adFieldRef : adSourceRef.getFields()) {

            Object value = interpreter.eval(adFieldRef);
            if (value instanceof byte[]) {
                log.debug(adFieldRef.getName()+": "+BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[])value));
            } else {
                log.debug(adFieldRef.getName()+": "+value);
            }
            if (value == null) continue;

            SimpleFilter sf = new SimpleFilter("objectGUID", "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);
        }

        if (filter != null) {

            log.debug("Searching Active Directory account.");

            SearchRequest adRequest = new SearchRequest();
            adRequest.setFilter(filter);

            SearchResponse adResponse = new SearchResponse();

            adSource.search(session, adRequest, adResponse);

            SearchResult adResult = adResponse.next();
            Attributes adAttributes = adResult.getAttributes();

            log.debug("Checking Active Directory account.");

            long userAccountControl = Long.parseLong((String)adAttributes.getValue("userAccountControl"));
            log.debug("userAccountControl: "+userAccountControl);

            if (checkAccountDisabled && (userAccountControl & 0x0002) > 0) {
                log.debug("Account is disabled.");
                throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
            }
/*
            if (checkAccountLockout && (userAccountControl & 0x0010) > 0) {
                log.debug("Account is locked out.");
                throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
            }

            if (checkAccountExpires) {
                long accountExpires = Long.parseLong((String)adAttributes.getValue("accountExpires"));
                log.debug("accountExpires: "+accountExpires);

                if (accountExpires != 0 && accountExpires != 0x7FFFFFFFFFFFFFFFL) {
                    Date d = toDate(accountExpires);
                    if (d.getTime() < System.currentTimeMillis()) {
                        log.debug("Account has expired at "+d+".");
                        throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
                    }
                }
            }
*/
            String bind = adSourceRef.getBind();
            if (!EntrySourceConfig.IGNORE.equals(bind)) {
                log.debug("Binding to Active Directory account.");

                BindRequest newRequest = (BindRequest)request.clone();
                newRequest.setDn(adResult.getDn());

                adSource.bind(session, newRequest, response);
            }

            if (EntrySourceConfig.SUFFICIENT.equals(bind)) {
                return;
            }
        }

        Object userPassword = interpreter.get("g.userPassword");

        if (userPassword != null) {
            log.debug("Binding to Global Repository.");

            EntrySource sourceRef = getSource("g");
            Source source = sourceRef.getSource();

            DN globalDn = new DN((String)interpreter.get("g.dn"));
            DN globalBaseDn = new DN(source.getParameter(LDAPSource.BASE_DN));

            BindRequest newRequest = (BindRequest)request.clone();
            newRequest.setDn(globalDn.getPrefix(globalBaseDn));

            source.bind(session, newRequest, response);

            return;
        }

        EntrySource sourceRef = getSource("n");
        Source source = sourceRef.getSource();

        DN nisDn = new DN((String)interpreter.get("n.dn"));
        DN nisBaseDn = new DN(source.getParameter(LDAPSource.BASE_DN));

        log.debug("Binding to NIS Repository.");

        BindRequest newRequest = (BindRequest)request.clone();
        newRequest.setDn(nisDn.getPrefix(nisBaseDn));

        source.bind(session, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validateFilter(Filter filter) throws Exception {
        // ignore
    }

    public Filter createFilter(Attributes attributes) throws Exception {

        Filter filter = null;

        for (Attribute attribute : attributes.getAll()) {
            String name = attribute.getName();
            for (Object value : attribute.getValues()) {
                SimpleFilter sf = new SimpleFilter(name, "=", value);
                filter = FilterTool.appendAndFilter(filter, sf);
            }
        }

        return filter;
    }

    public void executeSearch(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        //super.executeSearch(session, request, response);

        DN baseDn = request.getDn();
        int scope = request.getScope();

        boolean baseSearch = (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB)
                && getDn().matches(baseDn);

        Filter filter = request.getFilter();

        SourceAttributes sourceAttributes = new SourceAttributes();
        Interpreter interpreter = partition.newInterpreter();

        log.debug("Extracting source attributes from DN:");
        extractSourceAttributes(baseDn, interpreter, sourceAttributes);

        if (debug) {
            log.debug("Source attributes:");
            sourceAttributes.print();
        }

        EntrySource n = getSource("n");
        EntrySource g = getSource("g");
        EntrySource a = getSource("a");

        Collection<DN> dns = new LinkedHashSet<DN>();

        FilterBuilder filterBuilder = new FilterBuilder(this, sourceAttributes, interpreter);

        Filter nFilter = filterBuilder.convert(filter, n);
        nFilter = FilterTool.appendAndFilter(nFilter, createFilter(sourceAttributes.get("n")));

        Filter gFilter = filterBuilder.convert(filter, g);
        gFilter = FilterTool.appendAndFilter(gFilter, createFilter(sourceAttributes.get("g")));

        Filter aFilter = filterBuilder.convert(filter, a);
        aFilter = FilterTool.appendAndFilter(aFilter, createFilter(sourceAttributes.get("a")));

        if (!baseSearch) { // prevent infinite loop

            if (aFilter != null) {
                if (debug) {
                    log.debug("################################################################");
                    log.debug("Search source a with filter "+aFilter);
                }
                SourceAttributes sa = new SourceAttributes();

                SearchResponse aResponse = a.search(session, aFilter);

                while (aResponse.hasNext()) {

                    SearchResult aResult = aResponse.next();
                    sa.set("a", aResult);

                    Object objectGUID = sa.getValue("a", "objectGUID");

                    SearchResponse gResponse = g.search(
                        session,
                        new SimpleFilter("seeAlsoObjectGUID", "=", objectGUID)
                    );

                    while (gResponse.hasNext()) {

                        SearchResult gResult = gResponse.next();
                        sa.set("g", gResult);

                        Collection<Object> seeAlsoValues = sa.getValues("g", "seeAlso");

                        for (Object seeAlso : seeAlsoValues) {

                            try {
                                SearchResult nResult = n.find(session, (String)seeAlso);
                                sa.set("n", nResult);

                                DN dn = createDn(sa);
                                if (debug) log.debug("Found "+dn);

                                dns.add(dn);

                            } catch (Exception e) {
                                // ignore
                            }
                        }
                    }
                }
            }

            if (gFilter != null) {
                if (debug) {
                    log.debug("################################################################");
                    log.debug("Search source g with filter "+gFilter);
                }
                SourceAttributes sa = new SourceAttributes();

                SearchResponse gResponse = g.search(session, gFilter);

                while (gResponse.hasNext()) {

                    SearchResult gResult = gResponse.next();
                    sa.set("g", gResult);

                    Collection<Object> seeAlsoValues = sa.getValues("g", "seeAlso");

                    for (Object seeAlso : seeAlsoValues) {

                        try {
                            SearchResult nResult = n.find(session, (String)seeAlso);
                            sa.set("n", nResult);

                            DN dn = createDn(sa);
                            if (debug) log.debug("Found "+dn);

                            dns.add(dn);

                        } catch (Exception e) {
                            // ignore
                        }
                    }
                }
            }

            if (nFilter != null) {
                if (debug) {
                    log.debug("################################################################");
                    log.debug("Search source n with filter "+nFilter);
                }
                SourceAttributes sa = new SourceAttributes();

                SearchResponse nResponse = n.search(session, nFilter);

                while (nResponse.hasNext()) {
                    SearchResult nResult = nResponse.next();
                    sa.set("n", nResult);

                    DN dn = createDn(sa);
                    if (debug) log.debug("Found "+dn);

                    dns.add(dn);
                }
            }

            if (nFilter != null || gFilter != null || aFilter != null) {
                log.debug("################################################################");
                for (DN dn : dns) {
                    if (debug) log.debug("Returning "+dn);
                    SearchResult result = find(session, dn);
                    response.add(result);
                }
                log.debug("################################################################");
                return;
            }
        }

        SourceAttributes sa = new SourceAttributes();

        SearchResponse nResponse = n.search(session, nFilter);

        while (nResponse.hasNext()) {

            SearchResult nResult = nResponse.next();
            sa.set("n", nResult);

            Object dn = sa.getValue("n", "dn");

            SearchResponse gResponse = g.search(
                session,
                new SimpleFilter("seeAlso", "=", dn)
            );

            if (!gResponse.hasNext()) {
                response.add(createSearchResult(sa));
                continue;
            }

            do {
                SearchResult gResult = gResponse.next();
                sa.set("g", gResult);

                Object seeAlsoObjectGUID = sa.getValue("g", "seeAlsoObjectGUID");

                if (seeAlsoObjectGUID == null) {
                    response.add(createSearchResult(sa));
                    continue;
                }

                SearchResponse aResponse = a.search(
                        session,
                        new SimpleFilter("objectGUID", "=", seeAlsoObjectGUID)
                );

                if (!aResponse.hasNext()) {
                    response.add(createSearchResult(sa));
                    continue;
                }

                do {
                    SearchResult aResult = aResponse.next();
                    sa.set("a", aResult);

                    response.add(createSearchResult(sa));

                } while (aResponse.hasNext());

                sa.remove("a");

            } while (gResponse.hasNext());

            sa.remove("g");
        }
    }

    public Date toDate(long accountExpires) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(1601, 0, 1, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        accountExpires = accountExpires / 10000 + calendar.getTime().getTime();
        return new Date(accountExpires);
    }

    public boolean isCheckAccountExpires() {
        return checkAccountExpires;
    }

    public void setCheckAccountExpires(boolean checkAccountExpires) {
        this.checkAccountExpires = checkAccountExpires;
    }

    public boolean isCheckAccountLockout() {
        return checkAccountLockout;
    }

    public void setCheckAccountLockout(boolean checkAccountLockout) {
        this.checkAccountLockout = checkAccountLockout;
    }

    public boolean isCheckAccountDisabled() {
        return checkAccountDisabled;
    }

    public void setCheckAccountDisabled(boolean checkAccountDisabled) {
        this.checkAccountDisabled = checkAccountDisabled;
    }
}
