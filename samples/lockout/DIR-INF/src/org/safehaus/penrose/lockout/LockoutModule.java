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
package org.safehaus.penrose.lockout;

import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.event.BindEvent;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.Session;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Date;
import java.sql.Timestamp;

/**
 * @author Endi Sukma Dewata
 */
public class LockoutModule extends Module {

    public final static String LOCKS = "locks";
    public final static String DEFAULT_LOCKS = "locks";

    public final static String LIMIT = "limit";
    public final static int DEFAULT_LIMIT = 3;

    public final static String EXPIRATION = "expiration";
    public final static int DEFAULT_EXPIRATION = 5; // minutes

    public Source source;
    public int limit;
    public int expiration;

    public void init() throws Exception {
        String s = getParameter(LOCKS);

        SourceManager sourceManager = partition.getSourceManager();
        source = sourceManager.getSource(s == null ? DEFAULT_LOCKS : s);

        s = getParameter(LIMIT);
        limit = s == null ? DEFAULT_LIMIT : Integer.parseInt(s);

        s = getParameter(EXPIRATION);
        expiration = s == null ? DEFAULT_EXPIRATION : Integer.parseInt(s);
    }

    public void beforeBind(BindEvent event) throws Exception {

        if (limit == 0) return;

        Session session = getSession();

        try {
            BindRequest request = event.getRequest();
            BindResponse response = event.getResponse();

            String account = request.getDn().getNormalizedDn();

            log.debug("Checking account lockout for "+account+".");

            RDNBuilder rb = new RDNBuilder();
            rb.set("account", account);
            RDN rdn = rb.toRdn();

            SearchResult result = source.find(session, rdn);
            if (result == null) return;

            Attributes attributes = result.getAttributes();

            if (expiration > 0) {
                Date timestamp = new Date(((Timestamp)attributes.getValue("timestamp")).getTime());
                log.debug("Timestamp: "+Lock.DATE_FORMAT.format(timestamp));

                long elapsed = System.currentTimeMillis() - timestamp.getTime();
                if (elapsed >= expiration * 60 * 1000) {
                    log.debug("Lock has expired, removing lock.");
                    source.delete(session, rdn);
                    return;
                }
            }

            int counter = (Integer)attributes.getValue("counter");
            log.debug("Counter: "+counter);

            if (counter >= limit) {
                log.debug("Account has been locked.");
                response.setException(LDAP.createException(LDAP.INVALID_CREDENTIALS));
            }

        } finally {
            session.close();
        }
    }

    public void afterBind(BindEvent event) throws Exception {

        if (limit == 0) return;

        Session session = getSession();

        try {
            BindRequest request = event.getRequest();
            BindResponse response = event.getResponse();

            String account = request.getDn().getNormalizedDn();

            RDNBuilder rb = new RDNBuilder();
            rb.set("account", account);
            RDN rdn = rb.toRdn();

            if (response.getReturnCode() == LDAP.SUCCESS) {
                log.debug("Bind succeeded, removing lock.");
                source.delete(session, rdn);
                return;
            }

            log.debug("Bind failed, incrementing counter.");

            PartitionContext partitionContext = partition.getPartitionContext();
            PenroseConfig penroseConfig = partitionContext.getPenroseConfig();
            if (penroseConfig.getRootDn().matches(account)) return;

            SearchResult result = source.find(session, rdn);

            if (result == null) {
                Attributes attributes = new Attributes();
                attributes.add(new Attribute("account", account));
                attributes.add(new Attribute("counter", 1));
                attributes.add(new Attribute("timestamp", new Timestamp(System.currentTimeMillis())));

                source.add(session, rdn, attributes);

            } else {
                Attributes attributes = result.getAttributes();
                int counter = (Integer)attributes.getValue("counter");

                counter++;

                Collection<Modification> modifications = new ArrayList<Modification>();
                modifications.add(
                        new Modification(Modification.REPLACE, new Attribute("counter", counter))
                );
                modifications.add(
                        new Modification(Modification.REPLACE, new Attribute("timestamp", new Timestamp(System.currentTimeMillis())))
                );

                source.modify(session, rdn, modifications);
            }

        } finally {
            session.close();
        }
    }

    public void reset(String account) throws Exception {
        reset(new DN(account));
    }
    
    public void reset(DN account) throws Exception {

        Session session = getSession();

        try {
            RDNBuilder rb = new RDNBuilder();
            rb.set("account", account.getNormalizedDn());
            RDN rdn = rb.toRdn();

            source.delete(session, rdn);

        } finally {
            session.close();
        }
    }

    public Collection<Lock> list() throws Exception {

        Collection<Lock> accounts = new ArrayList<Lock>();
        if (limit == 0) return accounts;

        Session session = getSession();

        try {
            SearchRequest request = new SearchRequest();
            SearchResponse response = new SearchResponse();

            source.search(session, request, response);

            while (response.hasNext()) {
                SearchResult result = response.next();
                Attributes attributes = result.getAttributes();

                String account = (String)attributes.getValue("account");
                int counter = (Integer)attributes.getValue("counter");
                Date timestamp = new Date(((Timestamp)attributes.getValue("timestamp")).getTime());

                Lock lock = new Lock(account, counter, timestamp);
                accounts.add(lock);
            }

            return accounts;

        } finally {
            session.close();
        }
    }

    public void purge() throws Exception {

        if (expiration <= 0) return;
        
        Session session = getSession();

        try {
            SearchRequest request = new SearchRequest();
            SearchResponse response = new SearchResponse();

            source.search(session, request, response);

            while (response.hasNext()) {
                SearchResult result = response.next();
                Attributes attributes = result.getAttributes();

                String account = (String)attributes.getValue("account");
                Date timestamp = new Date(((Timestamp)attributes.getValue("timestamp")).getTime());

                log.debug("Checking lock timestamp for "+account+": "+Lock.DATE_FORMAT.format(timestamp));

                long elapsed = System.currentTimeMillis() - timestamp.getTime();
                if (elapsed < expiration * 60 * 1000) continue;

                log.debug("Unlocking "+account+".");

                source.delete(session, result.getDn());
            }

        } finally {
            session.close();
        }
    }
}
