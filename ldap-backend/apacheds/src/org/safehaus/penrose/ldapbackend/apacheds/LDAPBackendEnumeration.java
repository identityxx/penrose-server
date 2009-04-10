/**
 * Copyright 2009 Red Hat, Inc.
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
package org.safehaus.penrose.ldapbackend.apacheds;

import org.safehaus.penrose.ldapbackend.SearchReferenceException;
import org.safehaus.penrose.ldapbackend.SearchResponse;
import org.safehaus.penrose.ldapbackend.SearchResult;
import org.apache.directory.shared.ldap.exception.LdapReferralException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class LDAPBackendEnumeration implements NamingEnumeration {

    public Logger log = LoggerFactory.getLogger(getClass());

    public SearchResponse searchResponse;

    public LDAPBackendEnumeration(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
    }

    public void close() throws NamingException {
        try {
            //searchResponse.close();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public boolean hasMore() throws NamingException {
        try {
            boolean hasNext = searchResponse.hasNext();
            if (hasNext) return true;
/*
            List referrals = searchResponse.getReferrals();
            //log.debug("Search operation returned "+referrals.size()+" referral(s).");

            if (!referrals.isEmpty()) {
                LDAPBackendReferralException lre = new LDAPBackendReferralException(referrals);
                throw lre;

                String referral = (String)referrals.remove(0);
                log.debug("Referral: "+referral);
                throw new LDAPBackendReferralException(referral, !referrals.isEmpty());
            }
*/

            //log.warn("Search operation returned "+searchResponse.getTotalCount()+" entries.");

            return false;

        } catch (Exception e) {
        	log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public Object next() throws NamingException {
        try {
            SearchResult result = (SearchResult)searchResponse.next();
            return EntryTool.createSearchResult(result);

        } catch (SearchReferenceException e) {
            Collection<String> urls = new ArrayList<String>();
            try {
                urls.addAll(e.getReference().getUrls());
            } catch (Exception e2) {
                log.error(e.getMessage(), e);
            }
            throw new LdapReferralException(urls);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public boolean hasMoreElements() {
        try {
            return hasMore();
        } catch (Exception e) {
            return false;
        }
    }

    public Object nextElement() {
        try {
            return next();
        } catch (Exception e) {
            return null;
        }
    }
}
