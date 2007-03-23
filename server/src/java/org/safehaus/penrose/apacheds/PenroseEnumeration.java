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
package org.safehaus.penrose.apacheds;

import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.session.SearchResult;
import org.safehaus.penrose.entry.Entry;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public class PenroseEnumeration implements NamingEnumeration {

    Logger log = LoggerFactory.getLogger(getClass());

    public SearchResponse searchResponse;

    public PenroseEnumeration(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
    }

    public void close() throws NamingException {
        try {
            searchResponse.close();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public boolean hasMore() throws NamingException {
        try {
            boolean hasNext = searchResponse.hasNext();
            if (hasNext) return true;

            List referrals = searchResponse.getReferrals();
            //log.debug("Search operation returned "+referrals.size()+" referral(s).");

            if (!referrals.isEmpty()) {
                PenroseReferralException lre = new PenroseReferralException(referrals);
                throw lre;
                /*
                String referral = (String)referrals.remove(0);
                log.debug("Referral: "+referral);
                throw new PenroseReferralException(referral, !referrals.isEmpty());
                */
            }

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
            Entry entry = result.getEntry();
            return EntryTool.createSearchResult(entry);

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
