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
package org.safehaus.penrose.ldapbackend;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface Connection {

    public DN getBindDn() throws Exception;
    public boolean isAnonymous() throws Exception;
    public boolean isRoot() throws Exception;

    public void close() throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Abandon
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void abandon(
            int idToAbandon
    ) throws Exception;

    public void abandon(
            AbandonRequest request,
            AbandonResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            DN dn,
            Attributes attributes
    ) throws Exception;

    public void add(
            AddRequest request,
            AddResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            DN dn,
            String password
    ) throws Exception;

    public void bind(
            DN dn,
            byte[] password
    ) throws Exception;

    public void bind(
            BindRequest request,
            BindResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean compare(
            DN dn,
            String attributeName,
            Object attributeValue
    ) throws Exception;

    public boolean compare(
            CompareRequest request,
            CompareResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            DN dn
    ) throws Exception;

    public void delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            DN dn,
            Collection<Modification> modifications
    ) throws Exception;

    public void modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception;

    public void modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResponse search(
            DN dn,
            Filter filter,
            int scope
    ) throws Exception;

    public void search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind() throws Exception;
    
    public void unbind(
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception;
}