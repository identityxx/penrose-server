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

/**
 * @author Endi S. Dewata
 */
public interface Backend {

    public void init() throws Exception;

    public boolean contains(String dn) throws Exception;
    public boolean contains(DN dn) throws Exception;

    public ConnectRequest createConnectRequest() throws Exception;
    public DisconnectRequest createDisconnectRequest() throws Exception;

    public Connection connect(ConnectRequest request) throws Exception;
    public Connection getConnection(Object connectionId) throws Exception;
    public void disconnect(DisconnectRequest request) throws Exception;

    public Control createControl(String oid, byte[] value, boolean critical) throws Exception;

    public DN createDn(String dn) throws Exception;
    public RDN createRdn(String rdn) throws Exception;
    public Filter createFilter(String filter) throws Exception;
    
    public Attributes createAttributes() throws Exception;
    public Attribute createAttribute(String name) throws Exception;
    public Modification createModification(int type, Attribute attribute) throws Exception;

    public AbandonRequest createAbandonRequest() throws Exception;
    public AbandonResponse createAbandonResponse() throws Exception;

    public AddRequest createAddRequest() throws Exception;
    public AddResponse createAddResponse() throws Exception;

    public BindRequest createBindRequest() throws Exception;
    public BindResponse createBindResponse() throws Exception;

    public CompareRequest createCompareRequest() throws Exception;
    public CompareResponse createCompareResponse() throws Exception;

    public DeleteRequest createDeleteRequest() throws Exception;
    public DeleteResponse createDeleteResponse() throws Exception;

    public ModifyRequest createModifyRequest() throws Exception;
    public ModifyResponse createModifyResponse() throws Exception;

    public ModRdnRequest createModRdnRequest() throws Exception;
    public ModRdnResponse createModRdnResponse() throws Exception;

    public SearchRequest createSearchRequest() throws Exception;
    public SearchResponse createSearchResponse() throws Exception;

    public UnbindRequest createUnbindRequest() throws Exception;
    public UnbindResponse createUnbindResponse() throws Exception;

}
