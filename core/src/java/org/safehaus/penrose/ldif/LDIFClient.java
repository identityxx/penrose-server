package org.safehaus.penrose.ldif;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldif.connection.LDIFConnection;

/**
 * @author Endi Sukma Dewata
 */
public class LDIFClient {

    public LDIFConnection connection;

    public LDIFClient(LDIFConnection connection) throws Exception {
        this.connection = connection;
    }

    public void add(AddRequest request, AddResponse response) throws Exception {
    }

    public void bind(BindRequest request, BindResponse response) throws Exception {
    }

    public void compare(CompareRequest request, CompareResponse response) throws Exception {
    }

    public void delete(DeleteRequest request, DeleteResponse response) throws Exception {
    }

    public void modify(ModifyRequest request, ModifyResponse response) throws Exception {
    }

    public void modrdn(ModRdnRequest request, ModRdnResponse response) throws Exception {
    }

    public void search(SearchRequest request, SearchResponse response) throws Exception {
    }

    public void close() throws Exception {
    }
}
