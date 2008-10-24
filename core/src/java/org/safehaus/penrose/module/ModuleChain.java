package org.safehaus.penrose.module;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.directory.Entry;

/**
 * @author Endi Sukma Dewata
 */
public class ModuleChain {

    protected Entry entry;
    protected Module module;
    protected ModuleChain chain;

    public ModuleChain(Entry entry) {
        this.entry = entry;
    }

    public ModuleChain(Entry entry, Module module, ModuleChain chain) {
        this.entry  = entry;
        this.module = module;
        this.chain  = chain;
    }

    public Entry getEntry() {
        return entry;
    }

    public void setEntry(Entry entry) {
        this.entry = entry;
    }

    public Module getModule() {
        return module;
    }

    public void setModule(Module module) {
        this.module = module;
    }

    public ModuleChain getChain() {
        return chain;
    }

    public void setChain(ModuleChain chain) {
        this.chain = chain;
    }

    public void add(Session session, AddRequest request, AddResponse response) throws Exception {
        if (module == null) {
            entry.add(session, request, response);
        } else {
            module.add(session, request, response, chain);
        }
    }

    public void bind(Session session, BindRequest request, BindResponse response) throws Exception {
        if (module == null) {
            entry.bind(session, request, response);
        } else {
            module.bind(session, request, response, chain);
        }
    }

    public void compare(Session session, CompareRequest request, CompareResponse response) throws Exception {
        if (module == null) {
            entry.compare(session, request, response);
        } else {
            module.compare(session, request, response, chain);
        }
    }

    public void delete(Session session, DeleteRequest request, DeleteResponse response) throws Exception {
        if (module == null) {
            entry.delete(session, request, response);
        } else {
            module.delete(session, request, response, chain);
        }
    }

    public void modify(Session session, ModifyRequest request, ModifyResponse response) throws Exception {
        if (module == null) {
            entry.modify(session, request, response);
        } else {
            module.modify(session, request, response, chain);
        }
    }

    public void modrdn(Session session, ModRdnRequest request, ModRdnResponse response) throws Exception {
        if (module == null) {
            entry.modrdn(session, request, response);
        } else {
            module.modrdn(session, request, response, chain);
        }
    }

    public void search(SearchOperation operation) throws Exception {
        if (module == null) {
            entry.search(operation);
        } else {
            module.search(operation, chain);
        }
    }

    public void unbind(Session session, UnbindRequest request, UnbindResponse response) throws Exception {
        if (module == null) {
            entry.unbind(session, request, response);
        } else {
            module.unbind(session, request, response, chain);
        }
    }
}
