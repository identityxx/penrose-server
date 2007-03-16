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

import org.apache.directory.shared.ldap.filter.ExprNode;
import org.apache.directory.shared.ldap.name.LdapDN;
import org.apache.directory.server.core.DirectoryServiceConfiguration;
import org.apache.directory.server.core.configuration.PartitionConfiguration;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.session.SearchResult;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.ietf.ldap.LDAPException;

import javax.naming.*;
import javax.naming.directory.*;
import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class PenrosePartition implements org.apache.directory.server.core.partition.Partition {

    Logger log = LoggerFactory.getLogger(getClass());

    DirectoryServiceConfiguration directoryServiceConfiguration;
    PartitionConfiguration partitionConfiguration;

    Penrose penrose;
    PartitionManager partitionManager;

    public void setPenrose(Penrose penrose) throws Exception {
        this.penrose = penrose;
        PenroseContext penroseContext = penrose.getPenroseContext();
        partitionManager = penroseContext.getPartitionManager();
    }

    public void init(
            DirectoryServiceConfiguration directoryServiceConfiguration,
            PartitionConfiguration partitionConfiguration
    ) throws NamingException {
        this.directoryServiceConfiguration = directoryServiceConfiguration;
        this.partitionConfiguration = partitionConfiguration;

        String name = partitionConfiguration.getName();

        File dir = new File("partitions/"+name);
        if (!dir.exists()) return;

        log.debug("-------------------------------------------------------------------------------");
        log.debug("Initializing "+name+" partition ...");
    }

    public boolean isInitialized() {
        return true;
    }

    public void destroy() {
    }

    public LdapDN getSuffix() throws NamingException {
        return getUpSuffix();
    }

    public LdapDN getUpSuffix() throws NamingException {
        try {
            Partition partition = partitionManager.getPartition("DEFAULT");
            Collection rootEntryMappings = partition.getRootEntryMappings();
            EntryMapping entryMapping = (EntryMapping)rootEntryMappings.iterator().next();
            return new LdapDN(entryMapping.getDn().toString());

        } catch (Exception e) {
            throw ExceptionTool.createNamingException(e);
        }
    }

    public final boolean isSuffix(LdapDN name) throws NamingException {
        return getSuffix().equals(name);
    }

    public void bind(LdapDN bindDn, byte[] credentials, List mechanisms, String saslAuthId) throws NamingException {
        log.info("Binding as \""+bindDn+"\"");
        log.info(" - mechanisms: \""+mechanisms+"\"");
        log.info(" - sslAuthId: \""+saslAuthId+"\"");
    }

    public void unbind(LdapDN bindDn) throws NamingException {
        log.info("Unbinding as \""+bindDn+"\"");
    }

    public void delete(LdapDN name) throws NamingException {
        String dn = name.getUpName();
        log.info("Deleting \""+dn+"\"");

        try {
            Session session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            session.delete(dn);

            session.close();

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public void add(LdapDN name, Attributes attrs) throws NamingException {
        DN dn = new DN(name.getUpName());
        log.info("Adding \""+dn+"\"");

        try {
            Session session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            org.safehaus.penrose.entry.Attributes attributes = new org.safehaus.penrose.entry.Attributes();
            for (NamingEnumeration ne = attrs.getAll(); ne.hasMore(); ) {
                javax.naming.directory.Attribute attribute = (javax.naming.directory.Attribute)ne.next();
                String attributeName = attribute.getID();

                for (NamingEnumeration ne2 = attribute.getAll(); ne2.hasMore(); ) {
                    Object value = ne2.next();
                    attributes.addValue(attributeName, value);
                }
            }

            AddRequest request = new AddRequest();
            request.setDn(dn);
            request.setAttributes(attributes);

            AddResponse response = new AddResponse();

            session.add(request, response);

            session.close();

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public void modify(LdapDN name, int modOp, Attributes attributes) throws NamingException {
        String dn = name.getUpName();
        log.info("Modifying \""+dn+"\"");
        log.debug("changetype: modify");

        try {
            Collection modifications = new ArrayList();

            for (Enumeration e=attributes.getAll(); e.hasMoreElements(); ) {
                Attribute attribute = (Attribute)e.nextElement();

                ModificationItem modification = new ModificationItem(modOp, attribute);
                modifications.add(modification);
            }

            Session session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            session.modify(dn, modifications);

            session.close();

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public void modify(LdapDN name, ModificationItem[] modificationItems) throws NamingException {
        String dn = name.getUpName();
        log.info("Modifying \""+dn+"\"");
        log.debug("changetype: modify");

        try {
            Session session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            Collection modifications = new ArrayList(Arrays.asList(modificationItems));
            session.modify(dn, modifications);

            session.close();

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public NamingEnumeration list(LdapDN name) throws NamingException {
        DN dn = new DN(name.getUpName());
        log.info("Listing \""+dn+"\"");

        try {
            Session session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter("(objectClass=*)");
            request.setScope(SearchRequest.SCOPE_ONE);
            request.setDereference(SearchRequest.DEREF_ALWAYS);

            SearchResponse response = new SearchResponse();

            session.search(request, response);

            return new PenroseEnumeration(response);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public NamingEnumeration search(
            LdapDN base,
            Map env,
            ExprNode filter,
            SearchControls searchControls
    ) throws NamingException {

        Session session = null;
        try {
            DN dn = new DN(base.getUpName());
            String deref = (String)env.get("java.naming.ldap.derefAliases");
            int scope = searchControls.getSearchScope();
            String returningAttributes[] = searchControls.getReturningAttributes();
            List attributeNames = returningAttributes == null ? new ArrayList() : Arrays.asList(returningAttributes);

            String newFilter = FilterTool.convert(filter).toString();

            log.info("Searching \""+dn+"\"");
            log.debug(" - deref: "+deref);
            log.debug(" - scope: "+scope);
            log.debug(" - filter: "+newFilter);
            log.debug(" - attributeNames: "+attributeNames);

            session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter(newFilter);
            request.setScope(searchControls.getSearchScope());
            request.setDereference(SearchRequest.DEREF_ALWAYS);
            request.setAttributes(searchControls == null ? null : searchControls.getReturningAttributes());

            SearchResponse response = new SearchResponse();

            session.search(request, response);

            return new PenroseEnumeration(response);

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);

        } finally {
            if (session != null) try { session.close(); } catch (Exception e) {}
        }
    }

    public Attributes lookup(LdapDN name) throws NamingException {
        DN dn = new DN(name.getUpName());
        log.debug("Looking up \""+dn+"\"");

        try {
            Session session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter("(objectClass=*)");
            request.setScope(SearchRequest.SCOPE_BASE);
            request.setDereference(SearchRequest.DEREF_ALWAYS);

            SearchResponse response = new SearchResponse();

            session.search(request, response);

            session.close();

            SearchResult result = (SearchResult)response.next();
            Entry entry = result.getEntry();

            javax.naming.directory.SearchResult sr = EntryTool.createSearchResult(entry);
            return sr.getAttributes();

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public Attributes lookup(LdapDN name, String[] attrIds) throws NamingException {
        DN dn = new DN(name.getUpName());

        try {
            //log.debug("===============================================================================");
            //log.debug("lookup(\""+dn+"\") as \""+principalDn+"\"");

            Session session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter("(objectClass=*)");
            request.setScope(SearchRequest.SCOPE_BASE);
            request.setDereference(SearchRequest.DEREF_ALWAYS);

            SearchResponse response = new SearchResponse();

            session.search(request, response);

            SearchResult result = (SearchResult)response.next();
            Entry entry = result.getEntry();

            javax.naming.directory.SearchResult sr = EntryTool.createSearchResult(entry);
            return sr.getAttributes();

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public boolean hasEntry(LdapDN name) throws NamingException {
        DN dn = new DN(name.getUpName());
        log.info("Checking \""+dn+"\"");

        try {
            Session session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter("(objectClass=*)");
            request.setScope(SearchRequest.SCOPE_BASE);
            request.setDereference(SearchRequest.DEREF_ALWAYS);

            SearchResponse response = new SearchResponse();

            session.search(request, response);

            boolean result = response.hasNext();

            session.close();

            return result;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public void modifyRn(LdapDN name, String newRn, boolean deleteOldRn) throws NamingException {
        String dn = name.getUpName();
        try {
            log.debug("===============================================================================");
            log.debug("modifyDn(\""+dn+"\")");

            Session session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            session.modrdn(dn, newRn, deleteOldRn);

            session.close();

        } catch (NamingException e) {
            throw e;

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public void move(LdapDN oriChildName, LdapDN newParentName) throws NamingException {
        log.info("Moving \""+oriChildName+"\" to \""+newParentName+"\"");
    }

    public void move(LdapDN oriChildName, LdapDN newParentName, String newRn, boolean deleteOldRn) throws NamingException {
        log.info("Moving \""+oriChildName+"\" to \""+newParentName+"\"");
    }

    public void sync() throws NamingException {
        //log.info("sync();");
    }

    public void close() throws NamingException {
        try {
            penrose.stop();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public boolean isClosed() {
        //log.info("isClosed();");
        return false;
    }
}
