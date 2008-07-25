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
package org.safehaus.penrose.federation.module;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.mapping.Mapping;
import org.safehaus.penrose.mapping.MappingManager;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.session.Session;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class LinkingModule extends Module {

    String targetPartitionName;

    String sourceAttribute;
    String targetAttribute;

    String mappingName;
    String mappingPrefix;

    public void init() throws Exception {

        targetPartitionName = getParameter("target");
        log.debug("Target partition: "+targetPartitionName);

        sourceAttribute = getParameter("sourceAttribute");
        if (sourceAttribute == null) sourceAttribute = "dn";
        log.debug("Source attribute: "+ sourceAttribute);

        targetAttribute = getParameter("targetAttribute");
        if (targetAttribute == null) targetAttribute = "seeAlso";
        log.debug("Target attribute: "+ targetAttribute);

        mappingName = getParameter("mapping");
        log.debug("Mapping name: "+ mappingName);

        mappingPrefix = getParameter("mappingPrefix");
        log.debug("Mapping prefix: "+ mappingPrefix);
    }

    public void linkEntry(DN sourceDn, DN targetDn) throws Exception {

        Session adminSession = createAdminSession();

        try {
            Partition targetPartition = partition.getPartitionContext().getPartition(targetPartitionName);

            log.debug("##################################################################################################");
            log.debug("Link "+sourceDn+" to "+targetDn);

            Object value;
            if (sourceAttribute == null || sourceAttribute.equals("dn")) {
                value = sourceDn.toString();

            } else {
                SearchResult sourceEntry = partition.find(adminSession, sourceDn);
                Attributes sourceAttributes = sourceEntry.getAttributes();
                value = sourceAttributes.getValue(sourceAttribute);
            }

            Collection<Modification> modifications = new ArrayList<Modification>();
            modifications.add(new Modification(Modification.ADD, new Attribute(targetAttribute, value)));

            ModifyRequest request = new ModifyRequest();
            request.setDn(targetDn);
            request.setModifications(modifications);

            ModifyResponse response = new ModifyResponse();

            targetPartition.modify(adminSession, request, response);

        } finally {
            adminSession.close();
        }
    }

    public void unlinkEntry(DN sourceDn, DN targetDn) throws Exception {

        Session adminSession = createAdminSession();

        try {
            Partition targetPartition = partition.getPartitionContext().getPartition(targetPartitionName);

            log.debug("##################################################################################################");
            log.debug("Unlink "+sourceDn+" from "+targetDn);

            Object value;
            if (sourceAttribute == null || sourceAttribute.equals("dn")) {
                value = sourceDn.toString();

            } else {
                SearchResult sourceEntry = partition.find(adminSession, sourceDn);
                Attributes sourceAttributes = sourceEntry.getAttributes();
                value = sourceAttributes.getValue(sourceAttribute);
            }

            Collection<Modification> modifications = new ArrayList<Modification>();
            modifications.add(new Modification(Modification.DELETE, new Attribute(targetAttribute, value)));

            ModifyRequest request = new ModifyRequest();
            request.setDn(targetDn);
            request.setModifications(modifications);

            ModifyResponse response = new ModifyResponse();

            targetPartition.modify(adminSession, request, response);

        } finally {
            adminSession.close();
        }
    }

    public Collection<DN> searchLinks(DN sourceDn) throws Exception {

        Session adminSession = createAdminSession();

        try {
            Partition targetPartition = partition.getPartitionContext().getPartition(targetPartitionName);

            DN targetSuffix = targetPartition.getDirectory().getSuffix();

            log.debug("##################################################################################################");
            log.debug("Search links for "+sourceDn);

            Object value;
            if (sourceAttribute == null || sourceAttribute.equals("dn")) {
                value = sourceDn.toString();

            } else {
                SearchResult sourceEntry = partition.find(adminSession, sourceDn);
                Attributes sourceAttributes = sourceEntry.getAttributes();
                value = sourceAttributes.getValue(sourceAttribute);
            }

            SimpleFilter filter = new SimpleFilter(targetAttribute, "=", value);

            SearchRequest request = new SearchRequest();
            request.setDn(targetSuffix);
            request.setFilter(filter);

            SearchResponse response = new SearchResponse();

            targetPartition.search(adminSession, request, response);

            Collection<DN> results = new ArrayList<DN>();

            while (response.hasNext()) {
                SearchResult result = response.next();
                DN dn = result.getDn();
                results.add(dn);
            }

            return results;

        } finally {
            adminSession.close();
        }
    }

    public DN importEntry(DN sourceDn) throws Exception {

        Session adminSession = createAdminSession();

        Attributes sourceAttributes = null;

        DN targetDn = null;
        Attributes targetAttributes = null;

        try {
            Partition targetPartition = partition.getPartitionContext().getPartition(targetPartitionName);

            DN sourceSuffix = partition.getDirectory().getSuffix();
            DN targetSuffix = targetPartition.getDirectory().getSuffix();

            log.debug("##################################################################################################");
            log.debug("Import "+ sourceDn);

            SearchResult sourceEntry = partition.find(adminSession, sourceDn);
            sourceAttributes = sourceEntry.getAttributes();

            targetDn = sourceEntry.getDn().getPrefix(sourceSuffix).append(targetSuffix);

            if (mappingName == null) {
                targetAttributes = (Attributes)sourceAttributes.clone();

            } else {
                MappingManager mappingManager = partition.getMappingManager();
                Mapping mapping = mappingManager.getMapping(mappingName);

                targetAttributes = mapping.map(mappingPrefix, sourceAttributes);
            }

            Object value;
            if (sourceAttribute == null || sourceAttribute.equals("dn")) {
                value = sourceEntry.getDn().toString();

            } else {
                value = sourceEntry.getAttributes().getValue(sourceAttribute);
            }

            targetAttributes.addValue(targetAttribute, value);

            AddRequest request = new AddRequest();
            request.setDn(targetDn);
            request.setAttributes(targetAttributes);

            AddResponse response = new AddResponse();

            targetPartition.add(adminSession, request, response);

            return targetDn;

        } catch (LDAPException e) {
            LinkingException le = new LinkingException(e);
            le.setSourceDn(sourceDn);
            le.setSourceAttributes(sourceAttributes);
            le.setTargetDn(targetDn);
            le.setTargetAttributes(targetAttributes);
            le.setReason(e.getLDAPErrorMessage());
            throw le;

        } finally {
            adminSession.close();
        }
    }

    public void addEntry(DN targetDn, Attributes targetAttributes) throws Exception {

        Session adminSession = createAdminSession();

        try {
            Partition targetPartition = partition.getPartitionContext().getPartition(targetPartitionName);

            log.debug("##################################################################################################");
            log.debug("Add "+ targetDn);

            AddRequest request = new AddRequest();
            request.setDn(targetDn);
            request.setAttributes(targetAttributes);

            AddResponse response = new AddResponse();

            targetPartition.add(adminSession, request, response);

        } finally {
            adminSession.close();
        }
    }

    public void deleteEntry(DN targetDn) throws Exception {

        Session adminSession = createAdminSession();

        try {
            Partition targetPartition = partition.getPartitionContext().getPartition(targetPartitionName);

            log.debug("##################################################################################################");
            log.debug("Delete "+ targetDn);

            DeleteRequest request = new DeleteRequest();
            request.setDn(targetDn);

            DeleteResponse response = new DeleteResponse();

            targetPartition.delete(adminSession, request, response);

        } finally {
            adminSession.close();
        }
    }
}