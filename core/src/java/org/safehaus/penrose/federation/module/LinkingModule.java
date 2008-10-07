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
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.mapping.Mapping;
import org.safehaus.penrose.mapping.MappingManager;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.federation.LinkingData;
import org.safehaus.penrose.federation.LinkingMBean;
import org.safehaus.penrose.source.Source;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class LinkingModule extends Module implements LinkingMBean {

    public final static String SOURCE = "source";
    public final static String TARGET = "target";

    protected String sourcePartitionName;
    protected String sourceName;

    protected String targetPartitionName;
    protected String targetName;

    protected String sourceAttribute;
    protected String targetAttribute;
    protected String storage;

    protected String mappingName;
    protected String mappingPrefix;

    public void init() throws Exception {

        String s = getParameter("source");
        int i = s.indexOf('.');

        if (i < 0) {
            sourcePartitionName = getPartition().getName();
            sourceName = s;
        } else {
            sourcePartitionName = s.substring(0, i);
            sourceName = s.substring(i+1);
        }

        log.debug("Source partition: "+sourcePartitionName);
        log.debug("Source: "+sourceName);

        s = getParameter("target");
        i = s.indexOf('.');

        if (i < 0) {
            targetPartitionName = getPartition().getName();
            targetName = s;
        } else {
            targetPartitionName = s.substring(0, i);
            targetName = s.substring(i+1);
        }

        log.debug("Target partition: "+targetPartitionName);
        log.debug("Target: "+targetName);

        sourceAttribute = getParameter("sourceAttribute");
        if (sourceAttribute == null) sourceAttribute = "dn";
        log.debug("Source attribute: "+sourceAttribute);

        targetAttribute = getParameter("targetAttribute");
        if (targetAttribute == null) targetAttribute = "seeAlso";
        log.debug("Target attribute: "+targetAttribute);

        storage = getParameter("storage");
        if (storage == null) storage = SOURCE;
        log.debug("Storage: "+storage);

        mappingName = getParameter("mapping");
        log.debug("Mapping name: "+mappingName);

        mappingPrefix = getParameter("mappingPrefix");
        log.debug("Mapping prefix: "+mappingPrefix);
    }

    public void linkEntry(DN sourceDn, DN targetDn) throws Exception {

        Session adminSession = createAdminSession();

        try {
            Source source = getSource();
            Source target = getTarget();

            log.debug("##################################################################################################");
            log.debug("Link "+sourceDn+" to "+targetDn);

            String sa, ta;
            DN sdn, tdn;
            Source s, t;

            if (storage.equals(SOURCE)) {
                sa = targetAttribute;
                ta = sourceAttribute;
                sdn = targetDn;
                tdn = sourceDn;
                s = target;
                t = source;

            } else {
                sa = sourceAttribute;
                ta = targetAttribute;
                sdn = sourceDn;
                tdn = targetDn;
                s = source;
                t = target;
            }

            Object value;
            if (sa == null || sa.equals("dn")) {
                value = sdn.toString();

            } else {
                SearchResult entry = s.find(adminSession, sdn);
                Attributes attributes = entry.getAttributes();
                value = attributes.getValue(sa);
            }

            Collection<Modification> modifications = new ArrayList<Modification>();
            modifications.add(new Modification(Modification.ADD, new Attribute(ta, value)));

            ModifyRequest request = new ModifyRequest();
            request.setDn(tdn);
            request.setModifications(modifications);

            ModifyResponse response = new ModifyResponse();

            t.modify(adminSession, request, response);

        } finally {
            adminSession.close();
        }
    }

    public void unlinkEntry(DN sourceDn, DN targetDn) throws Exception {

        Session adminSession = createAdminSession();

        try {
            Source source = getSource();
            Source target = getTarget();

            log.debug("##################################################################################################");
            log.debug("Unlink "+sourceDn+" from "+targetDn);

            String sa, ta;
            DN sdn, tdn;
            Source s, t;

            if (storage.equals(SOURCE)) {
                sa = targetAttribute;
                ta = sourceAttribute;
                sdn = targetDn;
                tdn = sourceDn;
                s = target;
                t = source;

            } else {
                sa = sourceAttribute;
                ta = targetAttribute;
                sdn = sourceDn;
                tdn = targetDn;
                s = source;
                t = target;
            }

            Object value;
            if (sa == null || sa.equals("dn")) {
                value = sdn.toString();

            } else {
                SearchResult entry = s.find(adminSession, sdn);
                Attributes attributes = entry.getAttributes();
                value = attributes.getValue(sa);
            }

            Collection<Modification> modifications = new ArrayList<Modification>();
            modifications.add(new Modification(Modification.DELETE, new Attribute(ta, value)));

            ModifyRequest request = new ModifyRequest();
            request.setDn(tdn);
            request.setModifications(modifications);

            ModifyResponse response = new ModifyResponse();

            t.modify(adminSession, request, response);

        } finally {
            adminSession.close();
        }
    }

    public Collection<LinkingData> search(SearchRequest request) throws Exception {

        Session adminSession = createAdminSession();

        try {
            Source source = getSource();
            Source target = getTarget();

            DN targetSuffix = getTargetSuffix();

            log.debug("##################################################################################################");
            log.debug("Search "+request.getDn()+".");

            Collection<LinkingData> results = new ArrayList<LinkingData>();

            SearchResponse response = new SearchResponse();

            source.search(adminSession, request, response);

            while (response.hasNext()) {
                SearchResult localEntry = response.next();

                LinkingData data = new LinkingData(localEntry);
                data.setStorage(SOURCE.equals(storage) ? LinkingData.LOCAL_STORAGE : LinkingData.GLOBAL_STORAGE);
                data.setLocalAttribute(sourceAttribute);
                data.setGlobalAttribute(targetAttribute);

                Collection<Object> links = new ArrayList<Object>();

                if (sourceAttribute == null || sourceAttribute.equals("dn")) {
                    links.add(localEntry.getDn().toString());

                } else {
                    Attributes sourceAttributes = localEntry.getAttributes();
                    links.addAll(sourceAttributes.getValues(sourceAttribute));
                }

                for (Object link : links) {
                    SearchRequest globalRequest = new SearchRequest();

                    if (targetAttribute == null || targetAttribute.equals("dn")) {
                        globalRequest.setDn(link.toString());
                        globalRequest.setScope(SearchRequest.SCOPE_BASE);
                    } else {
                        globalRequest.setDn(targetSuffix);
                        globalRequest.setFilter(new SimpleFilter(targetAttribute, "=", link));
                    }

                    SearchResponse globalResponse = new SearchResponse();

                    target.search(adminSession, globalRequest, globalResponse);

                    if (!globalResponse.hasNext()) continue;

                    while (globalResponse.hasNext()) {
                        SearchResult globalEntry = globalResponse.next();
                        data.addLinkedEntry(globalEntry);
                    }
                }

                results.add(data);
            }

            return results;

        } finally {
            adminSession.close();
        }
    }

    public Collection<SearchResult> searchLinks(SearchResult sourceEntry) throws Exception {

        Session adminSession = createAdminSession();

        try {
            Source target = getTarget();

            DN sourceDn = sourceEntry.getDn();

            log.debug("##################################################################################################");
            log.debug("Search links for "+sourceDn);

            Collection<Object> links;

            if (sourceAttribute == null || sourceAttribute.equals("dn")) {
                links = new ArrayList<Object>();
                links.add(sourceDn);

            } else {
                Attributes sourceAttributes = sourceEntry.getAttributes();
                links = sourceAttributes.getValues(sourceAttribute);
            }

            Collection<SearchResult> results = new ArrayList<SearchResult>();

            if (targetAttribute == null || targetAttribute.equals("dn")) {
                for (Object link : links) {
                    DN dn;
                    if (link instanceof DN) {
                        dn = (DN)link;
                    } else {
                        dn = new DN(link.toString());
                    }
                    SearchResult result = target.find(adminSession, dn);
                    results.add(result);
                }

            } else {
                SearchRequest request = new SearchRequest();
                request.setAttributes(new String[] { "dn" });

                DN targetSuffix = new DN(target.getParameter("baseDn"));
                request.setDn(targetSuffix);

                Filter filter = null;
                for (Object link : links) {
                    SimpleFilter sf = new SimpleFilter(targetAttribute, "=", link);
                    filter = FilterTool.appendOrFilter(filter, sf);
                }
                request.setFilter(filter);

                SearchResponse response = new SearchResponse();

                target.search(adminSession, request, response);

                while (response.hasNext()) {
                    SearchResult result = response.next();
                    results.add(result);
                }
            }

            return results;

        } finally {
            adminSession.close();
        }
    }

    public SearchResult importEntry(SearchResult sourceEntry) throws Exception {

        Session adminSession = createAdminSession();

        DN sourceDn = sourceEntry.getDn();
        Attributes sourceAttributes = null;

        DN targetDn = null;
        Attributes targetAttributes = null;

        try {
            DN sourceSuffix = getSourceSuffix();
            DN targetSuffix = getTargetSuffix();

            log.debug("##################################################################################################");
            log.debug("Import "+ sourceDn+".");

            sourceAttributes = sourceEntry.getAttributes();

            targetDn = sourceEntry.getDn().getPrefix(sourceSuffix).append(targetSuffix);

            if (mappingName == null) {
                targetAttributes = (Attributes)sourceAttributes.clone();

            } else {
                MappingManager mappingManager = partition.getMappingManager();
                Mapping mapping = mappingManager.getMapping(mappingName);

                targetAttributes = mapping.map(mappingPrefix, sourceAttributes);
            }

            if (!storage.equals(SOURCE)) {

                log.debug("Creating link on target.");

                Object value;
                if (sourceAttribute == null || sourceAttribute.equals("dn")) {
                    value = sourceDn.toString();

                } else {
                    value = sourceAttributes.getValue(sourceAttribute);
                }

                targetAttributes.addValue(targetAttribute, value);
            }

            AddRequest request = new AddRequest();
            request.setDn(targetDn);
            request.setAttributes(targetAttributes);

            AddResponse response = new AddResponse();

            Source target = getTarget();
            target.add(adminSession, request, response);

            if (storage.equals(SOURCE)) {
                try {
                    log.debug("Creating link on source.");

                    Object value;
                    if (targetAttribute == null || targetAttribute.equals("dn")) {
                        value = targetDn.toString();

                    } else {
                        value = targetAttributes.getValue(targetAttribute);
                    }

                    Modification modification = new Modification(
                            Modification.ADD,
                            new Attribute(sourceAttribute, value)
                    );

                    ModifyRequest modifyRequest = new ModifyRequest();
                    modifyRequest.setDn(sourceDn);
                    modifyRequest.addModification(modification);

                    ModifyResponse modifyResponse = new ModifyResponse();

                    partition.modify(adminSession, modifyRequest, modifyResponse);

                } catch (LDAPException e) {
                    log.error(e.getMessage(), e);
                    if (e.getResultCode() != LDAP.ATTRIBUTE_OR_VALUE_EXISTS) throw e;
                }
            }

            return new SearchResult(targetDn, targetAttributes);

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

    public SearchResult importEntry(DN sourceDn, SearchResult targetEntry) throws Exception {

        Session adminSession = createAdminSession();

        Attributes sourceAttributes = null;

        DN targetDn = null;
        Attributes targetAttributes = null;

        try {
            log.debug("##################################################################################################");
            log.debug("Import "+ sourceDn);

            Source source = getSource();
            SearchResult sourceEntry = source.find(adminSession, sourceDn);
            sourceAttributes = sourceEntry.getAttributes();

            targetDn = targetEntry.getDn();
            targetAttributes = targetEntry.getAttributes();

            if (!storage.equals(SOURCE)) {

                log.debug("Creating link on target.");

                Object value;
                if (sourceAttribute == null || sourceAttribute.equals("dn")) {
                    value = sourceDn.toString();

                } else {
                    value = sourceAttributes.getValue(sourceAttribute);
                }

                targetAttributes.addValue(targetAttribute, value);
            }

            AddRequest request = new AddRequest();
            request.setDn(targetDn);
            request.setAttributes(targetAttributes);

            AddResponse response = new AddResponse();

            Source target = getTarget();
            target.add(adminSession, request, response);

            if (storage.equals(SOURCE)) {
                try {
                    log.debug("Creating link on source.");

                    Object value;
                    if (targetAttribute == null || targetAttribute.equals("dn")) {
                        value = targetDn.toString();

                    } else {
                        value = targetAttributes.getValue(targetAttribute);
                    }

                    Modification modification = new Modification(
                            Modification.ADD,
                            new Attribute(sourceAttribute, value)
                    );

                    ModifyRequest modifyRequest = new ModifyRequest();
                    modifyRequest.setDn(sourceDn);
                    modifyRequest.addModification(modification);

                    ModifyResponse modifyResponse = new ModifyResponse();

                    partition.modify(adminSession, modifyRequest, modifyResponse);

                } catch (LDAPException e) {
                    log.error(e.getMessage(), e);
                    if (e.getResultCode() != LDAP.ATTRIBUTE_OR_VALUE_EXISTS) throw e;
                }
            }

            return new SearchResult(targetDn, targetAttributes);

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
            log.debug("##################################################################################################");
            log.debug("Add "+ targetDn);

            AddRequest request = new AddRequest();
            request.setDn(targetDn);
            request.setAttributes(targetAttributes);

            AddResponse response = new AddResponse();

            Source target = getTarget();
            target.add(adminSession, request, response);

        } finally {
            adminSession.close();
        }
    }

    public void deleteEntry(DN targetDn) throws Exception {

        Session adminSession = createAdminSession();

        try {
            log.debug("##################################################################################################");
            log.debug("Delete "+ targetDn);

            DeleteRequest request = new DeleteRequest();
            request.setDn(targetDn);

            DeleteResponse response = new DeleteResponse();

            Source target = getTarget();
            target.delete(adminSession, request, response);

        } finally {
            adminSession.close();
        }
    }

    public Source getSource() throws Exception {
        Partition sourcePartition = getPartition(sourcePartitionName);
        return sourcePartition.getSourceManager().getSource(sourceName);
    }

    public Source getTarget() throws Exception {
        Partition targetPartition = getPartition(targetPartitionName);
        return targetPartition.getSourceManager().getSource(targetName);
    }

    public DN getSourceSuffix() throws Exception {
        Source source = getSource();
        return new DN(source.getParameter("baseDn"));
    }

    public DN getTargetSuffix() throws Exception {
        Source target = getTarget();
        return new DN(target.getParameter("baseDn"));
    }
}