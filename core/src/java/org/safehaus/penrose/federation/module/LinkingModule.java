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
import org.safehaus.penrose.ldap.source.LDAPSource;
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
import java.util.Map;
import java.util.HashMap;

/**
 * @author Endi Sukma Dewata
 */
public class LinkingModule extends Module implements LinkingMBean {

    public final static String SOURCE = "source";
    public final static String TARGET = "target";
    public final static String BOTH   = "both";

    protected String sourcePartitionName;
    protected String sourceName;

    protected String targetPartitionName;
    protected String targetName;

    protected String sourceAttribute;
    protected String targetAttribute;

    protected String sourceKey;
    protected String targetKey;

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
        log.debug("Source attribute: "+sourceAttribute);

        targetAttribute = getParameter("targetAttribute");
        log.debug("Target attribute: "+targetAttribute);

        sourceKey = getParameter("sourceKey");
        if (sourceKey == null) sourceKey = "dn";
        log.debug("Source key: "+sourceKey);

        targetKey = getParameter("targetKey");
        if (targetKey == null) targetKey = "dn";
        log.debug("Target key: "+targetKey);

        mappingName = getParameter("mapping");
        log.debug("Mapping name: "+mappingName);

        mappingPrefix = getParameter("mappingPrefix");
        log.debug("Mapping prefix: "+mappingPrefix);
    }

    public void linkEntry(DN sourceDn, DN targetDn) throws Exception {

        Session session = createAdminSession();

        try {
            Source source = getSource();
            Source target = getTarget();

            log.debug("##################################################################################################");
            log.debug("Link "+sourceDn+" to "+targetDn);

            if (sourceAttribute != null) {
                linkEntry(session, targetDn, sourceDn, targetKey, sourceAttribute, target, source);
            }

            if (targetAttribute != null) {
                linkEntry(session, sourceDn, targetDn, sourceKey, targetAttribute, source, target);
            }

        } finally {
            session.close();
        }
    }

    public void linkEntry(
            Session session,
            DN sourceDn,
            DN targetDn,
            String sourceKey,
            String targetAttribute,
            Source source,
            Source target
    ) throws Exception {

        Object value;
        if (sourceKey == null || sourceKey.equals("dn")) {
            value = sourceDn.toString();

        } else {
            SearchResult entry = source.find(session, sourceDn);
            Attributes attributes = entry.getAttributes();
            value = attributes.getValue(sourceKey);
        }

        Collection<Modification> modifications = new ArrayList<Modification>();
        modifications.add(new Modification(Modification.ADD, new Attribute(targetAttribute, value)));

        ModifyRequest request = new ModifyRequest();
        request.setDn(targetDn);
        request.setModifications(modifications);

        ModifyResponse response = new ModifyResponse();

        try {
            target.modify(session, request, response);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void unlinkEntry(DN sourceDn, DN targetDn) throws Exception {

        Session session = createAdminSession();

        try {
            Source source = getSource();
            Source target = getTarget();

            log.debug("##################################################################################################");
            log.debug("Unlink "+sourceDn+" from "+targetDn);

            if (sourceAttribute != null) {
                unlinkEntry(session, targetDn, sourceDn, targetKey, sourceAttribute, target, source);
            }

            if (targetAttribute != null) {
                unlinkEntry(session, sourceDn, targetDn, sourceKey, targetAttribute, source, target);
            }

        } finally {
            session.close();
        }
    }

    public void unlinkEntry(
            Session session,
            DN sourceDn,
            DN targetDn,
            String sourceKey,
            String targetAttribute,
            Source source,
            Source target
    ) throws Exception {

        Object value;
        if (sourceKey == null || sourceKey.equals("dn")) {
            value = sourceDn.toString();

        } else {
            SearchResult entry = source.find(session, sourceDn);
            Attributes attributes = entry.getAttributes();
            value = attributes.getValue(sourceKey);
        }

        Collection<Modification> modifications = new ArrayList<Modification>();
        modifications.add(new Modification(Modification.DELETE, new Attribute(targetAttribute, value)));

        ModifyRequest request = new ModifyRequest();
        request.setDn(targetDn);
        request.setModifications(modifications);

        ModifyResponse response = new ModifyResponse();

        try {
            target.modify(session, request, response);
            
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public Collection<LinkingData> search(SearchRequest request) throws Exception {

        Session session = createAdminSession();

        try {
            Source source = getSource();
            Source target = getTarget();

            DN targetSuffix = getTargetSuffix();

            log.debug("##################################################################################################");
            log.debug("Search "+request.getDn()+".");

            Collection<LinkingData> results = new ArrayList<LinkingData>();

            SearchResponse response = new SearchResponse();

            source.search(session, request, response);

            while (response.hasNext()) {
                SearchResult localEntry = response.next();

                LinkingData data = new LinkingData(localEntry);

                if (sourceAttribute != null) {
                    data.setLocalAttribute(sourceAttribute);

                    Attributes sourceAttributes = localEntry.getAttributes();
                    Collection<Object> links = sourceAttributes.getValues(sourceAttribute);

                    for (Object link : links) {
                        SearchRequest globalRequest = new SearchRequest();

                        if (targetKey == null || targetKey.equals("dn")) {
                            globalRequest.setDn(link.toString());
                            globalRequest.setScope(SearchRequest.SCOPE_BASE);
                        } else {
                            globalRequest.setDn(targetSuffix);
                            globalRequest.setFilter(new SimpleFilter(targetKey, "=", link));
                        }

                        SearchResponse globalResponse = new SearchResponse();

                        try {
                            target.search(session, globalRequest, globalResponse);

                            while (globalResponse.hasNext()) {
                                SearchResult globalEntry = globalResponse.next();
                                data.addLinkedEntry(globalEntry);
                            }
                        } catch (Exception e) {
                            log.error("Unable to find "+targetKey+"="+link);
                        }
                    }
                }

                if (targetAttribute != null) {
                    data.setGlobalAttribute(targetAttribute);

                    Collection<Object> targetLinks;

                    if (sourceKey == null || sourceKey.equals("dn")) {
                        targetLinks = new ArrayList<Object>();
                        targetLinks.add(localEntry.getDn().toString());

                    } else {
                        Attributes sourceAttributes = localEntry.getAttributes();
                        targetLinks = sourceAttributes.getValues(sourceKey);
                    }

                    for (Object link : targetLinks) {
                        SearchRequest globalRequest = new SearchRequest();
                        globalRequest.setDn(targetSuffix);
                        globalRequest.setFilter(new SimpleFilter(targetAttribute, "=", link));

                        SearchResponse globalResponse = new SearchResponse();

                        target.search(session, globalRequest, globalResponse);

                        while (globalResponse.hasNext()) {
                            SearchResult globalEntry = globalResponse.next();
                            data.addLinkedEntry(globalEntry);
                        }
                    }
                }

                results.add(data);
            }

            return results;

        } finally {
            session.close();
        }
    }

    public Collection<SearchResult> searchLinks(SearchResult sourceEntry) throws Exception {

        Session adminSession = createAdminSession();

        try {
            LDAPSource target = getTarget();

            DN sourceDn = sourceEntry.getDn();

            log.debug("##################################################################################################");
            log.debug("Search links for "+sourceDn);

            Map<DN,SearchResult> map = new HashMap<DN,SearchResult>();

            if (sourceAttribute != null) {

                Attributes sourceAttributes = sourceEntry.getAttributes();
                Collection<Object> links = sourceAttributes.getValues(sourceAttribute);

                if (targetKey == null || targetKey.equals("dn")) {
                    for (Object link : links) {
                        String dn = (String)link;
                        SearchResult result = target.find(adminSession, dn);
                        map.put(result.getDn(), result);
                    }

                } else {
                    Filter filter = null;
                    for (Object link : links) {
                        SimpleFilter sf = new SimpleFilter(targetKey, "=", link);
                        filter = FilterTool.appendOrFilter(filter, sf);
                    }

                    SearchRequest request = new SearchRequest();
                    request.setFilter(filter);

                    SearchResponse response = new SearchResponse();

                    target.search(adminSession, request, response);

                    while (response.hasNext()) {
                        SearchResult result = response.next();
                        map.put(result.getDn(), result);
                    }
                }
            }

            if (targetAttribute != null) {

                Collection<Object> links;

                if (sourceKey == null || sourceKey.equals("dn")) {
                    links = new ArrayList<Object>();
                    links.add(sourceDn);

                } else {
                    Attributes sourceAttributes = sourceEntry.getAttributes();
                    links = sourceAttributes.getValues(sourceKey);
                }

                Filter filter = null;
                for (Object link : links) {
                    SimpleFilter sf = new SimpleFilter(targetAttribute, "=", link);
                    filter = FilterTool.appendOrFilter(filter, sf);
                }

                SearchRequest request = new SearchRequest();
                request.setFilter(filter);

                SearchResponse response = new SearchResponse();

                target.search(adminSession, request, response);

                while (response.hasNext()) {
                    SearchResult result = response.next();
                    map.put(result.getDn(), result);
                }
            }

            Collection<SearchResult> results = new ArrayList<SearchResult>();
            results.addAll(map.values());

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
            Source source = getSource();
            Source target = getTarget();

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

            if (targetAttribute != null) {

                log.debug("Creating link on target.");

                Object value;
                if (sourceKey == null || sourceKey.equals("dn")) {
                    value = sourceDn.toString();

                } else {
                    value = sourceAttributes.getValue(sourceKey);
                }

                targetAttributes.addValue(targetAttribute, value);
            }

            AddRequest request = new AddRequest();
            request.setDn(targetDn);
            request.setAttributes(targetAttributes);

            AddResponse response = new AddResponse();

            target.add(adminSession, request, response);

            if (sourceAttribute != null) {
                try {
                    log.debug("Creating link on source.");

                    Object value;
                    if (targetKey == null || targetKey.equals("dn")) {
                        value = targetDn.toString();

                    } else {
                        value = targetAttributes.getValue(targetKey);
                    }

                    Modification modification = new Modification(
                            Modification.ADD,
                            new Attribute(sourceAttribute, value)
                    );

                    ModifyRequest modifyRequest = new ModifyRequest();
                    modifyRequest.setDn(sourceDn);
                    modifyRequest.addModification(modification);

                    ModifyResponse modifyResponse = new ModifyResponse();

                    source.modify(adminSession, modifyRequest, modifyResponse);

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
            Source target = getTarget();
            Source source = getSource();

            log.debug("##################################################################################################");
            log.debug("Import "+ sourceDn);

            SearchResult sourceEntry = source.find(adminSession, sourceDn);
            sourceAttributes = sourceEntry.getAttributes();

            targetDn = targetEntry.getDn();
            targetAttributes = targetEntry.getAttributes();

            if (targetAttribute != null) {

                log.debug("Creating link on target.");

                Object value;
                if (sourceKey == null || sourceKey.equals("dn")) {
                    value = sourceDn.toString();

                } else {
                    value = sourceAttributes.getValue(sourceKey);
                }

                targetAttributes.addValue(targetAttribute, value);
            }

            AddRequest request = new AddRequest();
            request.setDn(targetDn);
            request.setAttributes(targetAttributes);

            AddResponse response = new AddResponse();

            target.add(adminSession, request, response);

            if (sourceAttribute != null) {
                try {
                    log.debug("Creating link on source.");

                    Object value;
                    if (targetKey == null || targetKey.equals("dn")) {
                        value = targetDn.toString();

                    } else {
                        value = targetAttributes.getValue(targetKey);
                    }

                    Modification modification = new Modification(
                            Modification.ADD,
                            new Attribute(sourceAttribute, value)
                    );

                    ModifyRequest modifyRequest = new ModifyRequest();
                    modifyRequest.setDn(sourceDn);
                    modifyRequest.addModification(modification);

                    ModifyResponse modifyResponse = new ModifyResponse();

                    source.modify(adminSession, modifyRequest, modifyResponse);

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

    public void deleteEntry(DN sourceDn, DN targetDn) throws Exception {

        Session session = createAdminSession();

        try {
            Source source = getSource();
            Source target = getTarget();

            log.debug("##################################################################################################");
            log.debug("Delete "+ targetDn);

            if (sourceAttribute != null) {
                unlinkEntry(session, targetDn, sourceDn, targetKey, sourceAttribute, target, source);
            }

            DeleteRequest request = new DeleteRequest();
            request.setDn(targetDn);

            DeleteResponse response = new DeleteResponse();

            target.delete(session, request, response);

        } finally {
            session.close();
        }
    }

    public LDAPSource getSource() throws Exception {
        Partition sourcePartition = getPartition(sourcePartitionName);
        return (LDAPSource)sourcePartition.getSourceManager().getSource(sourceName);
    }

    public LDAPSource getTarget() throws Exception {
        Partition targetPartition = getPartition(targetPartitionName);
        return (LDAPSource)targetPartition.getSourceManager().getSource(targetName);
    }

    public DN getSourceSuffix() throws Exception {
        LDAPSource source = getSource();
        return source.getBaseDn();
    }

    public DN getTargetSuffix() throws Exception {
        LDAPSource target = getTarget();
        return target.getBaseDn();
    }
}