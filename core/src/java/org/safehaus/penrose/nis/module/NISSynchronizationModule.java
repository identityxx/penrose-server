package org.safehaus.penrose.nis.module;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.synchronization.module.SynchronizationModule;
import org.safehaus.penrose.synchronization.SynchronizationResult;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.nis.NISSynchronizationModuleMBean;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class NISSynchronizationModule extends SynchronizationModule implements NISSynchronizationModuleMBean {

    public static Map<String,String> nisMapRDNs = new LinkedHashMap<String,String>();

    public void init() throws Exception {
        super.init();

        for (String name : getParameterNames()) {
            if (!name.startsWith("nisMap.")) continue;

            String nisMap = name.substring(7);
            String rdn = getParameter(name);
            nisMapRDNs.put(nisMap, rdn);
        }
    }

    public Map<String,String> getNisMapRDNs() {
        return nisMapRDNs;
    }

    public Collection<String> getNisMaps() {
        Collection<String> list = new ArrayList<String>();
        list.addAll(nisMapRDNs.keySet());
        return list;
    }

    public String getNisMapRDN(String nisMap) {
        return nisMapRDNs.get(nisMap);
    }

    public void convertAutomount(Attributes attributes) throws Exception {

        Attribute attribute = attributes.get("nisMapEntry");
        if (attribute == null) return;

        DN sourceSuffix = getSourceSuffix();
        DN targetSuffix = getTargetSuffix();

        Collection<Object> removeList = new ArrayList<Object>();
        Collection<Object> addList = new ArrayList<Object>();

        for (Object object : attribute.getValues()) {
            String value = object.toString();
            if (!value.startsWith("ldap:")) continue;
            int i = value.indexOf(' ', 5);

            String name;
            String info;

            if (i < 0) {
                name = value.substring(5);
                info = null;
            } else {
                name = value.substring(5, i);
                info = value.substring(i+1);
            }

            DN dn = new DN(name);
            DN newDn = dn.getPrefix(sourceSuffix).append(targetSuffix);

            String newValue = "ldap:"+newDn+(info == null ? "" : " "+info);

            removeList.add(value);
            addList.add(newValue);
        }

        attribute.removeValues(removeList);
        attribute.addValues(addList);
    }

    public SynchronizationResult synchronize() throws Exception {

        SynchronizationResult totalResult = new SynchronizationResult();
        for (String nisMap : nisMapRDNs.keySet()) {
            SynchronizationResult r = synchronizeNISMap(nisMap);
            totalResult.add(r);
        }

        return totalResult;
    }

    public SynchronizationResult synchronizeNISMap(String nisMap) throws Exception {

        log.debug("Synchronizing NIS map "+nisMap+"...");

        String rdn = nisMapRDNs.get(nisMap);

        if (rdn == null) {
            log.debug("Unknown NIS map: "+nisMap);
            return new SynchronizationResult();
        }

        DN targetSuffix = getTargetSuffix();
        
        DNBuilder db = new DNBuilder();
        db.append(rdn);
        db.append(targetSuffix);
        DN targetDn = db.toDn();

        return synchronize(targetDn);
    }

    public SynchronizationResult synchronize(final DN targetDn) throws Exception {

        Session adminSession = createAdminSession();

        try {
            return synchronize(adminSession, targetDn);

        } finally {
            adminSession.close();
        }
    }

    public SynchronizationResult synchronize(final Session session, final DN targetDn) throws Exception {

        long startTime = System.currentTimeMillis();

        final boolean warn = log.isWarnEnabled();

        final SynchronizationResult result = new SynchronizationResult();

        final DN sourceSuffix = getSourceSuffix();
        final DN targetSuffix = getTargetSuffix();

        log.debug("##################################################################################################");
        log.warn("Synchronizing "+targetDn);

        final DN sourceDn = targetDn.getPrefix(targetSuffix).append(sourceSuffix);

        final Collection<String> dns = new LinkedHashSet<String>();

        SearchRequest targetRequest = new SearchRequest();
        targetRequest.setDn(targetDn);
        targetRequest.setAttributes(new String[] { "dn" });

        if (warn) log.warn("Searching existing entries: "+targetDn);

        SearchResponse targetResponse = new SearchResponse() {
            public void add(SearchResult result) throws Exception {

                DN dn = result.getDn();
                if (dn.equals(targetDn)) return;

                totalCount++;

                String normalizedDn = dn.getNormalizedDn();
                //if (warn) log.warn(" - "+normalizedDn);

                dns.add(normalizedDn);

                if (warn) {
                    if (totalCount % 100 == 0) log.warn("Found "+totalCount+" entries.");
                }
            }
        };

        final Source target = getTarget();

        try {
            target.search(session, targetRequest, targetResponse);

        } catch (LDAPException e) {
            int rc = e.getResultCode();
            if (rc == LDAP.CONNECT_ERROR) {
                throw LDAP.createException(rc, "Unable to access target server.");
            } else {
                throw e;
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }

        int rc1 = targetResponse.waitFor();
        if (warn) log.warn("Search completed. RC="+rc1+".");

        if (rc1 != LDAP.SUCCESS) {
            throw LDAP.createException(rc1, "Unable to access target server.");
        }

        long targetEntries = targetResponse.getTotalCount();
        if (warn) log.warn("Found "+targetEntries+" entries.");

        result.setTargetEntries(targetEntries);

        SearchRequest sourceRequest = new SearchRequest();
        sourceRequest.setDn(sourceDn);

        if (warn) log.warn("Searching new entries: "+sourceDn);

        SearchResponse sourceResponse = new SearchResponse() {
            public void add(SearchResult result2) throws Exception {

                DN dn2 = result2.getDn();
                if (dn2.equals(sourceDn)) return;

                totalCount++;

                DN dn1 = dn2.getPrefix(sourceSuffix).append(targetSuffix);
                String normalizedDn = dn1.getNormalizedDn();

                Attributes attributes2 = result2.getAttributes();
                convertAutomount(attributes2);

                if (dns.contains(normalizedDn)) {

                    SearchResult result1 = target.find(session, dn1);

                    Attributes attributes1 = result1.getAttributes();
                    if (!checkSearchResult(result1)) return;

                    Collection<Modification> modifications = createModifications(
                            attributes1,
                            attributes2
                    );

                    if (modifications.isEmpty()) {
                        //if (warn) log.warn("No changes, skipping "+normalizedDn+".");
                        result.incUnchangedEntries();

                    } else { // modify entry

                        //if (warn) log.warn("Modifying "+normalizedDn+".");

                        ModifyRequest request = new ModifyRequest();
                        request.setDn(dn1);
                        request.setModifications(modifications);

                        try {
                            execute(session, request);
                            result.incModifiedEntries();

                        } catch (Exception e) {
                            result.incFailedEntries();
                        }
                    }

                    dns.remove(normalizedDn);

                } else { // add entry

                    //if (warn) log.warn("Adding "+normalizedDn+".");

                    AddRequest request = new AddRequest();
                    request.setDn(dn1);
                    request.setAttributes(attributes2);

                    try {
                        execute(session, request);
                        result.incAddedEntries();

                    } catch (Exception e) {
                        result.incFailedEntries();
                    }
                }

                if (warn) {
                    if (totalCount % 100 == 0) log.warn("Processed "+totalCount+" entries.");
                }
            }
        };

        Source source = getSource();

        try {
            source.search(session, sourceRequest, sourceResponse);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return result;
        }

        int rc2 = sourceResponse.waitFor();
        if (warn) log.warn("Search completed. RC="+rc2+".");

        if (rc2 != LDAP.SUCCESS) {
            return result;
        }

        if (warn) log.warn("Found "+sourceResponse.getTotalCount()+" source entries.");
        result.setSourceEntries(sourceResponse.getTotalCount());

        for (String normalizedDn : dns) { // deleting entry

            List<DN> list = getDns(session, normalizedDn);

            //DeleteRequest request = new DeleteRequest();
            //request.setDn(normalizedDn);
            //if (!execute(request)) success[0] = false;

            for (int i=list.size()-1; i>=0; i--) {
                DN dn = list.get(i);

                //if (warn) log.warn("Deleting "+dn+".");

                DeleteRequest deleteRequest = new DeleteRequest();
                deleteRequest.setDn(dn);

                try {
                    execute(session, deleteRequest);
                    result.incDeletedEntries();

                } catch (Exception e) {
                    result.incFailedEntries();
                }
            }
        }

        long endTime = System.currentTimeMillis();
        result.setDuration(endTime - startTime);

        if (warn) {
            log.warn(result.toString());
        }

        return result;
    }
}
