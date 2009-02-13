package org.safehaus.penrose.synchronization.module;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.synchronization.SynchronizationMBean;
import org.safehaus.penrose.ldap.source.LDAPSource;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.synchronization.SynchronizationResult;
import org.ietf.ldap.LDAPException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class SynchronizationModule extends Module implements SynchronizationMBean {

    protected String sourcePartitionName;
    protected String sourceName;

    protected String targetPartitionName;
    protected String targetName;

    protected Source changes;
    protected Source errors;

    protected Collection<String> ignoredObjectClasses = new HashSet<String>();
    protected Collection<String> ignoredAttributes = new HashSet<String>();

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

        String changesName = getParameter("changes");
        log.debug("Changes: "+changesName);

        String errorsName = getParameter("errors");
        log.debug("Errors: "+errorsName);

        SourceManager sourceManager = partition.getSourceManager();
        changes = sourceManager.getSource(changesName);
        errors = sourceManager.getSource(errorsName);

        s = getParameter("ignoredObjectClasses");
        log.debug("Ignored object classes: "+s);
        if (s != null) {
            StringTokenizer st = new StringTokenizer(s, ", \t\n\r\f");
            while (st.hasMoreTokens()) {
                String ignoredObjectClass = st.nextToken().toLowerCase();
                ignoredObjectClasses.add(ignoredObjectClass);
            }
        }

        s = getParameter("ignoredAttributes");
        log.debug("Ignored attributes: "+s);
        if (s != null) {
            StringTokenizer st = new StringTokenizer(s, ", \t\n\r\f");
            while (st.hasMoreTokens()) {
                String ignoredAttribute = st.nextToken().toLowerCase();
                ignoredAttributes.add(ignoredAttribute);
            }
        }
    }

    public void execute(Session session, AddRequest request) throws Exception {

        try {
            AddResponse response = new AddResponse();

            Source target = getTarget();
            target.add(session, request, response);

            if (changes != null) {
                Attributes attrs = new Attributes();
                attrs.setValue("time", new Timestamp(System.currentTimeMillis()));
                attrs.setValue("title", "Added "+request.getDn());

                StringBuilder sb = new StringBuilder();
                sb.append("The following operation succeeded:\n\n");

                sb.append(request);
                sb.append("\n");

                attrs.setValue("description", sb.toString());

                changes.add(session, new DN(), attrs);
            }

        } catch (Throwable e) {

            if (errors != null) {

                Attributes attrs = new Attributes();
                attrs.setValue("time", new Timestamp(System.currentTimeMillis()));
                attrs.setValue("title", "Error adding "+request.getDn());

                StringBuilder sb = new StringBuilder();
                sb.append("The following operation failed:\n\n");

                sb.append(request);
                sb.append("\n");

                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw, true));

                sb.append("Exception:\n");
                sb.append(sw);

                attrs.setValue("description", sb.toString());

                errors.add(session, new DN(), attrs);
            }

            throw new Exception(e);
        }
    }

    public void execute(Session session, DeleteRequest request) throws Exception {
        try {
            DeleteResponse response = new DeleteResponse();

            Source target = getTarget();
            target.delete(session, request, response);

            if (changes != null) {
                Attributes attrs = new Attributes();
                attrs.setValue("time", new Timestamp(System.currentTimeMillis()));
                attrs.setValue("title", "Deleted "+request.getDn());

                StringBuilder sb = new StringBuilder();
                sb.append("The following operation succeeded:\n\n");

                sb.append(request);
                sb.append("\n");

                attrs.setValue("description", sb.toString());

                changes.add(session, new DN(), attrs);
            }

        } catch (Throwable e) {

            if (errors != null) {

                Attributes attrs = new Attributes();
                attrs.setValue("time", new Timestamp(System.currentTimeMillis()));
                attrs.setValue("title", "Error deleting "+request.getDn());

                StringBuilder sb = new StringBuilder();
                sb.append("The following operation failed:\n\n");

                sb.append(request);
                sb.append("\n");

                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw, true));

                sb.append("Exception:\n");
                sb.append(sw);

                attrs.setValue("description", sb.toString());

                errors.add(session, new DN(), attrs);
            }

            throw new Exception(e);
        }
    }

    public void execute(Session session, ModifyRequest request) throws Exception {

        try {
            ModifyResponse response = new ModifyResponse();

            Source target = getTarget();
            target.modify(session, request, response);

            if (changes != null) {
                Attributes attrs = new Attributes();
                attrs.setValue("time", new Timestamp(System.currentTimeMillis()));
                attrs.setValue("title", "Modified "+request.getDn());

                StringBuilder sb = new StringBuilder();
                sb.append("The following operation succeeded:\n\n");

                sb.append(request);
                sb.append("\n");

                attrs.setValue("description", sb.toString());

                changes.add(session, new DN(), attrs);
            }

        } catch (Throwable e) {

            if (errors != null) {

                Attributes attrs = new Attributes();
                attrs.setValue("time", new Timestamp(System.currentTimeMillis()));
                attrs.setValue("title", "Error modifying "+request.getDn());

                StringBuilder sb = new StringBuilder();
                sb.append("The following operation failed:\n\n");

                sb.append(request);
                sb.append("\n");

                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw, true));

                sb.append("Exception:\n");
                sb.append(sw);

                attrs.setValue("description", sb.toString());

                errors.add(session, new DN(), attrs);
            }

            throw new Exception(e);
        }
    }

    public void deleteSubtree(Session session, String baseDn) throws Exception {
        deleteSubtree(session, new DN(baseDn));
    }

    public void deleteSubtree(Session session, final DN baseDn) throws Exception {

        final ArrayList<DN> dns = new ArrayList<DN>();

        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);

        SearchResponse response = new SearchResponse() {
            public void add(SearchResult result) throws Exception {
                DN dn = result.getDn();
                log.debug("Found "+dn);
                if (dn.equals(baseDn)) return;
                dns.add(dn);
            }
            public void close() throws Exception {
                super.close();
                long count = dns.size();
                log.debug("Found "+count+" entries.");
            }
        };

        Source target = getTarget();
        target.search(session, request, response);

        log.debug("Waiting for operation to complete.");
        int rc = response.waitFor();
        log.debug("RC: "+rc);

        for (int i=dns.size()-1; i>=0; i--) {
            DN dn = dns.get(i);

            //partition.delete(dn);

            DeleteRequest deleteRequest = new DeleteRequest();
            deleteRequest.setDn(dn);

            execute(session, deleteRequest);
        }
    }

    public boolean checkSearchResult(SearchResult result) throws Exception {

        Attributes attributes = result.getAttributes();
        Attribute objectClass = attributes.get("objectClass");

        if (objectClass != null && !ignoredObjectClasses.isEmpty()) {
            for (Object value : objectClass.getValues()) {
                String s = value.toString().toLowerCase();
                if (ignoredObjectClasses.contains(s)) {
                    if (warn) log.warn("Don't synchronize "+result.getDn()+".");
                    return false;
                }
            }
        }

        return true;
    }

    public List<DN> getDns(Session session, String baseDn) throws Exception {
        return getDns(session, new DN(baseDn));
    }

    public List<DN> getDns(Session session, final DN baseDn) throws Exception {

        //if (warn) log.warn("Getting DNs in subtree "+baseDn+".");

        final ArrayList<DN> dns = new ArrayList<DN>();

        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);

        SearchResponse response = new SearchResponse() {
            public void add(SearchResult result) throws Exception {
                DN dn = result.getDn();

                //log.debug("Found "+dn);

                if (!checkSearchResult(result)) return;

                dns.add(dn);

                totalCount++;

                //if (warn) {
                //    if (totalCount % 100 == 0) log.warn("Found "+totalCount+" entries.");
                //}
            }
        };

        Source target = getTarget();
        try {
            target.search(session, request, response);

            //log.debug("Waiting for operation to complete.");
            //int rc = response.waitFor();
            //log.debug("RC: "+rc);

        } catch (Exception e) {
            log.debug(e.getMessage());
        }

        //if (warn) log.warn("Found "+response.getTotalCount()+" entries.");

        return dns;
    }

    public void createBase() throws Exception {

        Session adminSession = createAdminSession();

        try {
            DN sourceSuffix = getSourceSuffix();
            DN targetSuffix = getTargetSuffix();

            log.debug("##################################################################################################");
            log.debug("Creating "+targetSuffix);

            Source source = getSource();
            SearchResult result = source.find(adminSession, sourceSuffix);
            Attributes attributes = result.getAttributes();

            AddRequest request = new AddRequest();
            request.setDn(targetSuffix);
            request.setAttributes(attributes);

            AddResponse response = new AddResponse();

            Source target = getTarget();
            target.add(adminSession, request, response);

        } finally {
            adminSession.close();
        }
    }

    public void removeBase() throws Exception {

        Session adminSession = createAdminSession();

        try {
            DN targetSuffix = getTargetSuffix();

            log.debug("##################################################################################################");
            log.debug("Creating "+targetSuffix);

            DeleteRequest request = new DeleteRequest();
            request.setDn(targetSuffix);

            DeleteResponse response = new DeleteResponse();

            Source target = getTarget();
            target.delete(adminSession, request, response);

        } finally {
            adminSession.close();
        }
    }

    public void create(String targetDn) throws Exception {
        create(new DN(targetDn));
    }

    public void create(DN targetDn) throws Exception {

        Session adminSession = createAdminSession();

        try {
            DN sourceSuffix = getSourceSuffix();
            DN targetSuffix = getTargetSuffix();

            log.debug("##################################################################################################");
            log.debug("Creating "+targetDn);

            DN sourceDn = targetDn.getPrefix(targetSuffix).append(sourceSuffix);

            SearchRequest request = new SearchRequest();
            request.setDn(sourceDn);

            SearchResponse response = new SearchResponse();

            Source source = getSource();
            source.search(adminSession, request, response);

            SearchResult result = response.next();
            Attributes attributes = result.getAttributes();

            AddRequest addRequest = new AddRequest();
            addRequest.setDn(targetDn);
            addRequest.setAttributes(attributes);

            AddResponse addResponse = new AddResponse();

            Source target = getTarget();
            target.add(adminSession, addRequest, addResponse);

        } finally {
            adminSession.close();
        }
    }

    public void load(String targetDn) throws Exception {
        load(new DN(targetDn));
    }

    public void load(final DN targetDn) throws Exception {

        final Session adminSession = createAdminSession();

        try {
            DN sourceSuffix = getSourceSuffix();
            DN targetSuffix = getTargetSuffix();

            log.debug("##################################################################################################");
            log.debug("Loading "+targetDn);

            final DN sourceDn = targetDn.getPrefix(targetSuffix).append(sourceSuffix);
            final int sourceDnSize = sourceDn.getSize();

            SearchRequest request = new SearchRequest();
            request.setDn(sourceDn);

            SearchResponse response = new SearchResponse() {
                public void add(SearchResult result) throws Exception {

                    DN dn = result.getDn();
                    if (sourceDn.equals(dn)) return;

                    int dnSize = dn.getSize();
                    DN newDn = dn.getPrefix(dnSize - sourceDnSize).append(targetDn);

                    Attributes attributes = result.getAttributes();

                    AddRequest request = new AddRequest();
                    request.setDn(newDn);
                    request.setAttributes(attributes);

                    execute(adminSession, request);
                }
            };

            Source source = getSource();
            source.search(adminSession, request, response);

            log.debug("Waiting for operation to complete.");
            int rc = response.waitFor();
            log.debug("RC: "+rc);

        } finally {
            adminSession.close();
        }
    }

    public void clear(String targetDn) throws Exception {
        clear(new DN(targetDn));
    }

    public void clear(DN targetDn) throws Exception {

        Session adminSession = createAdminSession();

        try {
            log.debug("##################################################################################################");
            log.debug("Clearing "+targetDn);

            deleteSubtree(adminSession, targetDn);

        } finally {
            adminSession.close();
        }
    }

    public void remove(String targetDn) throws Exception {
        remove(new DN(targetDn));
    }

    public void remove(DN targetDn) throws Exception {

        Session adminSession = createAdminSession();

        try {
            log.debug("##################################################################################################");
            log.debug("Removing "+targetDn);

            deleteSubtree(adminSession, targetDn);

            DeleteRequest request = new DeleteRequest();
            request.setDn(targetDn);

            DeleteResponse response = new DeleteResponse();

            Source target = getTarget();
            target.delete(adminSession, request, response);

        } finally {
            adminSession.close();
        }
    }

    public Long getSourceCount(String sourceDn) throws Exception {
        return getSourceCount(new DN(sourceDn));
    }

    public Long getSourceCount(final DN sourceDn) throws Exception {

        Session session = createAdminSession();

        try {
            SearchRequest request = new SearchRequest();
            request.setDn(sourceDn);
            request.setAttributes(new String[] { "dn" });
            request.setTypesOnly(true);

            SearchResponse response = new SearchResponse() {
                public void add(SearchResult result) throws Exception {

                    DN dn = result.getDn();
                    if (dn.equals(sourceDn)) return;

                    totalCount++;
                    if (warn && (totalCount % 100 == 0)) log.warn("Found "+totalCount+" entries.");
                }
                public void close() throws Exception {
                    if (warn && (totalCount % 100 != 0)) log.warn("Found "+totalCount+" entries.");
                    super.close();
                }
            };

            Source source = getSource();
            source.search(session, request, response);

            log.debug("Waiting for operation to complete.");
            int rc = response.waitFor();
            log.debug("RC: "+rc);

            if (rc != LDAP.SUCCESS) throw response.getException();

            return response.getTotalCount();

        } catch (LDAPException e) {
            log.debug(e.getMessage());
            return 0L;

        } finally {
            session.close();
        }
    }

    public Long getTargetCount(String targetDn) throws Exception {
        return getTargetCount(new DN(targetDn));
    }

    public Long getTargetCount(final DN targetDn) throws Exception {

        Session session = createAdminSession();

        try {
            SearchRequest request = new SearchRequest();
            request.setDn(targetDn);
            request.setAttributes(new String[] { "dn" });
            request.setTypesOnly(true);

            SearchResponse response = new SearchResponse() {
                public void add(SearchResult result) throws Exception {

                    DN dn = result.getDn();
                    if (dn.equals(targetDn)) return;

                    totalCount++;
                    if (warn && (totalCount % 100 == 0)) log.warn("Found "+totalCount+" entries.");
                }
                public void close() throws Exception {
                    if (warn && (totalCount % 100 != 0)) log.warn("Found "+totalCount+" entries.");
                    super.close();
                }
            };

            Source target = getTarget();
            target.search(session, request, response);

            log.debug("Waiting for operation to complete.");
            int rc = response.waitFor();
            log.debug("RC: "+rc);

            if (rc != LDAP.SUCCESS) throw response.getException();

            return response.getTotalCount();

        } finally {
            session.close();
        }
    }

    public SynchronizationResult synchronize() throws Exception {

        Session adminSession = createAdminSession();

        try {
            DN targetSuffix = getTargetSuffix();
            return synchronize(adminSession, targetSuffix);

        } finally {
            adminSession.close();
        }
    }

    public SynchronizationResult synchronize(String targetDn) throws Exception {
        return synchronize(new DN(targetDn));
    }

    public SynchronizationResult synchronize(final DN targetDn) throws Exception {

        Session adminSession = createAdminSession();

        try {
            return synchronize(adminSession, targetDn);

        } finally {
            adminSession.close();
        }
    }

    public Collection<Modification> createModifications(
            Attributes attributes1,
            Attributes attributes2
    ) throws Exception {

        Attributes attrs1 = (Attributes)attributes1.clone();
        attrs1.remove(ignoredAttributes);

        Attributes attrs2 = (Attributes)attributes2.clone();
        attrs2.remove(ignoredAttributes);

        return LDAP.createModifications(attrs1, attrs2);
    }

    public SynchronizationResult synchronize(final Session session, final DN targetDn) throws Exception {

        long startTime = System.currentTimeMillis();

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
        } catch (Exception e) {
            log.info("Message: "+e.getMessage());
        }

        int rc1 = targetResponse.waitFor();
        if (warn) log.warn("Search completed. RC="+rc1+".");

        long targetEntries = targetResponse.getTotalCount();
        if (warn) log.warn("Found "+targetEntries+" entries.");

        final SynchronizationResult result = new SynchronizationResult();
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

                if (dns.contains(normalizedDn)) {

                    SearchResult result1 = target.find(session, dn1);

                    Attributes attributes1 = result1.getAttributes();
                    if (!checkSearchResult(result1)) return;

                    Attributes attributes2 = result2.getAttributes();

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
                    request.setAttributes(result2.getAttributes());

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
        source.search(session, sourceRequest, sourceResponse);

        int rc2 = sourceResponse.waitFor();
        if (warn) log.warn("Search completed. RC="+rc2+".");

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

/*
    public boolean synchronize(final DN targetDn) throws Exception {

        final Partition sourcePartition = getSourcePartition();
        final Partition targetPartition = getTargetPartition();

        final DN sourceSuffix = sourcePartition.getDirectory().getSuffix();
        final DN targetSuffix = targetPartition.getDirectory().getSuffix();

        log.debug("##################################################################################################");
        log.debug("Synchronizing "+targetDn);

        final DN sourceDn = targetDn.getPrefix(targetSuffix).append(sourceSuffix);

        final Collection<DN> dns1 = new TreeSet<DN>();
        final Map<DN,DN> dns2 = new TreeMap<DN,DN>();

        SearchRequest request1 = new SearchRequest();
        request1.setDn(targetDn);
        request1.setAttributes(new String[] { "dn" });

        SearchResponse response1 = new SearchResponse() {
            public void add(SearchResult result) throws Exception {
                DN dn = result.getDn();
                if (dn.equals(targetDn)) return;
                dns1.add(dn);
            }
        };

        targetPartition.search(request1, response1);

        SearchRequest request2 = new SearchRequest();
        request2.setDn(sourceDn);
        request2.setAttributes(new String[] { "dn" });

        SearchResponse response2 = new SearchResponse() {
            public void add(SearchResult result) throws Exception {
                DN dn = result.getDn();
                if (dn.equals(sourceDn)) return;
                DN newDn = dn.getPrefix(sourceSuffix).append(targetSuffix);
                dns2.put(newDn, dn);
            }
        };

        sourcePartition.search(request2, response2);

        log.debug("Waiting for operation to complete.");
        int rc1 = response1.getReturnCode();
        log.debug("RC: "+rc1);

        log.debug("Waiting for operation to complete.");
        int rc2 = response2.getReturnCode();
        log.debug("RC: "+rc2);

        Iterator<DN> i1 = dns1.iterator();
        Iterator<DN> i2 = dns2.keySet().iterator();

        boolean b1 = i1.hasNext();
        boolean b2 = i2.hasNext();

        DN dn1 = b1 ? i1.next() : null;
        DN dn2 = b2 ? i2.next() : null;

        boolean success = true;

        while (b1 && b2) {

            if (debug) log.debug("Comparing ["+dn1+"] with ["+dn2+"]");

            int c = dn1.compareTo(dn2);

            if (c < 0) { // delete old entry
                DeleteRequest request = new DeleteRequest();
                request.setDn(dn1);
                if (!execute(request)) success = false;

                b1 = i1.hasNext();
                if (b1) dn1 = i1.next();

            } else if (c > 0) { // add new entry

                DN origDn = dns2.get(dn2);
                SearchResult result = sourcePartition.find(origDn);

                AddRequest request = new AddRequest();
                request.setDn(dn2);
                request.setAttributes(result.getAttributes());
                if (!execute(request)) success = false;

                b2 = i2.hasNext();
                if (b2) dn2 = i2.next();

            } else {

                SearchResult result1 = partition.find(dn1);

                DN origDn = dns2.get(dn2);
                SearchResult result2 = sourcePartition.find(origDn);

                Collection<Modification> modifications = createModifications(
                        result1.getAttributes(),
                        result2.getAttributes()
                );

                if (!modifications.isEmpty()) { // modify entry
                    ModifyRequest request = new ModifyRequest();
                    request.setDn(dn1);
                    request.setModifications(modifications);
                    if (!execute(request)) success = false;
                }

                b1 = i1.hasNext();
                if (b1) dn1 = i1.next();

                b2 = i2.hasNext();
                if (b2) dn2 = i2.next();
            }
        }

        while (b1) { // delete old entries
            DeleteRequest request = new DeleteRequest();
            request.setDn(dn1);
            if (!execute(request)) success = false;

            b1 = i1.hasNext();
            if (b1) dn1 = i1.next();
        }

        while (b2) { // add new entries

            DN origDn = dns2.get(dn2);
            SearchResult result = sourcePartition.find(origDn);

            AddRequest request = new AddRequest();
            request.setDn(dn2);
            request.setAttributes(result.getAttributes());
            if (!execute(request)) success = false;

            b2 = i2.hasNext();
            if (b2) dn2 = i2.next();
        }

        return success;
    }
*/

    public LDAPSource getSource() throws Exception {
        Partition sourcePartition = moduleContext.getPartition(sourcePartitionName);
        return (LDAPSource)sourcePartition.getSourceManager().getSource(sourceName);
    }

    public LDAPSource getTarget() throws Exception {
        Partition targetPartition = moduleContext.getPartition(targetPartitionName);
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