package org.safehaus.penrose.nis.scheduler;

import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.jdbc.source.JDBCSource;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.scheduler.Job;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * @author Endi Sukma Dewata
 */
public class NISSyncJob extends Job {

    Source source;
    Collection<Source> targets = new ArrayList<Source>();

    Source changelog;

    public void init() throws Exception {
        String sourceName    = jobConfig.getParameter("source");
        String targetNames   = jobConfig.getParameter("target");
        String changeLogName = jobConfig.getParameter("changelog");

        SourceManager sourceManager = partition.getSourceManager();

        source    = sourceManager.getSource(sourceName);

        StringTokenizer st = new StringTokenizer(targetNames, ", ");
        while (st.hasMoreTokens()) {
            String targetName = st.nextToken();

            Source target = sourceManager.getSource(targetName);
            targets.add(target);
        }

        changelog = sourceManager.getSource(changeLogName);
    }

    public void execute() throws Exception {
/*
        Collection<Source> tmps = createSources();
        loadSources(tmps);
        generateChangeLogs(tmps);
        switchSources(tmps);
*/
    }

    public Collection<Source> createSources() throws Exception {

        Collection<Source> tmps = new ArrayList<Source>();

        for (Source target : targets) {
            String tableName = target.getParameter(JDBCSource.TABLE);

            Source tmp = (Source)target.clone();
            tmp.setName(target.getName()+"_tmp");
            tmp.setParameter(JDBCSource.TABLE, tableName+"_tmp");

            tmps.add(tmp);

            try {
                tmp.create();
            } catch (Exception e) {
                log.error("Failed to create " + tmp.getName() + ": " + e.getMessage());
                tmp.drop();
                tmp.create();
            }
        }

        return tmps;
    }

    public void loadSources(Session session, Collection<Source> tmps) throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        Interpreter interpreter = partition.newInterpreter();

        SearchRequest sourceRequest = new SearchRequest();
        SearchResponse sourceResponse = new NISSyncSearchResponse(
                source,
                tmps,
                interpreter
        );

        source.search(session, sourceRequest, sourceResponse);
    }

    public void generateChangeLogs(Collection<Source> tmps) throws Exception {

    }
    
    public void switchSources(Collection<Source> tmps) throws Exception {

        Iterator i = targets.iterator();
        Iterator j = tmps.iterator();

        while (i.hasNext() && j.hasNext()) {
            Source target = (Source)i.next();
            Source tmp = (Source)j.next();

            try {
                target.drop();
            } catch (Exception e) {
                log.debug("Failed to drop "+target.getName()+": "+e.getMessage());
            }
            
            try {
                tmp.rename(target);
            } catch (Exception e) {
                log.debug("Failed to rename "+tmp.getName()+": "+e.getMessage());
            }
        }
    }
}
