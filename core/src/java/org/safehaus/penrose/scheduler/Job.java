package org.safehaus.penrose.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.Session;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class Job {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected JobConfig jobConfig;
    protected JobContext jobContext;

    protected Partition partition;

    public void init(JobConfig jobConfig, JobContext jobContext) throws Exception {
        this.jobConfig = jobConfig;
        this.jobContext = jobContext;

        partition = jobContext.getPartition();

        log.debug("Initializing "+jobConfig.getName()+" job.");
        
        init();
    }

    public void init() throws Exception {
    }

    public void execute() throws Exception {
    }

    public void destroy() throws Exception {
    }

    public String getDescription() {
        return jobConfig.getDescription();
    }
    
    public String getParameter(String name) {
        return jobConfig.getParameter(name);
    }

    public Collection<String> getParameterNames() {
        return jobConfig.getParameterNames();
    }
    
    public String getName() {
        return jobConfig.getName();
    }

    public JobConfig getJobConfig() {
        return jobConfig;
    }

    public void setJobConfig(JobConfig jobConfig) {
        this.jobConfig = jobConfig;
    }

    public JobContext getJobContext() {
        return jobContext;
    }

    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    public Partition getPartition() {
        return jobContext.getPartition();
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public Session getSession() throws Exception {
        SessionManager sessionManager = getPartition().getPartitionContext().getSessionManager();
        return sessionManager.newAdminSession();
    }
}
