package org.safehaus.penrose.federation;

/**
 * @author Endi Sukma Dewata
 */
public class NISDomain extends FederationRepositoryConfig {
    
    public final static String SERVER     = "server";
    public final static String DOMAIN     = "domain";

    public final static String YP         = "yp";

    public NISDomain() {
        setType("NIS");
    }

    public NISDomain(FederationRepositoryConfig repository) {
        super(repository);
    }
}
