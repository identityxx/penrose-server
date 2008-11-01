package org.safehaus.penrose.federation;

/**
 * @author Endi Sukma Dewata
 */
public class NISDomain extends FederationRepositoryConfig {
    
    public final static String SERVER     = "server";
    public final static String DOMAIN     = "domain";

    public final static String YP                    = "yp";
    public final static String NSS                   = "nss";
    public final static String DB                    = "db";

    public final static String YP_SUFFIX             = "ypSuffix";
    public final static String YP_TEMPLATE           = "ypTemplate";

    public final static String NIS_SUFFIX            = "nisSuffix";
    public final static String NIS_TEMPLATE          = "nisTemplate";

    public final static String NSS_SUFFIX            = "nssSuffix";
    public final static String NSS_TEMPLATE          = "nssTemplate";

    public NISDomain() {
        setType("NIS");
    }

    public NISDomain(FederationRepositoryConfig repository) {
        super(repository);
    }
}
