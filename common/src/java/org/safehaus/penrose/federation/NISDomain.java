package org.safehaus.penrose.federation;

/**
 * @author Endi Sukma Dewata
 */
public class NISDomain extends Repository {
    
    public final static String NIS_SERVER            = "nisServer";
    public final static String NIS_DOMAIN            = "nisDomain";

    public final static String YP                    = "yp";
    public final static String NIS                   = "nis";
    public final static String NSS                   = "nss";
    public final static String DB                    = "db";

    public final static String YP_ENABLED            = "ypEnabled";
    public final static String YP_SUFFIX             = "ypSuffix";
    public final static String YP_TEMPLATE           = "ypTemplate";

    public final static String NIS_ENABLED           = "nisEnabled";
    public final static String NIS_SUFFIX            = "nisSuffix";
    public final static String NIS_TEMPLATE          = "nisTemplate";

    public final static String NSS_ENABLED           = "nssEnabled";
    public final static String NSS_SUFFIX            = "nssSuffix";
    public final static String NSS_TEMPLATE          = "nssTemplate";

    public NISDomain() {
        setType("NIS");
    }

    public NISDomain(Repository repository) {
        super(repository);
    }
}
