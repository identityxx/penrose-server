/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"

#include "java_back.h"

int
java_back_config(
    BackendInfo *bi,
    const char *fname,
    int lineno,
    int argc,
    char **argv
)
{
    Debug(LDAP_DEBUG_TRACE, "==> java_back_config()\n", 0, 0, 0);
    Debug(LDAP_DEBUG_TRACE, "<== java_back_config()\n", 0, 0, 0);

    return 0;
}

int
java_back_db_config(
    BackendDB *be,
    const char *fname,
    int lineno,
    int argc,
    char **argv
)
{
    Debug( LDAP_DEBUG_TRACE, "==>java_back_db_config()\n", 0, 0, 0 );

    JavaBackend *java_back = (JavaBackend *)be->be_private;

    if ( !strcasecmp( argv[0], "class" ) ) {

        int i;

        if ( argc < 2 ) {
            Debug( LDAP_DEBUG_TRACE,
                "<==java_back_db_config (%s line %d): "
                "missing value in \"class\" directive\n",
                fname, lineno, 0 );
            return 1;
        }

        java_back->className = ch_strdup( argv[1] );

        for (i=0; java_back->className[i]; i++) {
            if (java_back->className[i] == '.') java_back->className[i] = '/';
        }

        Debug( LDAP_DEBUG_TRACE, "<==java_back_db_config(): className=%s\n", java_back->className, 0, 0 );

    } else if ( !strcasecmp( argv[0], "libpath" ) ) {

        int i;
        char **libpath;

        if ( argc < 2 ) {
            Debug( LDAP_DEBUG_TRACE,
                "<==java_back_db_config (%s line %d): "
                "missing value in \"libpath\" directive\n",
                fname, lineno, 0 );
            return 1;
        }

        libpath = ch_calloc(java_back->nlibpath + 1, sizeof(char *));
        libpath[java_back->nlibpath] = ch_strdup( argv[1] );

        if (java_back->nlibpath > 0) {

            for (i=0; i<java_back->nlibpath; i++) {
                //Debug( LDAP_DEBUG_TRACE, "Moving old libpath #%d\n", i, 0, 0 );
                libpath[i] = java_back->libpath[i];
                java_back->libpath[i] = NULL;
            }

            //Debug( LDAP_DEBUG_TRACE, "Releasing old libpath\n", 0, 0, 0);
            ch_free(java_back->libpath);
        }

        java_back->libpath = libpath;
        java_back->nlibpath++;

        Debug( LDAP_DEBUG_TRACE, "<==java_back_db_config(): libpath=%s\n", java_back->libpath[java_back->nlibpath-1], 0, 0 );

    } else if ( !strcasecmp( argv[0], "classpath" ) ) {

        int i;
        char **classpath;

        if ( argc < 2 ) {
            Debug( LDAP_DEBUG_TRACE,
                "<==java_back_db_config (%s line %d): "
                "missing value in \"classpath\" directive\n",
                fname, lineno, 0 );
            return 1;
        }

        classpath = ch_calloc(java_back->nclasspath + 1, sizeof(char *));
        classpath[java_back->nclasspath] = ch_strdup( argv[1] );

        if (java_back->nclasspath > 0) {

            for (i=0; i<java_back->nclasspath; i++) {
                //Debug( LDAP_DEBUG_TRACE, "Moving old classpath #%d\n", i, 0, 0 );
                classpath[i] = java_back->classpath[i];
                java_back->classpath[i] = NULL;
            }

            //Debug( LDAP_DEBUG_TRACE, "Releasing old classpath\n", 0, 0, 0);
            ch_free(java_back->classpath);
        }

        java_back->classpath = classpath;
        java_back->nclasspath++;

        Debug( LDAP_DEBUG_TRACE, "<==java_back_db_config(): classpath=%s\n", java_back->classpath[java_back->nclasspath-1], 0, 0 );

    } else if ( !strcasecmp( argv[0], "property" ) ) {

        int i;
        char **names;
        char **values;

        if ( argc < 2 ) {
            Debug( LDAP_DEBUG_TRACE,
                "<==java_back_db_config (%s line %d): "
                "missing name in \"property\" directive\n",
                fname, lineno, 0 );
            return 1;
        }

        if ( argc < 3 ) {
            Debug( LDAP_DEBUG_TRACE,
                "<==java_back_db_config (%s line %d): "
                "missing value in \"property\" directive\n",
                fname, lineno, 0 );
            return 1;
        }

        names = ch_calloc(java_back->nproperties + 1, sizeof(char *));
        names[java_back->nproperties] = ch_strdup( argv[1] );

        values = ch_calloc(java_back->nproperties + 1, sizeof(char *));
        values[java_back->nproperties] = ch_strdup( argv[2] );

        if (java_back->nproperties > 0) {

            for (i=0; i<java_back->nproperties; i++) {
                //Debug( LDAP_DEBUG_TRACE, "Moving old properties #%d\n", i, 0, 0 );

                names[i] = java_back->propertyNames[i];
                java_back->propertyNames[i] = NULL;

                values[i] = java_back->propertyValues[i];
                java_back->propertyValues[i] = NULL;
            }

            //Debug( LDAP_DEBUG_TRACE, "Releasing old properties\n", 0, 0, 0);
            ch_free(java_back->propertyNames);
            ch_free(java_back->propertyValues);
        }

        java_back->propertyNames = names;
        java_back->propertyValues = values;
        java_back->nproperties++;

        Debug(
            LDAP_DEBUG_TRACE,
            "<==java_back_db_config(): property %s=%s\n",
            java_back->propertyNames[java_back->nproperties-1],
            java_back->propertyValues[java_back->nproperties-1],
            0
        );

    } else if ( !strcasecmp( argv[0], "option" ) ) {

        int i;
        char **options;

        if ( argc < 2 ) {
            Debug( LDAP_DEBUG_TRACE,
                "<==java_back_db_config (%s line %d): "
                "missing value in \"option\" directive\n",
                fname, lineno, 0 );
            return 1;
        }

        options = ch_calloc(java_back->noptions + 1, sizeof(char *));
        options[java_back->noptions] = ch_strdup( argv[1] );

        if (java_back->noptions > 0) {

            for (i=0; i<java_back->noptions; i++) {
                //Debug( LDAP_DEBUG_TRACE, "Moving old options #%d\n", i, 0, 0 );
                options[i] = java_back->options[i];
                java_back->options[i] = NULL;
            }

            //Debug( LDAP_DEBUG_TRACE, "Releasing old options\n", 0, 0, 0);
            ch_free(java_back->options);
        }

        java_back->options = options;
        java_back->noptions++;

        Debug( LDAP_DEBUG_TRACE, "<==java_back_db_config(): option=%s\n", java_back->options[java_back->noptions-1], 0, 0 );

    }

    return 0;
}

