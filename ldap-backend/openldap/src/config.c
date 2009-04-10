/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>
#include "slap.h"
#include "java-backend.h"

void setClass(const char *value) {

    config.className = ch_strdup(value);

    int i;
    for (i=0; config.className[i]; i++) {
        if (config.className[i] == '.') config.className[i] = '/';
    }

    DEBUG1("Class: %s\n", config.className);
}

void addLibPath(const char *value) {

    char **libpath = (char**)ch_calloc(config.nlibpath + 1, sizeof(char *));
    libpath[config.nlibpath] = ch_strdup(value);

    if (config.nlibpath > 0) {

        int i;
        for (i=0; i<config.nlibpath; i++) {
            //DEBUG1("Moving old libpath #%d.\n", i);
            libpath[i] = config.libpath[i];
            config.libpath[i] = NULL;
        }

        //DEBUG("Releasing old libpath.\n");
        ch_free(config.libpath);
    }

    config.libpath = libpath;
    config.nlibpath++;

    DEBUG1("Libpath: %s\n", config.libpath[config.nlibpath-1]);
}

void addClassPath(const char *value) {

    char **classpath = (char**)ch_calloc(config.nclasspath + 1, sizeof(char *));
    classpath[config.nclasspath] = ch_strdup(value);

    if (config.nclasspath > 0) {

        int i;
        for (i=0; i<config.nclasspath; i++) {
            //DEBUG1("Moving old classpath #%d.\n", i);
            classpath[i] = config.classpath[i];
            config.classpath[i] = NULL;
        }

        //DEBUG("Releasing old classpath.\n");
        ch_free(config.classpath);
    }

    config.classpath = classpath;
    config.nclasspath++;

    DEBUG1("Classpath: %s\n", config.classpath[config.nclasspath-1]);
}

void addProperty(const char *value) {

    char **properties = (char**)ch_calloc(config.nproperties + 1, sizeof(char*));
    properties[config.nproperties] = ch_strdup(value);

    if (config.nproperties > 0) {

        int i;
        for (i=0; i<config.nproperties; i++) {
            //DEBUG1("Moving old properties #%d.\n", i);

            properties[i] = config.properties[i];
            config.properties[i] = NULL;
        }

        //DEBUG("Releasing old properties.\n");
        ch_free(config.properties);
    }

    config.properties = properties;
    config.nproperties++;

    DEBUG1("Property: %s\n", config.properties[config.nproperties-1]);
}

void addOption(const char *value) {

    char **options = (char**)ch_calloc(config.noptions + 1, sizeof(char *));
    options[config.noptions] = ch_strdup(value);

    if (config.noptions > 0) {

        int i;
        for (i=0; i<config.noptions; i++) {
            //DEBUG1("Moving old options #%d.\n", i);
            options[i] = config.options[i];
            config.options[i] = NULL;
        }

        //DEBUG("Releasing old options.\n");
        ch_free(config.options);
    }

    config.options = options;
    config.noptions++;

    DEBUG1("Option: %s\n", config.options[config.noptions-1]);
}

void addSuffix(const char *value) {

    char **suffixes = (char**)ch_calloc(config.nsuffixes + 1, sizeof(char *));
    suffixes[config.nsuffixes] = ch_strdup(value);

    if (config.nsuffixes > 0) {

        int i;
        for (i=0; i<config.nsuffixes; i++) {
            //DEBUG1("Moving old suffixes #%d.\n", i);
            suffixes[i] = config.suffixes[i];
            config.suffixes[i] = NULL;
        }

        //DEBUG("Releasing old suffixes.\n");
        ch_free(config.suffixes);
    }

    config.suffixes = suffixes;
    config.nsuffixes++;

    DEBUG1("Suffix: %s\n", config.suffixes[config.nsuffixes-1]);
}

int java_backend_config(
    BackendInfo *bi,
    const char *fname,
    int lineno,
    int argc,
    char **argv
) {
    DEBUG("==> java_backend_config()\n");
    DEBUG("<== java_backend_config()\n");

    return 0;
}

int java_backend_db_config(
    BackendDB *be,
    const char *fname,
    int lineno,
    int argc,
    char **argv
) {
    DEBUG("==>java_backend_db_config()\n");

    int rc = 0;
    DEBUG1("Parameter name: %s\n", argv[0]);

    if (!strcasecmp(argv[0], "class")) {

        if (argc < 2) {
            DEBUG2(
                "Missing value in \"class\" directive.\n",
                fname, lineno
            );
            rc = 1;
            goto exit;
        }

        setClass(argv[1]);

    } else if (!strcasecmp(argv[0], "libpath")) {

        if (argc < 2) {
            DEBUG2(
                "Missing value in \"libpath\" directive.\n",
                fname, lineno
            );
            rc = 1;
            goto exit;
        }

        addLibPath(argv[1]);

    } else if (!strcasecmp(argv[0], "classpath")) {

        if (argc < 2) {
            DEBUG2(
                "Missing value in \"classpath\" directive.\n",
                fname, lineno
            );
            rc = 1;
            goto exit;
        }

        addClassPath(argv[1]);

    } else if (!strcasecmp(argv[0], "property")) {

        if (argc < 2) {
            DEBUG2(
                "Missing name=value in \"property\" directive.\n",
                fname, lineno
            );
            rc = 1;
            goto exit;
        }

        addProperty(argv[1]);

    } else if (!strcasecmp(argv[0], "option")) {

        if (argc < 2) {
            DEBUG2(
                "Missing value in \"option\" directive.\n",
                fname, lineno
            );
            rc = 1;
            goto exit;
        }

        addOption(argv[1]);
    }

exit:
    DEBUG("<== java_backend_db_config()\n");
    return rc;
}

