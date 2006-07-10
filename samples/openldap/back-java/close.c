/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"

#include "java_back.h"

int
java_back_close(
	BackendInfo *bd
)
{
    if (slap_debug & 1024) {
        fprintf( stderr, "--------------------------------------------------------------------------------\n");
        fprintf( stderr, "Calling java_back_close\n");
        fprintf( stderr, "--------------------------------------------------------------------------------\n");
    }

    return 0;
}

int
java_back_destroy(
    BackendInfo *bd
)
{
    if (slap_debug & 1024) {
        fprintf( stderr, "--------------------------------------------------------------------------------\n");
        fprintf( stderr, "Calling java_back_destroy\n");
        fprintf( stderr, "--------------------------------------------------------------------------------\n");
    }

    (*jvm)->DestroyJavaVM(jvm);

    return 0;
}

int
java_back_db_destroy(
    BackendDB *be
)
{
    if (slap_debug & 1024) {
        fprintf( stderr, "--------------------------------------------------------------------------------\n");
        fprintf( stderr, "Calling java_back_db_destroy\n");
        fprintf( stderr, "--------------------------------------------------------------------------------\n");
    }
    
    free(java_back_db);
    free(java_back);

    return 0;
}

