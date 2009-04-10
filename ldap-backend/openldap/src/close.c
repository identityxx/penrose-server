/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"

#include "java-backend.h"

int java_backend_db_close(BackendDB   *be) {
    DEBUG("==> java_backend_db_close().\n");

    (*jvm)->DestroyJavaVM(jvm);

    DEBUG("<== java_backend_db_close().\n");

    return 0;
}

int java_backend_close(BackendInfo *bd) {
    DEBUG("==> java_backend_close().\n");
    DEBUG("<== java_backend_close().\n");

    return 0;
}

int
java_backend_destroy(
    BackendInfo *bd
)
{
    DEBUG("==> java_backend_destroy().\n");
    DEBUG("<== java_backend_destroy().\n");

    return 0;
}

int
java_backend_db_destroy(
    BackendDB *be
)
{
    DEBUG("==> java_backend_db_destroy().\n");
    
    free(be->be_private);
    be->be_private = NULL;

    DEBUG("<== java_backend_db_destroy().\n");

    return 0;
}

