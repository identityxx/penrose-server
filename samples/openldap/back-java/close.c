/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"

#include "java_back.h"

int
java_back_db_close(
    BackendDB   *be
)
{
    Debug( LDAP_DEBUG_TRACE, "==> java_back_db_close().\n", 0, 0, 0);
    Debug( LDAP_DEBUG_TRACE, "<== java_back_db_close().\n", 0, 0, 0);

    return 0;
}

int
java_back_close(
	BackendInfo *bd
)
{
    Debug( LDAP_DEBUG_TRACE, "==> java_back_close().\n", 0, 0, 0);
    Debug( LDAP_DEBUG_TRACE, "<== java_back_close().\n", 0, 0, 0);

    return 0;
}

int
java_back_destroy(
    BackendInfo *bd
)
{
    Debug( LDAP_DEBUG_TRACE, "==> java_back_destroy().\n", 0, 0, 0);
    Debug( LDAP_DEBUG_TRACE, "<== java_back_destroy().\n", 0, 0, 0);

    (*jvm)->DestroyJavaVM(jvm);

    return 0;
}

int
java_back_db_destroy(
    BackendDB *be
)
{
    Debug( LDAP_DEBUG_TRACE, "==> java_back_db_destroy().\n", 0, 0, 0);
    Debug( LDAP_DEBUG_TRACE, "<== java_back_db_destroy().\n", 0, 0, 0);
    
    //free(java_back_db);
    free(java_back);

    return 0;
}

