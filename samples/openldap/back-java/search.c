/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"

#include "java_back.h"

Entry* java_back_create_entry( JNIEnv *env, jobject searchResult );

int java_back_search(Operation *op, SlapReply *rs) { 

    Debug( LDAP_DEBUG_TRACE, "==> java_back_search()\n", 0, 0, 0);

    //JavaBackend *be = (JavaBackend *)op->o_bd->be_private;
    Connection *conn = (Connection *)op->o_conn;

    AttributeName *attrs = op->ors_attrs;
    int attrsonly        = op->ors_attrsonly;

    JNIEnv *env;
    jint res;
    jstring base;
    jstring filter;
    jobject jattrs;
    AttributeName* a;
    jobject results;
    jthrowable exc;
    jint size;
    jint i;
    jobject searchResult;
    jobject searchControls;
    jobjectArray attributeNames;

    res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_search(): Failed attaching to JVM.\n", 0, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }

    base = (*env)->NewStringUTF(env, op->o_req_dn.bv_val);
    filter = (*env)->NewStringUTF(env, op->ors_filterstr.bv_val);

    Debug( LDAP_DEBUG_TRACE, "SearchControls sc = new SearchControls();\n", 0, 0, 0);

    searchControls = (*env)->NewObject(env, java_back->searchControlsClass, java_back->searchControlsConstructor);

    if (searchControls == 0) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_search(): Failed creating search controls.\n", 0, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }

    (*env)->CallVoidMethod(env, searchControls, java_back->searchControlsSetSearchScope, op->ors_scope);
    (*env)->CallVoidMethod(env, searchControls, java_back->searchControlsSetCountLimit, op->ors_slimit);
    (*env)->CallVoidMethod(env, searchControls, java_back->searchControlsSetTimeLimit, op->ors_tlimit);

    Debug( LDAP_DEBUG_TRACE, "ArrayList attrs = new ArrayList();\n", 0, 0, 0);

    jattrs = (*env)->NewObject(env, java_back->arrayListClass, java_back->arrayListConstructor);

    a = attrs;
    while (a && a->an_name.bv_val) {
        jstring attributeName;

        attributeName = (*env)->NewStringUTF(env, a->an_name.bv_val);

        Debug( LDAP_DEBUG_TRACE, "attrs.add(\"%s\")\n", attributeName, 0, 0);

        (*env)->CallBooleanMethod(env, jattrs, java_back->arrayListAdd, attributeName);

        a++;
    }

    attributeNames = (*env)->CallObjectMethod(env, jattrs, java_back->arrayListToArray);

    Debug( LDAP_DEBUG_TRACE, "sc.setReturningAttributes(attrs);\n", 0, 0, 0);

    (*env)->CallVoidMethod(env, searchControls, java_back->searchControlsSetReturningAttributes, attributeNames);

    Debug( LDAP_DEBUG_TRACE, "Result result = backend.search(connectionId, base, filter, searchControls);\n", 0, 0, 0);

    results = (*env)->CallObjectMethod(env, java_back->backend, java_back->backendSearch,
        conn->c_connid, base, filter, searchControls);

    Debug( LDAP_DEBUG_TRACE, "int res = results.getReturnCode();\n", 0, 0, 0);

    res = (*env)->CallIntMethod(env, results, java_back->resultsGetReturnCode);
    //Debug( LDAP_DEBUG_TRACE, "RC: %d\n", res, 0, 0);

    exc = (*env)->ExceptionOccurred(env);

    if (exc) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_search(): Exception\n", 0, 0, 0);

        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }

    if (res > 0) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_search(): Failed searching base %s.\n", base, 0, 0);

        rs->sr_err = res;
        send_ldap_result( op, rs );
        return res;
    }
    
    Debug( LDAP_DEBUG_TRACE, "SearchResult sr = (SearchResult)result.next();\n", 0, 0, 0);

    searchResult = (*env)->CallObjectMethod(env, results, java_back->resultsNext);

    while (searchResult) {

        Entry *entry;

        entry = java_back_create_entry(env, searchResult);

        rs->sr_entry = entry;
        rs->sr_attrs = attrs;
        rs->sr_flags = REP_ENTRY_MODIFIABLE;
        rs->sr_err = LDAP_SUCCESS;

        rs->sr_err = send_search_entry(op, rs);

        if (rs->sr_err == LDAP_SIZELIMIT_EXCEEDED) {
            rs->sr_entry = NULL;
            exit;
        }

        entry_free(entry);

        Debug( LDAP_DEBUG_TRACE, "sr = (SearchResult)result.next();\n", 0, 0, 0);

        searchResult = (*env)->CallObjectMethod(env, results, java_back->resultsNext);
    }

    res = (*env)->CallIntMethod(env, results, java_back->resultsGetReturnCode);
    send_ldap_result(op, rs);

    return 0;
}

Entry* java_back_create_entry( JNIEnv *env, jobject searchResult ) {

    int rc;
    Entry		*e;
    char		*type;
    struct berval	vals[2];
    AttributeDescription *ad;
    const char *text;
    char	*next;
    jstring jdn;
    char *dn;
    jobject jattributes;
    jobject jiterator;
    int hasMore;

    e = (Entry *) ch_calloc( 1, sizeof(Entry) );

    if( e == NULL ) {
        return( NULL );
    }

    e->e_id = NOID;

    Debug( LDAP_DEBUG_TRACE, "sr.getName()\n", 0, 0, 0 );

    jdn = (*env)->CallObjectMethod(env, searchResult, java_back->searchResultGetName);

    vals[0].bv_val = (char *)(*env)->GetStringUTFChars(env, jdn, 0);
    vals[0].bv_len = strlen(vals[0].bv_val);
    if (slap_debug & 2048) fprintf(stderr, "dn: %s\n", vals[0].bv_val, vals[0].bv_len);

    rc = dnPrettyNormal( NULL, &vals[0], &e->e_name, &e->e_nname, NULL );
    (*env)->ReleaseStringUTFChars(env, jdn, vals[0].bv_val);

    if( rc != LDAP_SUCCESS ) { // invalid dn
        entry_free( e );
        return NULL;
    }

    vals[0].bv_len = 0;
    vals[1].bv_val = NULL;

    Debug( LDAP_DEBUG_TRACE, "Attributes attributes = sr.getAttributes();\n", 0, 0, 0 );

    jattributes = (*env)->CallObjectMethod(env, searchResult, java_back->searchResultGetAttributes);

    Debug( LDAP_DEBUG_TRACE, "NamingEnumeration ne = attributes.getAll();\n", 0, 0, 0 );

    jiterator = (*env)->CallObjectMethod(env, jattributes, java_back->attributesGetAll);

    Debug( LDAP_DEBUG_TRACE, "ne.hasMore();\n", 0, 0, 0 );

    hasMore = (*env)->CallBooleanMethod(env, jiterator, java_back->namingEnumerationHasMore);
    Debug( LDAP_DEBUG_TRACE, "HAS MORE: %d\n", hasMore, 0, 0);

    while ( hasMore ) {

    	jobject jattribute;
    	jstring jname;
    	jobject values;
        jsize len;
        int i;
        int valuesHasNext;

        Debug( LDAP_DEBUG_TRACE, "ne.next()\n", 0, 0, 0 );
        jattribute = (*env)->CallObjectMethod(env, jiterator, java_back->namingEnumerationNext);
        jname = (*env)->CallObjectMethod(env, jattribute, java_back->attributeGetID);
        type = (char *)(*env)->GetStringUTFChars(env, jname, 0);

        ad = NULL;
        rc = slap_str2ad( type, &ad, &text );
        //if (slap_debug & 2048) fprintf(stderr, " - slap_str2ad: %d\n", (rc == LDAP_SUCCESS));
        if( rc != LDAP_SUCCESS ) {

            rc = slap_str2undef_ad( type, &ad, &text, 0 );
            if( rc != LDAP_SUCCESS ) {
                entry_free( e );
                (*env)->ReleaseStringUTFChars(env, jname, type);
                return NULL;
            }
        }

        // core dump
        //if (slap_debug & 2048) fprintf(stderr, " - desc: %s\n", text);

        values = (*env)->CallObjectMethod(env, jattribute, java_back->attributeGetAll);

        valuesHasNext = (*env)->CallBooleanMethod(env, values, java_back->namingEnumerationHasMore);

        while (valuesHasNext) {
            jstring jvalue;

            jvalue = (*env)->CallObjectMethod(env, values, java_back->namingEnumerationNext);

            Debug( LDAP_DEBUG_TRACE, " - %s\n", type, jvalue, 0 );

            vals[0].bv_val = (char *)(*env)->GetStringUTFChars(env, jvalue, 0);
            vals[0].bv_len = strlen(vals[0].bv_val);

            if (slap_debug & 2048) fprintf(stderr, "%s: %s\n", type, vals[0].bv_val);

            rc = attr_merge( e, ad, vals, NULL );

            //fprintf(stderr, " - attr_merge: %d\n", (rc == LDAP_SUCCESS));
            (*env)->ReleaseStringUTFChars(env, jvalue, vals[0].bv_val);

            if( rc != 0 ) {
                entry_free( e );
                (*env)->ReleaseStringUTFChars(env, jname, type);
                return( NULL );
            }

            valuesHasNext = (*env)->CallBooleanMethod(env, values, java_back->namingEnumerationHasMore);
        }

        (*env)->ReleaseStringUTFChars(env, jname, type);

        hasMore = (*env)->CallBooleanMethod(env, jiterator, java_back->namingEnumerationHasMore);
    }

    if (slap_debug & 2048) fprintf(stderr, "\n");

    return e;
}

