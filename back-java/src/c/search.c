/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"

#include "java_back.h"

Entry* java_back_create_entry(JavaBackend *java_back, JNIEnv *env, jobject searchResult);

int java_back_search(Operation *op, SlapReply *rs) { 

    Debug( LDAP_DEBUG_TRACE, "==> java_back_search()\n", 0, 0, 0);

    JavaBackend *java_back = (JavaBackend *)op->o_bd->be_private;
    JavaVM *jvm = java_back->jvm;
    JNIEnv *env;

    Connection *conn = (Connection *)op->o_conn;

    AttributeName *attrs = op->ors_attrs;
    int attrsonly        = op->ors_attrsonly;

    jobject jattrs;
    AttributeName* a;
    jobject results;
    jthrowable exc;
    jint size;
    jint i;
    jobject searchControls;
    jobjectArray attributeNames;

    jint res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_search(): Failed attaching to JVM.\n", 0, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }

    jstring base = (*env)->NewStringUTF(env, op->o_req_dn.bv_val);
    jstring filter = (*env)->NewStringUTF(env, op->ors_filterstr.bv_val);

    Debug( LDAP_DEBUG_TRACE, "SearchControls sc = new SearchControls();\n", 0, 0, 0);

    searchControls = (*env)->NewObject(env, java_back->searchControlsClass, java_back->searchControlsConstructor);

    if (searchControls == 0) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_search(): Failed creating search controls.\n", 0, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }

    Debug( LDAP_DEBUG_TRACE, "sc.setSearchScope(%d);\n", op->ors_scope, 0, 0);

    (*env)->CallVoidMethod(env, searchControls, java_back->searchControlsSetSearchScope, op->ors_scope);

    Debug( LDAP_DEBUG_TRACE, "sc.setCountLimit(%d);\n", op->ors_slimit, 0, 0);

    (*env)->CallVoidMethod(env, searchControls, java_back->searchControlsSetCountLimit, op->ors_slimit);

    Debug( LDAP_DEBUG_TRACE, "sc.setTimeLimit(%d);\n", op->ors_tlimit, 0, 0);

    (*env)->CallVoidMethod(env, searchControls, java_back->searchControlsSetTimeLimit, op->ors_tlimit);

    Debug( LDAP_DEBUG_TRACE, "ArrayList attrs = new ArrayList();\n", 0, 0, 0);

    jattrs = (*env)->NewObject(env, java_back->arrayListClass, java_back->arrayListConstructor);

    a = attrs;
    while (a && a->an_name.bv_val) {
        jstring attributeName;

        attributeName = (*env)->NewStringUTF(env, a->an_name.bv_val);

        Debug( LDAP_DEBUG_TRACE, "attrs.add(\"%s\")\n", a->an_name.bv_val, 0, 0);

        (*env)->CallBooleanMethod(env, jattrs, java_back->arrayListAdd, attributeName);

        a++;
    }

    attributeNames = (*env)->CallObjectMethod(env, jattrs, java_back->arrayListToArray);

    Debug( LDAP_DEBUG_TRACE, "sc.setReturningAttributes(attrs);\n", 0, 0, 0);

    (*env)->CallVoidMethod(env, searchControls, java_back->searchControlsSetReturningAttributes, attributeNames);

    Debug( LDAP_DEBUG_TRACE, "Results results = backend.search(connectionId, \"%s\", \"%s\", searchControls);\n", op->o_req_dn.bv_val, op->ors_filterstr.bv_val, 0);

    results = (*env)->CallObjectMethod(env, java_back->backend, java_back->backendSearch,
        conn->c_connid, base, filter, searchControls);

    Debug( LDAP_DEBUG_TRACE, "int res = results.getReturnCode();\n", 0, 0, 0);

    exc = (*env)->ExceptionOccurred(env);

    if (exc) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_search(): Exception\n", 0, 0, 0);

        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }

    res = (*env)->CallIntMethod(env, results, java_back->resultsGetReturnCode);
    //Debug( LDAP_DEBUG_TRACE, "RC: %d\n", res, 0, 0);

    if (res > 0) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_search(): Failed searching base %s.\n", base, 0, 0);

        rs->sr_err = res;
        send_ldap_result( op, rs );
        return res;
    }
    
    Debug(LDAP_DEBUG_TRACE, "boolean hasNext = results.hasNext();\n", 0, 0, 0);

    int hasNext = (*env)->CallBooleanMethod(env, results, java_back->resultsHasNext);

    Debug(LDAP_DEBUG_TRACE, "hasNext: %d\n", hasNext, 0, 0);

    while (hasNext) {

        Debug( LDAP_DEBUG_TRACE, "SearchResult sr = (SearchResult)results.next();\n", 0, 0, 0);

        jobject searchResult = (*env)->CallObjectMethod(env, results, java_back->resultsNext);

        Entry *entry = java_back_create_entry(java_back, env, searchResult);

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

        Debug(LDAP_DEBUG_TRACE, "hasNext = results.hasNext();\n", 0, 0, 0);

        hasNext = (*env)->CallBooleanMethod(env, results, java_back->resultsHasNext);

        Debug(LDAP_DEBUG_TRACE, "hasNext: %d\n", hasNext, 0, 0);
    }

    Debug( LDAP_DEBUG_TRACE, "int rc = sr.getReturnCode();\n", 0, 0, 0);

    res = (*env)->CallIntMethod(env, results, java_back->resultsGetReturnCode);

    Debug( LDAP_DEBUG_TRACE, "rc: %d\n", res, 0, 0);

    rs->sr_err = res;

    send_ldap_result(op, rs);

    return 0;
}

Entry* java_back_create_entry(JavaBackend *java_back, JNIEnv *env, jobject searchResult) {

    int rc;
    char		*type;
    struct berval	vals[2];
    AttributeDescription *ad;
    const char *text;
    char	*next;
    jstring jdn;
    char *dn;
    jobject jiterator;
    int hasMore;

    Entry *e = (Entry *)ch_calloc( 1, sizeof(Entry) );

    if (e == NULL) {
        return NULL;
    }

    e->e_id = NOID;

    Debug(LDAP_DEBUG_TRACE, "String dn = sr.getName();\n", 0, 0, 0);

    jdn = (*env)->CallObjectMethod(env, searchResult, java_back->searchResultGetName);

    vals[0].bv_val = (char *)(*env)->GetStringUTFChars(env, jdn, 0);
    vals[0].bv_len = strlen(vals[0].bv_val);

    Debug(LDAP_DEBUG_TRACE, "dn: %s\n", vals[0].bv_val, 0, 0);

    rc = dnPrettyNormal(NULL, &vals[0], &e->e_name, &e->e_nname, NULL);

    (*env)->ReleaseStringUTFChars(env, jdn, vals[0].bv_val);

    if (rc != LDAP_SUCCESS) {
        Debug(LDAP_DEBUG_TRACE, "Invalid dn.\n", 0, 0, 0);
        entry_free(e);
        return NULL;
    }

    vals[0].bv_len = 0;
    vals[1].bv_val = NULL;

    Debug( LDAP_DEBUG_TRACE, "Attributes attributes = sr.getAttributes();\n", 0, 0, 0 );

    jobject jattributes = (*env)->CallObjectMethod(env, searchResult, java_back->searchResultGetAttributes);

    Debug( LDAP_DEBUG_TRACE, "NamingEnumeration ne = attributes.getAll();\n", 0, 0, 0 );

    jiterator = (*env)->CallObjectMethod(env, jattributes, java_back->attributesGetAll);

    Debug( LDAP_DEBUG_TRACE, "boolean hasMore = ne.hasMore();\n", 0, 0, 0 );

    hasMore = (*env)->CallBooleanMethod(env, jiterator, java_back->namingEnumerationHasMore);

    Debug( LDAP_DEBUG_TRACE, "hasMore: %d\n", hasMore, 0, 0);

    while ( hasMore ) {

    	jstring jname;
    	jobject values;
        jsize len;
        int i;
        int valuesHasNext;

        Debug( LDAP_DEBUG_TRACE, "Attribute attribute = (Attribute)ne.next();\n", 0, 0, 0 );

    	jobject jattribute = (*env)->CallObjectMethod(env, jiterator, java_back->namingEnumerationNext);

        jname = (*env)->CallObjectMethod(env, jattribute, java_back->attributeGetID);
        type = (char *)(*env)->GetStringUTFChars(env, jname, 0);

        ad = NULL;
        rc = slap_str2ad( type, &ad, &text );

        //if (slap_debug & 2048) fprintf(stderr, " - slap_str2ad: %d\n", (rc == LDAP_SUCCESS));

        if( rc != LDAP_SUCCESS ) {

            rc = slap_str2undef_ad( type, &ad, &text );
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

            vals[0].bv_val = (char *)(*env)->GetStringUTFChars(env, jvalue, 0);
            vals[0].bv_len = strlen(vals[0].bv_val);

            Debug(LDAP_DEBUG_TRACE, "%s: %s\n", type, vals[0].bv_val, 0);

            rc = attr_merge( e, ad, vals, NULL );

            //fprintf(stderr, " - attr_merge: %d\n", (rc == LDAP_SUCCESS));

            (*env)->ReleaseStringUTFChars(env, jvalue, vals[0].bv_val);

            if( rc != 0 ) {
                entry_free( e );
                (*env)->ReleaseStringUTFChars(env, jname, type);
                return NULL;
            }

            valuesHasNext = (*env)->CallBooleanMethod(env, values, java_back->namingEnumerationHasMore);
        }

        (*env)->ReleaseStringUTFChars(env, jname, type);

        hasMore = (*env)->CallBooleanMethod(env, jiterator, java_back->namingEnumerationHasMore);
    }

    return e;
}

