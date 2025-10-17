#ifndef _TINY_JSON_COMBINED_H_
#define _TINY_JSON_COMBINED_H_

#include "common.h"

#define json_containerOf(ptr,type,member)((type*)((char*)ptr-offsetof(type,member)))

typedef enum{JSON_OBJ,JSON_ARRAY,JSON_TEXT,JSON_BOOLEAN,JSON_INTEGER,JSON_REAL,JSON_NULL}jsonType_t;

typedef struct json_s{
    struct json_s*sibling;
    char const*name;
    union{
        char const*value;
        struct{struct json_s*child;struct json_s*last_child;}c;
    }u;
    jsonType_t type;
}json_t;

json_t const*json_create(char*str,json_t mem[],unsigned int qty);

static inline char const*json_getName(json_t const*json){return json->name;}
static inline char const*json_getValue(json_t const*property){return property->u.value;}
static inline jsonType_t json_getType(json_t const*json){return json->type;}
static inline json_t const*json_getSibling(json_t const*json){return json->sibling;}
json_t const*json_getProperty(json_t const*obj,char const*property);
char const*json_getPropertyValue(json_t const*obj,char const*property);
static inline json_t const*json_getChild(json_t const*json){return json->u.c.child;}
static inline bool json_getBoolean(json_t const*property){return *property->u.value=='t';}
static inline int64_t json_getInteger(json_t const*property){return strtoll(property->u.value,(char**)NULL,10);}
static inline double json_getReal(json_t const*property){return strtod(property->u.value,(char**)NULL);}

typedef struct jsonPool_s jsonPool_t;
struct jsonPool_s{json_t*(*init)(jsonPool_t*pool);json_t*(*alloc)(jsonPool_t*pool);};
json_t const*json_createWithPool(char*str,jsonPool_t*pool);

char*json_objOpen(char*dest,char const*name,size_t*remLen);
char*json_objClose(char*dest,size_t*remLen);
char*json_end(char*dest,size_t*remLen);
char*json_arrOpen(char*dest,char const*name,size_t*remLen);
char*json_arrClose(char*dest,size_t*remLen);
char*json_nstr(char*dest,char const*name,char const*value,int len,size_t*remLen);
static inline char*json_str(char*dest,char const*name,char const*value,size_t*remLen){return json_nstr(dest,name,value,-1,remLen);}
char*json_bool(char*dest,char const*name,int value,size_t*remLen);
char*json_null(char*dest,char const*name,size_t*remLen);
char*json_int(char*dest,char const*name,int value,size_t*remLen);
char*json_uint(char*dest,char const*name,unsigned int value,size_t*remLen);
char*json_long(char*dest,char const*name,long int value,size_t*remLen);
char*json_ulong(char*dest,char const*name,unsigned long int value,size_t*remLen);
char*json_verylong(char*dest,char const*name,long long int value,size_t*remLen);
char*json_double(char*dest,char const*name,double value,size_t*remLen);

#endif
