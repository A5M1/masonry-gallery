#include "common.h"
#include "tinyjson_combined.h"

typedef struct jsonStaticPool_s {
    json_t* mem;
    unsigned int qty;
    unsigned int nextFree;
    jsonPool_t pool;
}jsonStaticPool_t;

json_t const* json_getProperty(json_t const* obj, char const* property) {
    json_t const* sibling;
    for (sibling = obj->u.c.child;sibling;sibling = sibling->sibling)
        if (sibling->name && !strcmp(sibling->name, property))return sibling;
    return 0;
}

char const* json_getPropertyValue(json_t const* obj, char const* property) {
    json_t const* field = json_getProperty(obj, property);
    if (!field)return 0;
    jsonType_t type = json_getType(field);
    if (JSON_ARRAY >= type)return 0;
    return json_getValue(field);
}

static char* goBlank(char* str);
static char* goNum(char* str);
static json_t* poolInit(jsonPool_t* pool);
static json_t* poolAlloc(jsonPool_t* pool);
static char* objValue(char* ptr, json_t* obj, jsonPool_t* pool);
static char* setToNull(char* ch);
static bool isEndOfPrimitive(char ch);

json_t const* json_createWithPool(char* str, jsonPool_t* pool) {
    char* ptr = goBlank(str);
    if (!ptr || (*ptr != '{' && *ptr != '['))return 0;
    json_t* obj = pool->init(pool);
    obj->name = 0;
    obj->sibling = 0;
    obj->u.c.child = 0;
    ptr = objValue(ptr, obj, pool);
    if (!ptr)return 0;
    return obj;
}

json_t const* json_create(char* str, json_t mem[], unsigned int qty) {
    jsonStaticPool_t spool;
    spool.mem = mem;
    spool.qty = qty;
    spool.pool.init = poolInit;
    spool.pool.alloc = poolAlloc;
    return json_createWithPool(str, &spool.pool);
}

static char getEscape(char ch) {
    static struct { 
        char ch;
        char code; 
    } 
    const pair[] = { 
        {'\"','\"'},{'\\','\\'},{'/','/'},{'b','\b'},{'f','\f'},{'n','\n'},{'r','\r'},{'t','\t'} 
    };
    for (unsigned int i = 0;i < sizeof pair / sizeof * pair;++i)
        if (pair[i].ch == ch)return pair[i].code;
    return '\0';
}

static unsigned char getCharFromUnicode(unsigned char const* str) {
    for (unsigned int i = 0;i < 4;++i)if (!isxdigit(str[i]))return'\0';
    return'?';
}

static char* parseString(char* str) {
    unsigned char* head = (unsigned char*)str;
    unsigned char* tail = (unsigned char*)str;
    for (;*head;++head, ++tail) {
        if (*head == '\"') { *tail = '\0';return(char*)++head; }
        if (*head == '\\') {
            if (*++head == 'u') {
                char const ch = getCharFromUnicode(++head);
                if (ch == '\0')return 0;
                *tail = ch;
                head += 3;
            }
            else {
                char const esc = getEscape(*head);
                if (esc == '\0')return 0;
                *tail = esc;
            }
        }
        else *tail = *head;
    }
    return 0;
}

static char* propertyName(char* ptr, json_t* property) {
    property->name = ++ptr;
    ptr = parseString(ptr);
    if (!ptr)return 0;
    ptr = goBlank(ptr);
    if (!ptr)return 0;
    if (*ptr++ != ':')return 0;
    return goBlank(ptr);
}

static char* textValue(char* ptr, json_t* property) {
    ++property->u.value;
    ptr = parseString(++ptr);
    if (!ptr)return 0;
    property->type = JSON_TEXT;
    return ptr;
}

static char* checkStr(char* ptr, char const* str) {
    while (*str)if (*ptr++ != *str++)return 0;
    return ptr;
}

static char* primitiveValue(char* ptr, json_t* property, char const* value, jsonType_t type) {
    ptr = checkStr(ptr, value);
    if (!ptr || !isEndOfPrimitive(*ptr))return 0;
    ptr = setToNull(ptr);
    property->type = type;
    return ptr;
}

static char* trueValue(char* ptr, json_t* property) { return primitiveValue(ptr, property, "true", JSON_BOOLEAN); }
static char* falseValue(char* ptr, json_t* property) { return primitiveValue(ptr, property, "false", JSON_BOOLEAN); }
static char* nullValue(char* ptr, json_t* property) { return primitiveValue(ptr, property, "null", JSON_NULL); }

static char* expValue(char* ptr) {
    if (*ptr == '-' || *ptr == '+')++ptr;
    if (!isdigit((int)(*ptr)))return 0;
    return goNum(++ptr);
}

static char* fraqValue(char* ptr) {
    if (!isdigit((int)(*ptr)))return 0;
    return goNum(++ptr);
}

static char* numValue(char* ptr, json_t* property) {
    if (*ptr == '-')++ptr;
    if (!isdigit((int)(*ptr)))return 0;
    if (*ptr != '0') { ptr = goNum(ptr);if (!ptr)return 0; }
    else if (isdigit((int)(*++ptr)))return 0;
    property->type = JSON_INTEGER;
    if (*ptr == '.') { ptr = fraqValue(++ptr);if (!ptr)return 0;property->type = JSON_REAL; }
    if (*ptr == 'e' || *ptr == 'E') { ptr = expValue(++ptr);if (!ptr)return 0;property->type = JSON_REAL; }
    if (!isEndOfPrimitive(*ptr))return 0;
    if (JSON_INTEGER == property->type) {
        char const* value = property->u.value;
        bool const negative = *value == '-';
        static char const min[] = "-9223372036854775808";
        static char const max[] = "9223372036854775807";
        unsigned int const maxdigits = (negative ? sizeof min : sizeof max) - 1;
        unsigned int const len = (unsigned int const)(ptr - value);
        if (len > maxdigits)return 0;
        if (len == maxdigits) {
            char const tmp = *ptr;
            *ptr = '\0';
            char const* const threshold = negative ? min : max;
            if (0 > strcmp(threshold, value))return 0;
            *ptr = tmp;
        }
    }
    return setToNull(ptr);
}

static void add(json_t* obj, json_t* property) {
    property->sibling = 0;
    if (!obj->u.c.child) { obj->u.c.child = property;obj->u.c.last_child = property; }
    else { obj->u.c.last_child->sibling = property;obj->u.c.last_child = property; }
}

static char* objValue(char* ptr, json_t* obj, jsonPool_t* pool) {
    obj->type = *ptr == '{' ? JSON_OBJ : JSON_ARRAY;
    obj->u.c.child = 0;
    obj->sibling = 0;
    ptr++;
    for (;;) {
        ptr = goBlank(ptr);
        if (!ptr)return 0;
        if (*ptr == ',') { ++ptr;continue; }
        char const endchar = (obj->type == JSON_OBJ) ? '}' : ']';
        if (*ptr == endchar) {
            *ptr = '\0';
            json_t* parentObj = obj->sibling;
            if (!parentObj)return++ptr;
            obj->sibling = 0;
            obj = parentObj;
            ++ptr;
            continue;
        }
        json_t* property = pool->alloc(pool);
        if (!property)return 0;
        if (obj->type != JSON_ARRAY) {
            if (*ptr != '\"')return 0;
            ptr = propertyName(ptr, property);
            if (!ptr)return 0;
        }
        else property->name = 0;
        add(obj, property);
        property->u.value = ptr;
        switch (*ptr) {
        case '{':property->type = JSON_OBJ;property->u.c.child = 0;property->sibling = obj;obj = property;++ptr;break;
        case '[':property->type = JSON_ARRAY;property->u.c.child = 0;property->sibling = obj;obj = property;++ptr;break;
        case '\"':ptr = textValue(ptr, property);break;
        case 't':ptr = trueValue(ptr, property);break;
        case 'f':ptr = falseValue(ptr, property);break;
        case 'n':ptr = nullValue(ptr, property);break;
        default:ptr = numValue(ptr, property);break;
        }
        if (!ptr)return 0;
    }
}

static json_t* poolInit(jsonPool_t* pool) {
    jsonStaticPool_t* spool = json_containerOf(pool, jsonStaticPool_t, pool);
    spool->nextFree = 1;
    return spool->mem;
}

static json_t* poolAlloc(jsonPool_t* pool) {
    jsonStaticPool_t* spool = json_containerOf(pool, jsonStaticPool_t, pool);
    if (spool->nextFree >= spool->qty)return 0;
    return spool->mem + spool->nextFree++;
}

static bool isOneOfThem(char ch, char const* set) { while (*set != '\0')if (ch == *set++)return true;return false; }
static char* goWhile(char* str, char const* set) { for (;*str != '\0';++str) { if (!isOneOfThem(*str, set))return str; }return 0; }
static char const* const blank = " \n\r\t\f";
static char* goBlank(char* str) { return goWhile(str, blank); }
static char* goNum(char* str) { for (;*str != '\0';++str) { if (!isdigit((int)(*str)))return str; }return 0; }
static char const* const endofblock = "}]";
static char* setToNull(char* ch) { if (!isOneOfThem(*ch, endofblock))*ch++ = '\0';return ch; }
static bool isEndOfPrimitive(char ch) { return ch == ',' || isOneOfThem(ch, blank) || isOneOfThem(ch, endofblock); }


static char* chtoa(char* dest, char ch, size_t* remLen) { if (*remLen != 0) { --*remLen;*dest = ch;*++dest = '\0'; }return dest; }
static char* atoa(char* dest, char const* src, size_t* remLen) { for (;*src != '\0' && *remLen != 0;++dest, ++src, -- * remLen)*dest = *src;*dest = '\0';return dest; }
static char* strname(char* dest, char const* name, size_t* remLen) { dest = chtoa(dest, '\"', remLen);if (NULL != name) { dest = atoa(dest, name, remLen);dest = atoa(dest, "\":\"", remLen); }return dest; }
static int nibbletoch(int nibble) { return"0123456789ABCDEF"[nibble % 16u]; }
static int escape(int ch) {
    static struct { char code; char ch; } const pair[] = {
        {'\"','\"'}, {'\\','\\'}, {'/','/'}, {'b','\b'},
        {'f','\f'}, {'n','\n'}, {'r','\r'}, {'t','\t'}
    };
    for (size_t i = 0; i < sizeof pair / sizeof * pair; ++i)
        if (ch == pair[i].ch) return pair[i].code;
    return '\0';
}
static char* atoesc(char* dest, char const* src, int len, size_t* remLen) {
    for (int i = 0;src[i] != '\0' && (i<len || 0>len) && *remLen != 0;++dest, ++i, -- * remLen) {
        if (src[i] >= ' ' && src[i] != '\"' && src[i] != '\\' && src[i] != '/')*dest = src[i];
        else {
            if (*remLen != 0) {
                *dest++ = '\\';--*remLen;int const esc = escape(src[i]);
                if (esc) { if (*remLen != 0)*dest = esc; }
                else {
                    if (*remLen != 0) { --*remLen;*dest++ = 'u'; }
                    if (*remLen != 0) { --*remLen;*dest++ = '0'; }
                    if (*remLen != 0) { --*remLen;*dest++ = '0'; }
                    if (*remLen != 0) { --*remLen;*dest++ = nibbletoch(src[i] / 16); }
                    if (*remLen != 0) { --*remLen;*dest++ = nibbletoch(src[i]); }
                }
            }
        }
        if (*remLen == 0)break;
    }
    *dest = '\0';return dest;
}
static char* primitivename(char* dest, char const* name, size_t* remLen) {
    if (NULL == name)return dest;
    dest = chtoa(dest, '\"', remLen);
    dest = atoa(dest, name, remLen);
    dest = atoa(dest, "\":", remLen);
    return dest;
}


char* json_objOpen(char* dest, char const* name, size_t* remLen) {
    if (!name) return chtoa(dest, '{', remLen);
    dest = chtoa(dest, '\"', remLen);
    dest = atoa(dest, name, remLen);
    dest = atoa(dest, "\":{", remLen);
    return dest;
}
char* json_arrOpen(char* dest, char const* name, size_t* remLen) {
    if (!name) return chtoa(dest, '[', remLen);
    dest = chtoa(dest, '\"', remLen);
    dest = atoa(dest, name, remLen);
    dest = atoa(dest, "\":[", remLen);
    return dest;
}
char* json_objClose(char* dest, size_t* remLen) {
    while (dest != NULL && *(dest - 1) == ',') { --dest; ++*remLen; }
    return chtoa(dest, '}', remLen);
}
char* json_arrClose(char* dest, size_t* remLen) {
    while (dest != NULL && *(dest - 1) == ',') { --dest; ++*remLen; }
    return chtoa(dest, ']', remLen);
}
char* json_nstr(char* dest, char const* name, char const* value, int len, size_t* remLen) {
    dest = strname(dest, name, remLen);
    dest = atoesc(dest, value, len, remLen);
    dest = chtoa(dest, '\"', remLen);
    dest = chtoa(dest, ',', remLen);
    return dest;
}
char* json_bool(char* dest, char const* name, int value, size_t* remLen) {
    dest = primitivename(dest, name, remLen);
    dest = atoa(dest, value ? "true" : "false", remLen);
    dest = chtoa(dest, ',', remLen);
    return dest;
}
char* json_null(char* dest, char const* name, size_t* remLen) {
    dest = primitivename(dest, name, remLen);
    dest = atoa(dest, "null", remLen);
    dest = chtoa(dest, ',', remLen);
    return dest;
}
char* json_end(char* dest, size_t* remLen) {
    if (dest[-1] == ',') { --dest; ++*remLen; }
    return dest;
}


#define ALL_TYPES \
    X(json_int,int,"%d")\
    X(json_long,long,"%ld")\
    X(json_uint,unsigned int,"%u")\
    X(json_ulong,unsigned long,"%lu")\
    X(json_verylong,long long,"%lld")\
    X(json_double,double,"%g")

#define json_num(funcname, type, fmt) \
char* funcname(char* dest, const char* name, type value, size_t* remLen) { \
    int digitLen; \
    dest = primitivename(dest, name, remLen); \
    digitLen = snprintf(dest, *remLen, fmt, value); \
    if (digitLen >= (int)*remLen + 1) { \
        digitLen = (int)*remLen; \
    } \
    *remLen -= (size_t)digitLen; \
    dest += digitLen; \
    dest = chtoa(dest, ',', remLen); \
    return dest; \
}


#define X(name,type,fmt) json_num(name,type,fmt)
ALL_TYPES
#undef X
