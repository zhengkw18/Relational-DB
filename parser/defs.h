#ifndef DEFS
#define DEFS

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    FIELD_TYPE_INT,
    FIELD_TYPE_FLOAT,
    FIELD_TYPE_CHAR,
    FIELD_TYPE_DATE,
    FIELD_TYPE_VARCHAR
} field_type_t;

typedef enum {
    FIELD_FLAG_NOTNULL = 1,
    FIELD_FLAG_UNIQUE = 2
} field_flag_t;

typedef enum {
    TABLE_CONSTRAINT_PRIMARY_KEY,
    TABLE_CONSTRAINT_FOREIGN_KEY,
    TABLE_CONSTRAINT_CHECK,
    TABLE_CONSTRAINT_INDEX
} table_constraint_type_t;

#define OPERATOR_UNARY 0x80
typedef enum {
    OPERATOR_NONE = 0,
    /* arithematic */
    OPERATOR_ADD,
    OPERATOR_MINUS,
    OPERATOR_DIV,
    OPERATOR_MUL,
    /* logical */
    OPERATOR_AND,
    OPERATOR_OR,
    /* compare */
    OPERATOR_EQ,
    OPERATOR_GEQ,
    OPERATOR_LEQ,
    OPERATOR_NEQ,
    OPERATOR_GT,
    OPERATOR_LT,
    OPERATOR_IN,
    OPERATOR_IN_WHERE,
    OPERATOR_LIKE,
    /* unary */
    OPERATOR_NEGATE = OPERATOR_UNARY,
    OPERATOR_ISNULL,
    OPERATOR_NOTNULL,
    OPERATOR_NOT,
    /* aggregate */
    OPERATOR_SUM,
    OPERATOR_AVG,
    OPERATOR_MIN,
    OPERATOR_MAX,
    OPERATOR_COUNT,
} operator_type_t;

typedef enum {
    TERM_NONE = 0,
    TERM_COLUMN_REF,
    TERM_INT,
    TERM_STRING,
    TERM_DATE,
    TERM_FLOAT,
    TERM_BOOL,
    TERM_LITERAL_LIST,
    TERM_IN_WHERE,
    TERM_NULL
} term_type_t;

typedef struct field_item_t {
    char* name;
    int type, width, flags;
    struct expr_node_t* default_value;
} field_item_t;

typedef struct linked_list_t {
    void* data;
    struct linked_list_t* next;
} linked_list_t;

typedef struct column_ref_t {
    char* table;
    char* column;
} column_ref_t;

typedef struct table_def_t {
    char* name;
    struct linked_list_t* fields;
    struct linked_list_t* constraints;
} table_def_t;

typedef struct insert_info_t {
    char* table;
    linked_list_t *columns, *values;
} insert_info_t;

typedef struct expr_node_t {
    union {
        int val_i;
        float val_f;
        char val_b;
        char* val_s;
        struct column_ref_t* column_ref;
        struct expr_node_t* left;
        struct linked_list_t* literal_list;
        struct select_single_info_t* in_where;
    };
    struct expr_node_t* right;
    operator_type_t op;
    term_type_t term_type;
} expr_node_t;

typedef struct table_constraint_t {
    int type;
    linked_list_t* columns;
    char* index_name;
    char* foreign_name;
    char* foreign_table_ref;
    linked_list_t* foreign_column_ref;
    expr_node_t* check_cond;
} table_constraint_t;

typedef struct delete_info_t {
    char* table;
    expr_node_t* where;
} delete_info_t;

typedef struct update_info_t {
    char* table;
    char* column;
    expr_node_t *where, *value;
} update_info_t;

typedef struct select_info_t {
    linked_list_t *tables, *exprs;
    expr_node_t* where;
} select_info_t;

typedef struct select_single_info_t {
    char* table;
    expr_node_t* expr;
    expr_node_t* where;
} select_single_info_t;

#ifdef __cplusplus
};
#endif
#endif
