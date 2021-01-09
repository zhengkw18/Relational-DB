#ifndef FREE
#define FREE

#include "defs.h"
template <typename T, typename DataDeleter>
inline void free_linked_list(linked_list_t* linked_list, DataDeleter data_deleter)
{
    for (linked_list_t* l_ptr = linked_list; l_ptr;) {
        T* data = (T*)l_ptr->data;
        data_deleter(data);
        linked_list_t* tmp = l_ptr;
        l_ptr = l_ptr->next;
        free(tmp);
    }
}
inline void free_exprnode(expr_node_t* expr)
{
    if (expr == nullptr)
        return;
    if (expr->op == OPERATOR_NONE) {
        switch (expr->term_type) {
        case TERM_STRING:
            free(expr->val_s);
            break;
        case TERM_COLUMN_REF:
            free(expr->column_ref->table);
            free(expr->column_ref->column);
            free(expr->column_ref);
            break;
        case TERM_LITERAL_LIST:
            free_linked_list<expr_node_t>(expr->literal_list, free_exprnode);
            break;
        case TERM_IN_WHERE:
            free_exprnode(expr->in_where->where);
            free_exprnode(expr->in_where->expr);
            free(expr->in_where->table);
            free(expr->in_where);
            break;
        default:
            break;
        }
    } else {
        free_exprnode(expr->left);
        free_exprnode(expr->right);
    }

    free(expr);
}

inline void free_column_ref(column_ref_t* cref)
{
    if (cref == nullptr)
        return;
    free(cref->column);
    free(cref->table);
    free(cref);
}

inline void free_field_item(field_item_t* item)
{
    free(item->name);
    free_exprnode(item->default_value);
    free(item);
}
inline void free_table_constraint(table_constraint_t* data)
{
    free_exprnode(data->check_cond);
    free_linked_list<char>(data->columns, free);
    free(data->index_name);
    free(data->foreign_name);
    free(data->foreign_table_ref);
    free_linked_list<char>(data->foreign_column_ref, free);
    free(data);
}

inline void free_table_def(table_def_t* table)
{
    free(table->name);
    free_linked_list<table_constraint_t>(table->constraints, free_table_constraint);
    free_linked_list<field_item_t>(table->fields, free_field_item);
    free(table);
}
inline void free_insert_info(insert_info_t* insert_info)
{
    free(insert_info->table);
    free_linked_list<char>(insert_info->columns, free);
    free_linked_list<expr_node_t>(insert_info->values, free_exprnode);
    free(insert_info);
}
inline void free_select_info(select_info_t* select_info)
{
    free_exprnode(select_info->where);
    free_linked_list<expr_node_t>(select_info->exprs, free_exprnode);
    free_linked_list<char>(select_info->tables, free);
    free(select_info);
}
inline void free_update_info(update_info_t* update_info)
{
    free(update_info->table);
    free(update_info->column);
    free_exprnode(update_info->where);
    free_exprnode(update_info->value);
    free(update_info);
}
inline void free_delete_info(delete_info_t* delete_info)
{
    free(delete_info->table);
    free_exprnode(delete_info->where);
    free(delete_info);
}
#endif