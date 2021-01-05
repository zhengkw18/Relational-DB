#ifndef EXECUTE
#define EXECUTE

#include "defs.h"

#ifdef __cplusplus
extern "C" {
#endif

void execute_create_database(const char* db_name);
void execute_use_database(const char* db_name);
void execute_drop_database(const char* db_name);
void execute_show_database(const char* db_name);
void execute_show_databases();
void execute_create_table(const table_def_t* table);
void execute_drop_table(const char* table_name);
void execute_show_table(const char* table_name);
void execute_insert(const insert_info_t* insert_info);
void execute_delete(const delete_info_t* delete_info);
void execute_select(const select_info_t* select_info);
void execute_update(const update_info_t* update_info);
void execute_create_index(const char* table_name, const char* index_name, const linked_list_t* column_list);
void execute_drop_index(const char* table_name, const char* index_name);
void execute_quit();
void execute_add_primary(const char* table_name, const linked_list_t* column_list);
void execute_drop_primary(const char* table_name);
void execute_add_unique(const char* table_name, const char* column_name);
void execute_drop_unique(const char* table_name, const char* column_name);
void execute_add_col(const char* table_name, const field_item_t* field);
void execute_drop_col(const char* table_name, const char* field_name);
void execute_add_foreign(const char* table_name, const char* name, const linked_list_t* cols, const char* name_ref, const linked_list_t* cols_ref);
void execute_drop_foreign(const char* table_name, const char* name);
void execute_rename(const char* table_name, const char* new_name);
void execute_copy_from(const char* table_name, const char* path);

#ifdef __cplusplus
}
#endif

#endif
