#include "execute.h"
#include "../src/databasemanager.h"
#include "../src/expression.h"
#include "free.h"
#include <stdio.h>
#include <stdlib.h>
void execute_create_database(const char* db_name)
{
    DatabaseManager::get_instance()->CreateDB(db_name);
    free((char*)db_name);
}
void execute_use_database(const char* db_name)
{
    DatabaseManager::get_instance()->UseDB(db_name);
    free((char*)db_name);
}
void execute_drop_database(const char* db_name)
{
    DatabaseManager::get_instance()->DropDB(db_name);
    free((char*)db_name);
}
void execute_show_database(const char* db_name)
{
    DatabaseManager::get_instance()->ShowDB(db_name);
    free((char*)db_name);
}
void execute_show_databases()
{
    DatabaseManager::get_instance()->ShowDBs();
}
void execute_create_table(const table_def_t* table)
{
    DatabaseManager::get_instance()->CreateTable(table);
    free_table_def((table_def_t*)table);
}
void execute_drop_table(const char* table_name)
{
    DatabaseManager::get_instance()->DropTable(table_name);
    free((char*)table_name);
}
void execute_show_table(const char* table_name)
{
    DatabaseManager::get_instance()->ShowTable(table_name);
    free((char*)table_name);
}
void execute_insert(const insert_info_t* insert_info)
{
    DatabaseManager::get_instance()->Insert(insert_info);
    free_insert_info((insert_info_t*)insert_info);
}
void execute_delete(const delete_info_t* delete_info)
{
    DatabaseManager::get_instance()->Delete(delete_info);
    free_delete_info((delete_info_t*)delete_info);
}
void execute_select(const select_info_t* select_info)
{
    DatabaseManager::get_instance()->Select(select_info);
    free_select_info((select_info_t*)select_info);
}
void execute_update(const update_info_t* update_info)
{
    DatabaseManager::get_instance()->Update(update_info);
    free_update_info((update_info_t*)update_info);
}
void execute_create_index(const char* table_name, const char* index_name, const linked_list_t* column_list)
{
    DatabaseManager::get_instance()->AlterTable_CreateIndex(table_name, index_name, column_list);
    free((char*)table_name);
    free((char*)index_name);
    free_linked_list<char>((linked_list_t*)column_list, free);
}
void execute_drop_index(const char* table_name, const char* index_name)
{
    DatabaseManager::get_instance()->AlterTable_DropIndex(table_name, index_name);
    free((char*)table_name);
    free((char*)index_name);
}
void execute_quit()
{
    DatabaseManager::get_instance()->CloseDB();
}
void execute_add_primary(const char* table_name, const linked_list_t* column_list)
{
    DatabaseManager::get_instance()->AlterTable_AddPrimary(table_name, column_list);
    free((char*)table_name);
    free_linked_list<char>((linked_list_t*)column_list, free);
}
void execute_drop_primary(const char* table_name)
{
    DatabaseManager::get_instance()->AlterTable_DropPrimary(table_name);
    free((char*)table_name);
}
void execute_add_unique(const char* table_name, const char* column_name)
{
    DatabaseManager::get_instance()->AlterTable_AddUnique(table_name, column_name);
    free((char*)table_name);
    free((char*)column_name);
}
void execute_drop_unique(const char* table_name, const char* column_name)
{
    DatabaseManager::get_instance()->AlterTable_DropUnique(table_name, column_name);
    free((char*)table_name);
    free((char*)column_name);
}
void execute_add_col(const char* table_name, const field_item_t* field)
{
    DatabaseManager::get_instance()->AlterTable_AddCol(table_name, field);
    free((char*)table_name);
    free_field_item((field_item_t*)field);
}
void execute_drop_col(const char* table_name, const char* field_name)
{
    DatabaseManager::get_instance()->AlterTable_DropCol(table_name, field_name);
    free((char*)table_name);
    free((char*)field_name);
}
void execute_add_foreign(const char* table_name, const char* name, const linked_list_t* cols, const char* name_ref, const linked_list_t* cols_ref)
{
    DatabaseManager::get_instance()->AlterTable_AddForeign(table_name, name, cols, name_ref, cols_ref);
    free((char*)table_name);
    free((char*)name);
    free((char*)name_ref);
    free_linked_list<char>((linked_list_t*)cols, free);
    free_linked_list<char>((linked_list_t*)cols_ref, free);
}
void execute_drop_foreign(const char* table_name, const char* name)
{
    DatabaseManager::get_instance()->AlterTable_DropForeign(table_name, name);
    free((char*)table_name);
    free((char*)name);
}
void execute_rename(const char* table_name, const char* new_name)
{
    DatabaseManager::get_instance()->AlterTable_Rename(table_name, new_name);
    free((char*)table_name);
    free((char*)new_name);
}
void execute_copy_from(const char* table_name, const char* path)
{
    DatabaseManager::get_instance()->CopyFrom(table_name, path);
    free((char*)table_name);
    free((char*)path);
}