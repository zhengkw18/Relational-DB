#ifndef DATABASEMANAGER
#define DATABASEMANAGER
#include "../parser/defs.h"
#include "tablemanager.h"
#include "utils.h"
#include <vector>
struct DatabaseHeader {
    int table_num;
    char tables[MAX_TABLE_NUM][MAX_NAME_LEN];
};
struct Databases {
    int database_num;
    char databases[MAX_DATABASE_NUM][MAX_NAME_LEN];
};
class DatabaseManager {
private:
    char* current_database_name;
    DatabaseHeader current_databaseheader;
    Databases current_databases;
    TableManager* current_tables[MAX_TABLE_NUM];
    DatabaseManager();
    bool AssertUsing();
    bool GetTableId(const char* table_name, int& id);
    bool GetDBId(const char* db_name, int& id);
    void ShowTables(DatabaseHeader& header);

public:
    ~DatabaseManager();
    void CreateDB(const char* db_name);
    void DropDB(const char* db_name);
    void UseDB(const char* db_name);
    void CloseDB();
    void ShowDB(const char* db_name);
    void ShowDBs();

    void CreateTable(const table_def_t* table);
    void ShowTable(const char* table_name);
    void DropTable(const char* table_name);
    void Insert(const insert_info_t* insert_info);
    void CopyFrom(const char* table_name, const char* path);
    void Update(const update_info_t* update_info);
    void Select(const select_info_t* select_info);
    void Delete(const delete_info_t* delete_info);
    void AlterTable_Rename(const char* table_name, const char* new_name);
    void Rename(const char* table_name, const char* new_name);
    void AlterTable_AddPrimary(const char* table_name, const linked_list_t* column_list);
    void AlterTable_DropPrimary(const char* table_name);
    void AlterTable_AddUnique(const char* table_name, const char* field_name);
    void AlterTable_DropUnique(const char* table_name, const char* field_name);
    void AlterTable_AddCol(const char* table_name, const field_item_t* field);
    void AlterTable_DropCol(const char* table_name, const char* field_name);
    void AlterTable_AddForeign(const char* table_name, const char* name, const linked_list_t* cols, const char* name_ref, const linked_list_t* cols_ref);
    void AlterTable_DropForeign(const char* table_name, const char* name);
    void AlterTable_CreateIndex(const char* table_name, const char* index_name, const linked_list_t* column_list);
    void AlterTable_DropIndex(const char* table_name, const char* index_name);
    bool select_from_one_table(const char* table_name, expr_node_t* where, std::set<int>& rids);
    bool select_from_one_table(const char* table_name, std::vector<expr_node_t*> and_exprs, std::set<int>& rids);
    linked_list_t* select_in_where(const char* table_name, expr_node_t* where, expr_node_t* expr);
    TableManager* get_table_manager(const char* table_name);

    static DatabaseManager* get_instance()
    {
        static DatabaseManager dbm;
        return &dbm;
    }
};

#endif