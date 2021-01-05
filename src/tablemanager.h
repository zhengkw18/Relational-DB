#ifndef TABLEMANAGER
#define TABLEMANAGER
#include "../parser/defs.h"
#include "indexmanager.h"
#include "recordmanager.h"
#include "utils.h"
#include "varcharmanager.h"
#include <set>
#include <stdio.h>

struct TableHeader {
    int col_num;

    int records_num, check_constaint_num, index_num, primary_num, foreign_num;
    uint bitmap_notnull, bitmap_unique, bitmap_default;
    int col_type[MAX_COL_NUM];
    //------------
    int bitmap_len;
    int record_len;
    int col_length[MAX_COL_NUM];
    int col_aligned_len[MAX_COL_NUM];
    //------------
    char index_name[MAX_INDEX_NUM][MAX_NAME_LEN];
    int index_col_num[MAX_INDEX_NUM];
    int index_cols[MAX_INDEX_NUM][MAX_COL_NUM];

    int primary_cols[MAX_COL_NUM];

    char check_constaints[MAX_CHECK_CONSTRAINT_NUM][MAX_CHECK_CONSTRAINT_LEN];
    char default_values[MAX_COL_NUM][MAX_DEFAULT_LEN];

    char foreign_key_name[MAX_FOREIGN_NUM][MAX_NAME_LEN];
    char foreign_key_ref_table[MAX_FOREIGN_NUM][MAX_NAME_LEN];
    int foreign_key_column[MAX_FOREIGN_NUM][MAX_COL_NUM];
    int foreign_key_col_num[MAX_FOREIGN_NUM];
    int referred_num;

    char col_name[MAX_COL_NUM][MAX_NAME_LEN];
    char table_name[MAX_NAME_LEN];
};

class TableManager {
    TableHeader current_tableHeader;
    RecordManager* current_recordManager;
    VarcharManager* current_varcharManager;
    IndexManager* current_indexManagers[MAX_INDEX_NUM];
    IndexManager* current_primaryManager;
    IndexManager* current_uniqueManagers[MAX_COL_NUM];
    expr_node_t* checks[MAX_CHECK_CONSTRAINT_NUM];
    Expression defaults[MAX_COL_NUM];

public:
    TableManager(const char* tablename);
    ~TableManager();
    static void CreateTable(const TableHeader* tableheader);
    static void DropTable(const char* table_name);
    void AddPrimary(const linked_list_t* column_list);
    void DropPrimary();
    void AddUnique(const char* column_name);
    void DropUnique(const char* column_name);
    void CreateIndex(const char* index_name, const linked_list_t* column_list);
    void DropIndex(const char* index_name);
    void AddForeign(const char* name, const linked_list_t* cols, const char* name_ref, const linked_list_t* cols_ref);
    void DropForeign(const char* name);
    void Show();
    static TableHeader* InitHeader(const char* table_name, const linked_list_t* fields, const linked_list_t* constraints);
    static TableHeader* AddCol(const TableHeader* current, const field_item_t* field);
    static TableHeader* DeleteCol(const TableHeader* current, const char* field_name);
    IndexManager* GetIndexManager(int col);
    int GetColumnID(const char* col);
    void Insert(const linked_list_t* columns, const linked_list_t* values);
    void Update(const char* column, expr_node_t* where, const expr_node_t* value);
    void Delete(expr_node_t* where);
    void Cache(std::set<int>& rids);
    void Cache(Record* record, int rid);
    int GetColumnType(int col);
    void Refer();
    void DeRefer();
    bool is_referred();
    TableHeader* get_header();
    void DropAllForeignKey();
    void AddAllForeignKey();
    IndexManager* GetPrimaryManager();
};
#endif