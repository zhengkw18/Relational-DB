#include "tablemanager.h"
#include "databasemanager.h"
#include "expression.h"
#include <map>
TableManager::TableManager(const char* tablename)
{
    FILE* headerfile = fopen((std::string(tablename) + ".header").c_str(), "r");
    fread(&current_tableHeader, sizeof(TableHeader), 1, headerfile);
    fclose(headerfile);
    std::string tablename_str = std::string(tablename);
    current_varcharManager = new VarcharManager((tablename_str + ".varchar").c_str());
    current_recordManager = new RecordManager((tablename_str + ".record").c_str(), current_varcharManager);
    strcpy(current_tableHeader.table_name, tablename);
    if (current_tableHeader.primary_num > 0) {
        current_primaryManager = new IndexManager((tablename_str + ".primary").c_str());
    }
    if (current_tableHeader.bitmap_unique != 0) {
        for (int i = 0; i < current_tableHeader.col_num; i++) {
            if (getBit(&current_tableHeader.bitmap_unique, i)) {
                string uniquename = std::string(current_tableHeader.table_name) + ".unique." + std::string(current_tableHeader.col_name[i]);
                current_uniqueManagers[i] = new IndexManager(uniquename.c_str());
            }
        }
    }
    if (current_tableHeader.index_num > 0) {
        for (int i = 0; i < current_tableHeader.index_num; i++) {
            string indexname = std::string(current_tableHeader.table_name) + ".index." + std::string(current_tableHeader.index_name[i]);
            current_indexManagers[i] = new IndexManager(indexname.c_str());
        }
    }
    if (current_tableHeader.bitmap_default > 0) {
        for (int i = 0; i < current_tableHeader.col_num; i++) {
            if (getBit(&current_tableHeader.bitmap_default, i)) {
                std::istringstream is(std::string(current_tableHeader.default_values[i]));
                expr_node_t* expr = expression::from_poland(is);
                defaults[i] = expression::expr_to_Expr(expr);
                free_exprnode(expr);
            }
        }
    }
    for (int i = 0; i < current_tableHeader.check_constaint_num; i++) {
        std::istringstream is(std::string(current_tableHeader.check_constaints[i]));
        checks[i] = expression::from_poland(is);
    }
}
TableManager::~TableManager()
{
    FILE* headerfile = fopen((std::string(current_tableHeader.table_name) + ".header").c_str(), "wb+");
    fwrite(&current_tableHeader, sizeof(TableHeader), 1, headerfile);
    fclose(headerfile);
    delete current_recordManager;
    delete current_varcharManager;
    if (current_tableHeader.primary_num > 0) {
        delete current_primaryManager;
    }
    if (current_tableHeader.bitmap_unique != 0) {
        for (int i = 0; i < current_tableHeader.col_num; i++) {
            if (getBit(&current_tableHeader.bitmap_unique, i)) {
                delete current_uniqueManagers[i];
            }
        }
    }
    if (current_tableHeader.index_num > 0) {
        for (int i = 0; i < current_tableHeader.index_num; i++) {
            delete current_indexManagers[i];
        }
    }
    if (current_tableHeader.bitmap_default > 0) {
        for (int i = 0; i < current_tableHeader.col_num; i++) {
            if (getBit(&current_tableHeader.bitmap_default, i)) {
                if (defaults[i].type == TERM_STRING) {
                    free(defaults[i].val_s);
                }
            }
        }
    }
    for (int i = 0; i < current_tableHeader.check_constaint_num; i++) {
        free_exprnode(checks[i]);
    }
}
void TableManager::CreateTable(const TableHeader* tableheader)
{
    std::string tablename = std::string(tableheader->table_name);
    FILE* headerfile = fopen((tablename + ".header").c_str(), "wb+");
    fwrite(tableheader, sizeof(TableHeader), 1, headerfile);
    fclose(headerfile);
    RecordManager::CreateFile((tablename + ".record").c_str(), tableheader);
    VarcharManager::CreateFile((tablename + ".varchar").c_str());
    if (tableheader->primary_num > 0) {
        string primaryname = std::string(tableheader->table_name) + ".primary";
        IndexManager::CreateIndex(primaryname.c_str(), tableheader->primary_num, tableheader->primary_cols, tableheader);
    }
    if (tableheader->bitmap_unique != 0) {
        for (int i = 0; i < tableheader->col_num; i++) {
            if (getBit(&tableheader->bitmap_unique, i)) {
                string uniquename = std::string(tableheader->table_name) + ".unique." + std::string(tableheader->col_name[i]);
                int cols[] = { i };
                IndexManager::CreateIndex(uniquename.c_str(), 1, cols, tableheader);
            }
        }
    }
    if (tableheader->index_num > 0) {
        for (int i = 0; i < tableheader->index_num; i++) {
            string indexname = std::string(tableheader->table_name) + ".index." + std::string(tableheader->index_name[i]);
            IndexManager::CreateIndex(indexname.c_str(), tableheader->index_col_num[i], tableheader->index_cols[i], tableheader);
        }
    }
}
void TableManager::DropTable(const char* tablename)
{
    system(("ls " + std::string(tablename) + "* | xargs rm -rf").c_str());
}
void TableManager::AddPrimary(const linked_list_t* column_list)
{
    if (current_tableHeader.primary_num > 0) {
        fprintf(stderr, "[Error] Primary key already exists.\n");
    } else {
        string primaryname = std::string(current_tableHeader.table_name) + ".primary";
        std::vector<int> cols;
        for (const linked_list_t* link_ptr = column_list; link_ptr; link_ptr = link_ptr->next) {
            int id = GetColumnID((char*)link_ptr->data);
            if (id == -1) {
                fprintf(stderr, "[Error] Column not found.\n");
                return;
            }
            cols.push_back(id);
        }
        std::reverse(cols.begin(), cols.end());
        int* cols_arr = new int[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            cols_arr[i] = cols[i];
        }
        IndexManager::CreateIndex(primaryname.c_str(), cols.size(), cols_arr, &current_tableHeader);
        delete[] cols_arr;
        current_primaryManager = new IndexManager(primaryname.c_str());
        current_tableHeader.primary_num = cols.size();
        for (int i = 0; i < cols.size(); i++) {
            current_tableHeader.primary_cols[i] = cols[i];
        }
        Record* record;
        if (current_recordManager->InitializeIteration()) {
            do {
                record = current_recordManager->CurrentRecord();
                if (!current_primaryManager->CheckNotNull(record) || current_primaryManager->Exists(record, -1)) {
                    DropPrimary();
                    fprintf(stderr, "[Error] Primary constraints failed.\n");
                    delete record;
                    return;
                }
                current_primaryManager->Insert(record);
                delete record;
            } while (current_recordManager->NextRecord());
        }
    }
}
void TableManager::DropPrimary()
{
    if (current_tableHeader.primary_num > 0) {
        if (is_referred()) {
            fprintf(stderr, "[Error] Primary key is referred.\n");
            return;
        }
        delete current_primaryManager;
        IndexManager::DeleteIndex((std::string(current_tableHeader.table_name) + ".primary").c_str());
        current_tableHeader.primary_num = 0;
    } else {
        fprintf(stderr, "[Error] No primary key exists.\n");
    }
}
void TableManager::AddUnique(const char* column_name)
{
    int col_id = GetColumnID(column_name);
    if (col_id == -1) {
        fprintf(stderr, "[Error] Column not found.\n");
        return;
    }
    if (getBit(&current_tableHeader.bitmap_unique, col_id)) {
        fprintf(stderr, "[Error] Column already has unique constraint.\n");
        return;
    } else {
        setBit(&current_tableHeader.bitmap_unique, col_id, true);
        string uniquename = std::string(current_tableHeader.table_name) + ".unique." + std::string(current_tableHeader.col_name[col_id]);
        int cols[] = { col_id };
        IndexManager::CreateIndex(uniquename.c_str(), 1, cols, &current_tableHeader);
        current_uniqueManagers[col_id] = new IndexManager(uniquename.c_str());
        Record* record;
        if (current_recordManager->InitializeIteration()) {
            do {
                record = current_recordManager->CurrentRecord();
                if (current_uniqueManagers[col_id]->Exists(record, -1)) {
                    DropUnique(column_name);
                    fprintf(stderr, "[Error] Unique constraints failed.\n");
                    delete record;
                    return;
                }
                current_uniqueManagers[col_id]->Insert(record);
                delete record;
            } while (current_recordManager->NextRecord());
        }
    }
}
void TableManager::DropUnique(const char* column_name)
{
    int col_id = GetColumnID(column_name);
    if (col_id == -1) {
        fprintf(stderr, "[Error] Column not found.\n");
        return;
    }
    if (!getBit(&current_tableHeader.bitmap_unique, col_id)) {
        fprintf(stderr, "[Error] No unique constraint exists.\n");
        return;
    } else {
        delete current_uniqueManagers[col_id];
        string uniquename = std::string(current_tableHeader.table_name) + ".unique." + std::string(current_tableHeader.col_name[col_id]);
        IndexManager::DeleteIndex(uniquename.c_str());
        setBit(&current_tableHeader.bitmap_unique, col_id, false);
    }
}
void TableManager::CreateIndex(const char* index_name, const linked_list_t* column_list)
{
    int current_index_num = current_tableHeader.index_num;
    if (current_index_num >= MAX_INDEX_NUM) {
        fprintf(stderr, "[Error] Too many indexes.\n");
        return;
    }
    if (strlen(index_name) + 1 > MAX_NAME_LEN) {
        fprintf(stderr, "[Error] Index name too long.\n");
        return;
    }
    for (int i = 0; i < current_tableHeader.index_num; i++) {
        if (strcmp(current_tableHeader.index_name[i], index_name) == 0) {
            fprintf(stderr, "[Error] Duplicate index name.\n");
            return;
        }
    }
    std::vector<int> cols;
    for (const linked_list_t* link_ptr = column_list; link_ptr; link_ptr = link_ptr->next) {
        int id = GetColumnID((char*)link_ptr->data);
        if (id == -1) {
            fprintf(stderr, "[Error] Column not found.\n");
            return;
        }
        cols.push_back(id);
    }
    std::reverse(cols.begin(), cols.end());
    current_tableHeader.index_col_num[current_index_num] = cols.size();
    for (int i = 0; i < cols.size(); i++) {
        current_tableHeader.index_cols[current_index_num][i] = cols[i];
    }
    strcpy(current_tableHeader.index_name[current_index_num], index_name);
    string indexname_str = std::string(current_tableHeader.table_name) + ".index." + std::string(index_name);
    IndexManager::CreateIndex(indexname_str.c_str(), cols.size(), current_tableHeader.index_cols[current_index_num], &current_tableHeader);
    IndexManager* index_manager = new IndexManager(indexname_str.c_str());
    current_indexManagers[current_index_num] = index_manager;
    current_tableHeader.index_num++;
    Record* record;
    if (current_recordManager->InitializeIteration()) {
        do {
            record = current_recordManager->CurrentRecord();
            index_manager->Insert(record);
            delete record;
        } while (current_recordManager->NextRecord());
    }
}
void TableManager::DropIndex(const char* indexname)
{
    int col = -1;
    for (int i = 0; i < current_tableHeader.index_num; i++) {
        if (strcmp(current_tableHeader.index_name[i], indexname) == 0) {
            col = i;
            break;
        }
    }
    if (col == -1) {
        fprintf(stderr, "[Error] Index not found.\n");
        return;
    }
    string indexname_str = std::string(current_tableHeader.table_name) + ".index." + std::string(indexname);
    delete current_indexManagers[col];
    IndexManager::DeleteIndex(indexname_str.c_str());
    for (int i = col; i < current_tableHeader.index_num - 1; i++) {
        memcpy(current_tableHeader.index_name[i], current_tableHeader.index_name[i + 1], MAX_NAME_LEN);
        memcpy(current_tableHeader.index_cols[i], current_tableHeader.index_cols[i + 1], MAX_COL_NUM * sizeof(int));
        current_tableHeader.index_col_num[i] = current_tableHeader.index_col_num[i + 1];
        current_indexManagers[i] = current_indexManagers[i + 1];
    }
    current_tableHeader.index_num--;
}
void TableManager::AddForeign(const char* name, const linked_list_t* cols, const char* name_ref, const linked_list_t* cols_ref)
{
    if (strcmp(name_ref, current_tableHeader.table_name) == 0) {
        fprintf(stderr, "[Error] Cannot refer to self.\n");
        return;
    }
    TableManager* table_ref = DatabaseManager::get_instance()->get_table_manager(name_ref);
    if (table_ref == nullptr) {
        fprintf(stderr, "[Error] Referred table not found.\n");
        return;
    }
    if (strlen(name) + 1 > MAX_NAME_LEN) {
        fprintf(stderr, "[Error] Foreign key name too long.\n");
        return;
    }
    for (int i = 0; i < current_tableHeader.foreign_num; i++) {
        if (strcmp(current_tableHeader.foreign_key_name[i], name) == 0) {
            fprintf(stderr, "[Error] Duplicate foreign name.\n");
            return;
        }
    }
    std::vector<std::string> cols_str;
    std::vector<std::string> cols_ref_str;
    for (const linked_list_t* link_ptr = cols; link_ptr; link_ptr = link_ptr->next) {
        cols_str.push_back((char*)link_ptr->data);
    }
    for (const linked_list_t* link_ptr = cols_ref; link_ptr; link_ptr = link_ptr->next) {
        cols_ref_str.push_back((char*)link_ptr->data);
    }
    if (cols_str.size() != cols_ref_str.size()) {
        fprintf(stderr, "[Error] Column number not match.\n");
        return;
    }
    TableHeader* header_ref = table_ref->get_header();
    if (cols_str.size() != header_ref->primary_num) {
        fprintf(stderr, "[Error] Referred columns should be primary.\n");
        return;
    }
    std::vector<int> col_ids;
    for (int i = 0; i < header_ref->primary_num; i++) {
        int id = -1;
        for (int j = 0; j < cols_ref_str.size(); j++) {
            if (strcmp(cols_ref_str[j].c_str(), header_ref->col_name[header_ref->primary_cols[i]]) == 0) {
                id = j;
                break;
            }
        }
        if (id == -1) {
            fprintf(stderr, "[Error] Referred columns should be primary.\n");
            return;
        }
        int col_id = GetColumnID(cols_str[id].c_str());
        if (col_id == -1) {
            fprintf(stderr, "[Error] Column not found.\n");
            return;
        }
        col_ids.push_back(col_id);
    }
    //check type compatible
    for (int i = 0; i < col_ids.size(); i++) {
        if (current_tableHeader.col_type[col_ids[i]] != header_ref->col_type[header_ref->primary_cols[i]]) {
            fprintf(stderr, "[Error] Type incompatible.\n");
            return;
        }
    }
    //check existed data
    int current_foreign_num = current_tableHeader.foreign_num;
    strcpy(current_tableHeader.foreign_key_name[current_foreign_num], name);
    strcpy(current_tableHeader.foreign_key_ref_table[current_foreign_num], name_ref);
    current_tableHeader.foreign_key_col_num[current_foreign_num] = col_ids.size();
    for (int i = 0; i < col_ids.size(); i++) {
        current_tableHeader.foreign_key_column[current_foreign_num][i] = col_ids[i];
    }
    current_tableHeader.foreign_num++;
    table_ref->Refer();
    Record *record, *part;
    if (current_recordManager->InitializeIteration()) {
        do {
            record = current_recordManager->CurrentRecord();
            part = current_recordManager->GetParser()->toRecordPart(record, current_tableHeader.foreign_key_column[current_foreign_num], header_ref->primary_num);
            if (!table_ref->GetPrimaryManager()->ExistsPart(part, -1)) {
                DropForeign(name);
                fprintf(stderr, "[Error] Foreign constraints failed.\n");
                delete record;
                delete part;
                return;
            }
            delete record;
            delete part;
        } while (current_recordManager->NextRecord());
    }
}
void TableManager::DropForeign(const char* name)
{
    int col = -1;
    for (int i = 0; i < current_tableHeader.foreign_num; i++) {
        if (strcmp(current_tableHeader.foreign_key_name[i], name) == 0) {
            col = i;
            break;
        }
    }
    if (col == -1) {
        fprintf(stderr, "[Error] Foreign key not found.\n");
        return;
    }
    TableManager* table_ref = DatabaseManager::get_instance()->get_table_manager(current_tableHeader.foreign_key_ref_table[col]);
    table_ref->DeRefer();
    for (int i = col; i < current_tableHeader.foreign_num - 1; i++) {
        memcpy(current_tableHeader.foreign_key_name[i], current_tableHeader.foreign_key_name[i + 1], MAX_NAME_LEN);
        memcpy(current_tableHeader.foreign_key_ref_table[i], current_tableHeader.foreign_key_ref_table[i + 1], MAX_NAME_LEN);
        memcpy(current_tableHeader.foreign_key_column[i], current_tableHeader.foreign_key_column[i + 1], MAX_COL_NUM * sizeof(int));
        current_tableHeader.foreign_key_col_num[i] = current_tableHeader.foreign_key_col_num[i + 1];
    }
    current_tableHeader.foreign_num--;
}
void TableManager::Show()
{
    std::printf("======== Table Info Begin ========\n");
    std::printf("Table name  : %s\n", current_tableHeader.table_name);
    std::printf("Column num  : %d\n", current_tableHeader.col_num);
    std::printf("Record num  : %d\n", current_tableHeader.records_num);
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        std::printf("  [column] name = %s, type = ", current_tableHeader.col_name[i]);
        switch (current_tableHeader.col_type[i]) {
        case TYPE_INT:
            std::printf("INT");
            break;
        case TYPE_FLOAT:
            std::printf("FLOAT");
            break;
        case TYPE_DATE:
            std::printf("DATE");
            break;
        case TYPE_CHAR:
            std::printf("CHAR(%d)", current_tableHeader.col_length[i]);
            break;
        case TYPE_VARCHAR:
            std::printf("VARCHAR(%d)", current_tableHeader.col_length[i]);
            break;
        default:
            std::printf("UNKNOWN");
            break;
        }

        std::printf(", flags = ");
        if (getBit(&current_tableHeader.bitmap_notnull, i))
            std::printf("NOT NULL ");
        if (getBit(&current_tableHeader.bitmap_unique, i))
            std::printf("UNIQUE ");
        if (getBit(&current_tableHeader.bitmap_default, i)) {
            std::istringstream is(std::string(current_tableHeader.default_values[i]));
            std::printf("DEFAULT %s ", expression::poland_to_string(is).c_str());
        }
        std::puts("");
    }
    std::printf("  [primary] ");
    for (int i = 0; i < current_tableHeader.primary_num; i++) {
        std::printf("%s ", current_tableHeader.col_name[current_tableHeader.primary_cols[i]]);
    }
    std::puts("");
    for (int i = 0; i < current_tableHeader.foreign_num; i++) {
        std::printf("  [foreign] %s : ", current_tableHeader.foreign_key_name[i]);
        for (int j = 0; j < current_tableHeader.foreign_key_col_num[i]; j++) {
            std::printf("%s ", current_tableHeader.col_name[current_tableHeader.foreign_key_column[i][j]]);
        }
        std::printf("REFERENCES %s", current_tableHeader.foreign_key_ref_table[i]);
        std::puts("");
    }
    for (int i = 0; i < current_tableHeader.index_num; i++) {
        std::printf("  [index] %s : ", current_tableHeader.index_name[i]);
        for (int j = 0; j < current_tableHeader.index_col_num[i]; j++) {
            std::printf("%s ", current_tableHeader.col_name[current_tableHeader.index_cols[i][j]]);
        }
        std::puts("");
    }
    for (int i = 0; i < current_tableHeader.check_constaint_num; i++) {
        std::istringstream is(std::string(current_tableHeader.check_constaints[i]));
        std::printf("  [check] %s", expression::poland_to_string(is).c_str());
        std::puts("");
    }

    std::printf("======== Table Info End   ========\n");
}
TableHeader* TableManager::InitHeader(const char* table_name, const linked_list_t* fields, const linked_list_t* constraints)
{
    std::vector<field_item_t*> fields_vec;
    std::vector<table_constraint_t*> constraints_vec;
    for (const linked_list_t* link_ptr = fields; link_ptr; link_ptr = link_ptr->next) {
        fields_vec.push_back((field_item_t*)link_ptr->data);
    }
    for (const linked_list_t* link_ptr = constraints; link_ptr; link_ptr = link_ptr->next) {
        constraints_vec.push_back((table_constraint_t*)link_ptr->data);
    }
    std::reverse(fields_vec.begin(), fields_vec.end());
    std::reverse(constraints_vec.begin(), constraints_vec.end());
    TableHeader header;
    memset(&header, 0, sizeof(TableHeader));
    std::map<std::string, int> name_to_id;
    if (fields_vec.size() > MAX_COL_NUM) {
        fprintf(stderr, "[Error] Too many columns.\n");
        return nullptr;
    }
    header.col_num = fields_vec.size();
    for (int i = 0; i < header.col_num; i++) {
        field_item_t* field_item = fields_vec[i];
        if (strlen(field_item->name) + 1 >= MAX_NAME_LEN) {
            fprintf(stderr, "[Error] Column name too long.\n");
            return nullptr;
        }
        std::string name_str = field_item->name;
        if (name_to_id.count(name_str) > 0) {
            fprintf(stderr, "[Error] Column name duplicated.\n");
            return nullptr;
        }
        name_to_id[name_str] = i;
        strcpy(header.col_name[i], field_item->name);
        switch (field_item->type) {
        case FIELD_TYPE_INT:
            header.col_type[i] = TYPE_INT;
            header.col_length[i] = 4;
            break;
        case FIELD_TYPE_FLOAT:
            header.col_type[i] = TYPE_FLOAT;
            header.col_length[i] = 4;
            break;
        case FIELD_TYPE_DATE:
            header.col_type[i] = TYPE_DATE;
            header.col_length[i] = 4;
            break;
        case FIELD_TYPE_CHAR:
            header.col_type[i] = TYPE_CHAR;
            header.col_length[i] = field_item->width;
            break;
        case FIELD_TYPE_VARCHAR:
            header.col_type[i] = TYPE_VARCHAR;
            header.col_length[i] = field_item->width;
            break;
        default:
            assert(false);
            break;
        }
        if (header.col_length[i] <= 0) {
            fprintf(stderr, "[Error] Invalid field width.\n");
            return nullptr;
        }
        if (field_item->flags & FIELD_FLAG_NOTNULL)
            setBit(&header.bitmap_notnull, i, true);
        if (field_item->flags & FIELD_FLAG_UNIQUE)
            setBit(&header.bitmap_unique, i, true);
        if (field_item->default_value != nullptr) {
            Expression expr = expression::eval(field_item->default_value);
            if (!expression::type_compatible(header.col_type[i], expr)) {
                fprintf(stderr, "[Error] Default type incompatible.\n");
                return nullptr;
            }
            std::string default_str = expression::to_poland(field_item->default_value);
            if (strlen(default_str.c_str()) + 1 >= MAX_DEFAULT_LEN) {
                fprintf(stderr, "[Error] Default too long.\n");
                return nullptr;
            }
            strcpy(header.default_values[i], default_str.c_str());
            setBit(&header.bitmap_default, i, 1);
        }
    }
    bool has_primary = false;
    for (table_constraint_t* constraint : constraints_vec) {
        switch (constraint->type) {
        case TABLE_CONSTRAINT_PRIMARY_KEY: {
            if (has_primary) {
                fprintf(stderr, "[Error] Multiple definition of primary key.\n");
                return nullptr;
            }
            has_primary = true;
            std::vector<std::string> primary_cols;
            for (linked_list_t* link_ptr = constraint->columns; link_ptr; link_ptr = link_ptr->next) {
                primary_cols.push_back(std::string((char*)link_ptr->data));
            }
            std::reverse(primary_cols.begin(), primary_cols.end());
            for (std::string primary_col : primary_cols) {
                if (name_to_id.count(primary_col) == 0) {
                    fprintf(stderr, "[Error] Primary key column not found.\n");
                    return nullptr;
                }
                header.primary_cols[header.primary_num] = name_to_id[primary_col];
                header.primary_num++;
            }
            break;
        }
        case TABLE_CONSTRAINT_CHECK: {
            if (header.check_constaint_num >= MAX_CHECK_CONSTRAINT_NUM) {
                fprintf(stderr, "[Error] Check constraints too many.\n");
                return nullptr;
            }
            std::vector<column_ref_t*> col_refs;
            expression::extract_col_refs(constraint->check_cond, col_refs);
            for (column_ref_t* col_ref : col_refs) {
                if (col_ref->table != nullptr) {
                    fprintf(stderr, "[Error] Table name should not appear in check constraints.\n");
                    return nullptr;
                }
            }
            std::string constraint_str = expression::to_poland(constraint->check_cond);
            if (strlen(constraint_str.c_str()) + 1 >= MAX_CHECK_CONSTRAINT_LEN) {
                fprintf(stderr, "[Error] Check constraint too long.\n");
                return nullptr;
            }
            strcpy(header.check_constaints[header.check_constaint_num], constraint_str.c_str());
            header.check_constaint_num++;
            break;
        }
        case TABLE_CONSTRAINT_FOREIGN_KEY: {
            if (strcmp(constraint->foreign_table_ref, table_name) == 0) {
                fprintf(stderr, "[Error] Cannot refer to self.\n");
                return nullptr;
            }
            TableManager* table_ref = DatabaseManager::get_instance()->get_table_manager(constraint->foreign_table_ref);
            if (table_ref == nullptr) {
                fprintf(stderr, "[Error] Referred table not found.\n");
                return nullptr;
            }
            if (strlen(constraint->foreign_name) + 1 > MAX_NAME_LEN) {
                fprintf(stderr, "[Error] Foreign key name too long.\n");
                return nullptr;
            }
            for (int i = 0; i < header.foreign_num; i++) {
                if (strcmp(header.foreign_key_name[i], constraint->foreign_name) == 0) {
                    fprintf(stderr, "[Error] Duplicate foreign name.\n");
                    return nullptr;
                }
            }
            std::vector<std::string> cols_str;
            std::vector<std::string> cols_ref_str;
            for (const linked_list_t* link_ptr = constraint->columns; link_ptr; link_ptr = link_ptr->next) {
                cols_str.push_back((char*)link_ptr->data);
            }
            for (const linked_list_t* link_ptr = constraint->foreign_column_ref; link_ptr; link_ptr = link_ptr->next) {
                cols_ref_str.push_back((char*)link_ptr->data);
            }
            if (cols_str.size() != cols_ref_str.size()) {
                fprintf(stderr, "[Error] Column number not match.\n");
                return nullptr;
            }
            TableHeader* header_ref = table_ref->get_header();
            if (cols_str.size() != header_ref->primary_num) {
                fprintf(stderr, "[Error] Referred columns should be primary.\n");
                return nullptr;
            }
            std::vector<int> col_ids;
            for (int i = 0; i < header_ref->primary_num; i++) {
                int id = -1;
                for (int j = 0; j < cols_ref_str.size(); j++) {
                    if (strcmp(cols_ref_str[j].c_str(), header_ref->col_name[header_ref->primary_cols[i]]) == 0) {
                        id = j;
                        break;
                    }
                }
                if (id == -1) {
                    fprintf(stderr, "[Error] Referred columns should be primary.\n");
                    return nullptr;
                }
                int col_id = -1;
                for (int j = 0; j < header.col_num; j++) {
                    if (strcmp(header.col_name[j], cols_str[id].c_str()) == 0) {
                        col_id = j;
                        break;
                    }
                }
                if (col_id == -1) {
                    fprintf(stderr, "[Error] Column not found.\n");
                    return nullptr;
                }
                col_ids.push_back(col_id);
            }
            //check type compatible
            for (int i = 0; i < col_ids.size(); i++) {
                if (header.col_type[col_ids[i]] != header_ref->col_type[header_ref->primary_cols[i]]) {
                    fprintf(stderr, "[Error] Type incompatible.\n");
                    return nullptr;
                }
            }
            int current_foreign_num = header.foreign_num;
            strcpy(header.foreign_key_name[current_foreign_num], constraint->foreign_name);
            strcpy(header.foreign_key_ref_table[current_foreign_num], constraint->foreign_table_ref);
            header.foreign_key_col_num[current_foreign_num] = col_ids.size();
            for (int i = 0; i < col_ids.size(); i++) {
                header.foreign_key_column[current_foreign_num][i] = col_ids[i];
            }
            header.foreign_num++;
            break;
        }
        case TABLE_CONSTRAINT_INDEX: {
            int current_index_num = header.index_num;
            if (current_index_num >= MAX_INDEX_NUM) {
                fprintf(stderr, "[Error] Too many indexes.\n");
                return nullptr;
            }
            if (strlen(constraint->index_name) + 1 > MAX_NAME_LEN) {
                fprintf(stderr, "[Error] Index name too long.\n");
                return nullptr;
            }
            for (int i = 0; i < header.index_num; i++) {
                if (strcmp(header.index_name[i], constraint->index_name) == 0) {
                    fprintf(stderr, "[Error] Duplicate index name.\n");
                    return nullptr;
                }
            }
            std::vector<int> cols;
            for (const linked_list_t* link_ptr = constraint->columns; link_ptr; link_ptr = link_ptr->next) {
                int col_id = -1;
                for (int j = 0; j < header.col_num; j++) {
                    if (strcmp(header.col_name[j], (char*)link_ptr->data) == 0) {
                        col_id = j;
                        break;
                    }
                }
                if (col_id == -1) {
                    fprintf(stderr, "[Error] Column not found.\n");
                    return nullptr;
                }
                cols.push_back(col_id);
            }
            std::reverse(cols.begin(), cols.end());
            header.index_col_num[current_index_num] = cols.size();
            for (int i = 0; i < cols.size(); i++) {
                header.index_cols[current_index_num][i] = cols[i];
            }
            strcpy(header.index_name[current_index_num], constraint->index_name);
            header.index_num++;
            break;
        }
        default:
            assert(false);
        }
    }
    header.bitmap_len = ceil(header.col_num, 32);
    header.record_len = header.bitmap_len;
    for (int i = 0; i < header.col_num; i++) {
        if (header.col_type[i] == TYPE_VARCHAR)
            header.col_aligned_len[i] = 1;
        else if (header.col_type[i] == TYPE_CHAR)
            header.col_aligned_len[i] = ceil(header.col_length[i] + 1, 4);
        else
            header.col_aligned_len[i] = ceil(header.col_length[i], 4);
        header.record_len += header.col_aligned_len[i];
    }
    TableHeader* header_ptr = new TableHeader;
    *header_ptr = header;
    strcpy(header_ptr->table_name, table_name);
    return header_ptr;
}
TableHeader* TableManager::AddCol(const TableHeader* current, const field_item_t* field)
{
    TableHeader newheader;
    memcpy(&newheader, current, sizeof(TableHeader));
    if (current->col_num == MAX_COL_NUM) {
        fprintf(stderr, "[Error] Too many columns.\n");
        return nullptr;
    }
    if (strlen(field->name) + 1 > MAX_NAME_LEN) {
        fprintf(stderr, "[Error] Column name too long.\n");
        return nullptr;
    }
    for (int i = 0; i < current->col_num; i++) {
        if (strcmp(current->col_name[i], field->name) == 0) {
            fprintf(stderr, "[Error] Column name duplicated.\n");
            return nullptr;
        }
    }
    if (current->records_num > 0) {
        if (field->flags & FIELD_FLAG_UNIQUE) {
            fprintf(stderr, "[Error] New column should not have unique constraint when table is not empty.\n");
            return nullptr;
        }
        if ((field->flags & FIELD_FLAG_NOTNULL) && field->default_value == nullptr) {
            fprintf(stderr, "[Error] New column should have default constraint when table is not empty and not null constraint is set.\n");
            return nullptr;
        }
    }
    int id = current->col_num;
    strcpy(newheader.col_name[id], field->name);
    switch (field->type) {
    case FIELD_TYPE_INT:
        newheader.col_type[id] = TYPE_INT;
        newheader.col_length[id] = 4;
        break;
    case FIELD_TYPE_FLOAT:
        newheader.col_type[id] = TYPE_FLOAT;
        newheader.col_length[id] = 4;
        break;
    case FIELD_TYPE_DATE:
        newheader.col_type[id] = TYPE_DATE;
        newheader.col_length[id] = 4;
        break;
    case FIELD_TYPE_CHAR:
        newheader.col_type[id] = TYPE_CHAR;
        newheader.col_length[id] = field->width;
        break;
    case FIELD_TYPE_VARCHAR:
        newheader.col_type[id] = TYPE_VARCHAR;
        newheader.col_length[id] = field->width;
        break;
    default:
        assert(false);
        break;
    }
    if (field <= 0) {
        fprintf(stderr, "[Error] Invalid field width.\n");
        return nullptr;
    }

    if (field->flags & FIELD_FLAG_NOTNULL)
        setBit(&newheader.bitmap_notnull, id, true);
    if (field->flags & FIELD_FLAG_UNIQUE)
        setBit(&newheader.bitmap_unique, id, true);
    if (field->default_value != nullptr) {
        Expression expr = expression::eval(field->default_value);
        if (!expression::type_compatible(newheader.col_type[id], expr)) {
            fprintf(stderr, "[Error] Default type incompatible.\n");
            return nullptr;
        }
        std::string default_str = expression::to_poland(field->default_value);
        if (strlen(default_str.c_str()) + 1 >= MAX_DEFAULT_LEN) {
            fprintf(stderr, "[Error] Default too long.\n");
            return nullptr;
        }
        strcpy(newheader.default_values[id], default_str.c_str());
        setBit(&newheader.bitmap_default, id, 1);
    }
    newheader.records_num = 0;
    newheader.col_num++;
    newheader.bitmap_len = ceil(newheader.col_num, 32);
    newheader.record_len = newheader.bitmap_len;
    for (int i = 0; i < newheader.col_num; i++) {
        if (newheader.col_type[i] == TYPE_VARCHAR)
            newheader.col_aligned_len[i] = 1;
        else if (newheader.col_type[i] == TYPE_CHAR)
            newheader.col_aligned_len[i] = ceil(newheader.col_length[i] + 1, 4);
        else
            newheader.col_aligned_len[i] = ceil(newheader.col_length[i], 4);
        newheader.record_len += newheader.col_aligned_len[i];
    }
    TableHeader* header_ptr = new TableHeader;
    *header_ptr = newheader;
    return header_ptr;
}
TableHeader* TableManager::DeleteCol(const TableHeader* current, const char* field_name)
{
    TableHeader newheader;
    memset(&newheader, 0, sizeof(TableHeader));
    if (current->referred_num > 0) {
        fprintf(stderr, "[Error] The table is referred.\n");
        return nullptr;
    }
    int id = -1;
    for (int i = 0; i < current->col_num; i++) {
        if (strcmp(current->col_name[i], field_name) == 0) {
            id = i;
            break;
        }
    }
    if (id == -1) {
        fprintf(stderr, "[Error] Column not found.\n");
        return nullptr;
    }
    if (current->col_num == 1) {
        fprintf(stderr, "[Error] Can not delete the only column.\n");
        return nullptr;
    }
    strcpy(newheader.table_name, current->table_name);
    for (int col = 0; col < current->col_num; col++) {
        if (col == id)
            continue;
        int new_col = newheader.col_num;
        strcpy(newheader.col_name[new_col], current->col_name[col]);
        newheader.col_length[new_col] = current->col_length[col];
        newheader.col_type[new_col] = current->col_type[col];
        if (getBit(&current->bitmap_notnull, col)) {
            setBit(&newheader.bitmap_notnull, new_col, true);
        }
        if (getBit(&current->bitmap_unique, col)) {
            setBit(&newheader.bitmap_unique, new_col, true);
        }
        if (getBit(&current->bitmap_default, col)) {
            setBit(&newheader.bitmap_default, new_col, true);
            memcpy(newheader.default_values[new_col], current->default_values[col], MAX_DEFAULT_LEN);
        }
        newheader.col_num++;
    }
    newheader.records_num = 0;
    newheader.bitmap_len = ceil(newheader.col_num, 32);
    newheader.record_len = newheader.bitmap_len;
    for (int i = 0; i < newheader.col_num; i++) {
        if (newheader.col_type[i] == TYPE_VARCHAR)
            newheader.col_aligned_len[i] = 1;
        else if (newheader.col_type[i] == TYPE_CHAR)
            newheader.col_aligned_len[i] = ceil(newheader.col_length[i] + 1, 4);
        else
            newheader.col_aligned_len[i] = ceil(newheader.col_length[i], 4);
        newheader.record_len += newheader.col_aligned_len[i];
    }
    //add primary
    bool flag = false;
    for (int i = 0; i < current->primary_num; i++) {
        if (current->primary_cols[i] == id) {
            flag = true;
            break;
        }
    }
    if (flag) {
        newheader.primary_num = 0;
    } else {
        newheader.primary_num = current->primary_num;
        for (int i = 0; i < current->primary_num; i++) {
            if (current->primary_cols[i] < id) {
                newheader.primary_cols[i] = current->primary_cols[i];
            } else {
                newheader.primary_cols[i] = current->primary_cols[i] - 1;
            }
        }
    }
    //add index;
    for (int i = 0; i < current->index_num; i++) {
        bool flag = false;
        for (int j = 0; j < current->index_col_num[i]; j++) {
            if (current->index_cols[i][j] == id) {
                flag = true;
                break;
            }
        }
        if (flag) {
            continue;
        } else {
            newheader.index_col_num[newheader.index_num] = current->index_col_num[i];
            strcpy(newheader.index_name[newheader.index_num], current->index_name[i]);
            for (int j = 0; j < current->index_col_num[i]; j++) {
                if (current->index_cols[i][j] < id) {
                    newheader.index_cols[newheader.index_num][j] = current->index_cols[i][j];
                } else {
                    newheader.index_cols[newheader.index_num][j] = current->index_cols[i][j] - 1;
                }
            }
            newheader.index_num++;
        }
    }
    //add check
    for (int i = 0; i < current->check_constaint_num; i++) {
        std::istringstream is(std::string(current->check_constaints[i]));
        expr_node_t* check = expression::from_poland(is);
        std::vector<column_ref_t*> col_refs;
        expression::extract_col_refs(check, col_refs);
        bool flag = true;
        for (column_ref_t* col_ref : col_refs) {
            if (strcmp(col_ref->column, field_name) == 0) {
                flag = false;
                break;
            }
        }
        if (flag) {
            strcpy(newheader.check_constaints[newheader.check_constaint_num], current->check_constaints[i]);
            newheader.check_constaint_num++;
        }
        free_exprnode(check);
    }
    //add foreign
    for (int i = 0; i < current->foreign_num; i++) {
        bool flag = false;
        for (int j = 0; j < current->foreign_key_col_num[i]; j++) {
            if (current->foreign_key_column[i][j] == id) {
                flag = true;
                break;
            }
        }
        if (flag) {
            DatabaseManager::get_instance()->get_table_manager(current->foreign_key_ref_table[i])->DeRefer();
            continue;
        } else {
            int new_foreign = newheader.foreign_num;
            newheader.foreign_key_col_num[new_foreign] = current->foreign_key_col_num[i];
            strcpy(newheader.foreign_key_name[new_foreign], current->foreign_key_name[i]);
            strcpy(newheader.foreign_key_ref_table[new_foreign], current->foreign_key_ref_table[i]);
            for (int j = 0; j < current->foreign_key_col_num[i]; j++) {
                newheader.foreign_key_column[new_foreign][j] = current->foreign_key_column[i][j];
            }
            newheader.foreign_num++;
        }
    }
    TableHeader* header_ptr = new TableHeader;
    *header_ptr = newheader;
    return header_ptr;
}
IndexManager* TableManager::GetIndexManager(int col)
{
    if (current_tableHeader.primary_num > 0 && current_tableHeader.primary_cols[0] == col)
        return current_primaryManager;
    if (getBit(&current_tableHeader.bitmap_unique, col))
        return current_uniqueManagers[col];
    for (int i = 0; i < current_tableHeader.index_num; i++) {
        if (current_tableHeader.index_cols[i][0] == col)
            return current_indexManagers[i];
    }
    return nullptr;
}
int TableManager::GetColumnID(const char* col)
{
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        if (strcmp(current_tableHeader.col_name[i], col) == 0)
            return i;
    }
    return -1;
}
void TableManager::Insert(const linked_list_t* columns, const linked_list_t* values)
{
    expression::set_current_table(std::string(current_tableHeader.table_name));
    std::vector<int> cols;
    std::vector<expr_node_t*> vals;
    std::set<std::string> names;
    for (const linked_list_t* link_ptr = columns; link_ptr; link_ptr = link_ptr->next) {
        std::string name_str = std::string((char*)link_ptr->data);
        if (names.count(name_str) > 0) {
            fprintf(stderr, "[Error] Duplicated columns.\n");
            return;
        }
        names.insert(name_str);
        int col_id = GetColumnID((char*)link_ptr->data);
        if (col_id == -1) {
            fprintf(stderr, "[Error] Column not found.\n");
            return;
        }
        cols.push_back(col_id);
    }
    for (const linked_list_t* link_ptr = values; link_ptr; link_ptr = link_ptr->next) {
        vals.push_back((expr_node_t*)link_ptr->data);
    }
    std::reverse(cols.begin(), cols.end());
    std::reverse(vals.begin(), vals.end());
    if (cols.empty()) {
        for (int i = 0; i < vals.size(); i++) {
            cols.push_back(i);
        }
    }
    if (cols.size() != vals.size()) {
        fprintf(stderr, "[Error] Column num and value num mismatch.\n");
        return;
    }
    std::map<int, expr_node_t*> col_to_vals;
    for (int i = 0; i < cols.size(); i++)
        col_to_vals[cols[i]] = vals[i];
    Record record;
    record.iscopy = 1;
    record.num = current_tableHeader.col_num;
    record.type = new int[record.num];
    memcpy(record.type, current_tableHeader.col_type, record.num * sizeof(int));
    record.datas = new BufType[current_tableHeader.col_num];
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        record.datas[i] = nullptr;
    }
    std::vector<Expression> exprs;
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        if (col_to_vals.count(i) > 0) {
            Expression expr = expression::eval(col_to_vals[i]);
            if (!expression::type_compatible(current_tableHeader.col_type[i], expr)) {
                fprintf(stderr, "[Error] Type incompatible.\n");
                return;
            }
            record.datas[i] = expression::expr_to_buftype(expr);
            exprs.push_back(expr);
        } else if (getBit(&current_tableHeader.bitmap_default, i)) {
            record.datas[i] = expression::expr_to_buftype(defaults[i]);
            exprs.push_back(defaults[i]);
        } else {
            record.datas[i] = nullptr;
            Expression null_expr;
            null_expr.type = TERM_NULL;
            exprs.push_back(null_expr);
        }
        if (current_tableHeader.col_type[i] == TYPE_CHAR || current_tableHeader.col_type[i] == TYPE_VARCHAR) {
            if (record.datas[i] != nullptr && strlen((char*)record.datas[i]) > current_tableHeader.col_length[i]) {
                fprintf(stderr, "[Error] String too long.\n");
                return;
            }
        }
    }
    //check primary
    if (current_tableHeader.primary_num > 0) {
        if (!current_primaryManager->CheckNotNull(&record)) {
            fprintf(stderr, "[Error] Primary key cannot be null.\n");
            return;
        }
        if (current_primaryManager->Exists(&record, -1)) {
            fprintf(stderr, "[Error] Primary key duplicated.\n");
            return;
        }
    }
    //check not null
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        if (getBit(&current_tableHeader.bitmap_notnull, i)) {
            if (record.datas[i] == nullptr) {
                fprintf(stderr, "[Error] Not null constraint violation.\n");
                return;
            }
        }
    }
    //check unique
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        if (getBit(&current_tableHeader.bitmap_unique, i)) {
            if (current_uniqueManagers[i]->Exists(&record, -1)) {
                fprintf(stderr, "[Error] Unique constraint violation.\n");
                return;
            }
        }
    }
    //check foreign
    for (int i = 0; i < current_tableHeader.foreign_num; i++) {
        TableManager* table_ref = DatabaseManager::get_instance()->get_table_manager(current_tableHeader.foreign_key_ref_table[i]);
        Record* part = current_recordManager->GetParser()->toRecordPart(&record, current_tableHeader.foreign_key_column[i], current_tableHeader.foreign_key_col_num[i]);
        if (!table_ref->GetPrimaryManager()->ExistsPart(part, -1)) {
            fprintf(stderr, "[Error] Foreign constraint violation.\n");
            delete part;
            return;
        }
        delete part;
    }
    //check check
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        expression::cache_expr(std::string(current_tableHeader.table_name), std::string(current_tableHeader.col_name[i]), 0, exprs[i]);
    }
    expression::set_current_rid(std::string(current_tableHeader.table_name), 0);
    for (int i = 0; i < current_tableHeader.check_constaint_num; i++) {
        if (!expression::expr_to_bool(expression::eval(checks[i]))) {
            fprintf(stderr, "[Error] Check constraint violation.\n");
            expression::clear_cache(false);
            return;
        }
    }
    expression::clear_cache(false);
    int rid;
    current_recordManager->InsertRecord(rid, &record);
    record.rid = rid;
    current_tableHeader.records_num++;
    if (current_tableHeader.primary_num > 0) {
        current_primaryManager->Insert(&record);
    }
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        if (getBit(&current_tableHeader.bitmap_unique, i)) {
            current_uniqueManagers[i]->Insert(&record);
        }
    }
    for (int i = 0; i < current_tableHeader.index_num; i++) {
        current_indexManagers[i]->Insert(&record);
    }
}
void TableManager::Insert(const std::vector<expr_node_t*>& values)
{
    expression::set_current_table(std::string(current_tableHeader.table_name));
    std::vector<int> cols;
    std::vector<expr_node_t*> vals = values;
    std::set<std::string> names;
    for (int i = 0; i < vals.size(); i++) {
        cols.push_back(i);
    }
    std::map<int, expr_node_t*> col_to_vals;
    for (int i = 0; i < cols.size(); i++)
        col_to_vals[cols[i]] = vals[i];
    Record record;
    record.iscopy = 1;
    record.num = current_tableHeader.col_num;
    record.type = new int[record.num];
    memcpy(record.type, current_tableHeader.col_type, record.num * sizeof(int));
    record.datas = new BufType[current_tableHeader.col_num];
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        record.datas[i] = nullptr;
    }
    std::vector<Expression> exprs;
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        if (col_to_vals.count(i) > 0) {
            Expression expr = expression::eval(col_to_vals[i]);
            if (!expression::type_compatible(current_tableHeader.col_type[i], expr)) {
                fprintf(stderr, "[Error] Type incompatible.\n");
                return;
            }
            record.datas[i] = expression::expr_to_buftype(expr);
            exprs.push_back(expr);
        } else if (getBit(&current_tableHeader.bitmap_default, i)) {
            record.datas[i] = expression::expr_to_buftype(defaults[i]);
            exprs.push_back(defaults[i]);
        } else {
            record.datas[i] = nullptr;
            Expression null_expr;
            null_expr.type = TERM_NULL;
            exprs.push_back(null_expr);
        }
        if (current_tableHeader.col_type[i] == TYPE_CHAR || current_tableHeader.col_type[i] == TYPE_VARCHAR) {
            if (record.datas[i] != nullptr && strlen((char*)record.datas[i]) > current_tableHeader.col_length[i]) {
                fprintf(stderr, "[Error] String too long.\n");
                return;
            }
        }
    }
    //check primary
    if (current_tableHeader.primary_num > 0) {
        if (!current_primaryManager->CheckNotNull(&record)) {
            fprintf(stderr, "[Error] Primary key cannot be null.\n");
            return;
        }
        if (current_primaryManager->Exists(&record, -1)) {
            fprintf(stderr, "[Error] Primary key duplicated.\n");
            return;
        }
    }
    //check not null
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        if (getBit(&current_tableHeader.bitmap_notnull, i)) {
            if (record.datas[i] == nullptr) {
                fprintf(stderr, "[Error] Not null constraint violation.\n");
                return;
            }
        }
    }
    //check unique
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        if (getBit(&current_tableHeader.bitmap_unique, i)) {
            if (current_uniqueManagers[i]->Exists(&record, -1)) {
                fprintf(stderr, "[Error] Unique constraint violation.\n");
                return;
            }
        }
    }
    //check foreign
    for (int i = 0; i < current_tableHeader.foreign_num; i++) {
        TableManager* table_ref = DatabaseManager::get_instance()->get_table_manager(current_tableHeader.foreign_key_ref_table[i]);
        Record* part = current_recordManager->GetParser()->toRecordPart(&record, current_tableHeader.foreign_key_column[i], current_tableHeader.foreign_key_col_num[i]);
        if (!table_ref->GetPrimaryManager()->ExistsPart(part, -1)) {
            fprintf(stderr, "[Error] Foreign constraint violation.\n");
            delete part;
            return;
        }
        delete part;
    }
    //check check
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        expression::cache_expr(std::string(current_tableHeader.table_name), std::string(current_tableHeader.col_name[i]), 0, exprs[i]);
    }
    expression::set_current_rid(std::string(current_tableHeader.table_name), 0);
    for (int i = 0; i < current_tableHeader.check_constaint_num; i++) {
        if (!expression::expr_to_bool(expression::eval(checks[i]))) {
            fprintf(stderr, "[Error] Check constraint violation.\n");
            expression::clear_cache(false);
            return;
        }
    }
    expression::clear_cache(false);
    int rid;
    current_recordManager->InsertRecord(rid, &record);
    record.rid = rid;
    current_tableHeader.records_num++;
    if (current_tableHeader.primary_num > 0) {
        current_primaryManager->Insert(&record);
    }
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        if (getBit(&current_tableHeader.bitmap_unique, i)) {
            current_uniqueManagers[i]->Insert(&record);
        }
    }
    for (int i = 0; i < current_tableHeader.index_num; i++) {
        current_indexManagers[i]->Insert(&record);
    }
}
void TableManager::Update(const char* column, expr_node_t* where, const expr_node_t* value)
{
    int col_id = GetColumnID(column);
    if (col_id == -1) {
        fprintf(stderr, "[Error] Column not found.\n");
        return;
    }
    expression::set_current_table(std::string(current_tableHeader.table_name));
    std::set<int> rids;
    if (!DatabaseManager::get_instance()->select_from_one_table(current_tableHeader.table_name, where, rids)) {
        expression::clear_cache(true);
        return;
    }

    for (int rid : rids) {
        expression::set_current_rid(current_tableHeader.table_name, rid);
        Expression new_val = expression::eval(value);
        BufType val_buftype = expression::expr_to_buftype(new_val);
        if (!expression::type_compatible(current_tableHeader.col_type[col_id], new_val)) {
            fprintf(stderr, "[Error] Type incompatible.\n");
            expression::clear_cache(true);
            if (val_buftype != nullptr)
                delete[] val_buftype;
            return;
        }
        Record* old_record = current_recordManager->GetRecord(rid);
        Record new_record;
        new_record.iscopy = 0;
        new_record.rid = rid;
        new_record.num = old_record->num;
        new_record.type = old_record->type;
        new_record.datas = new BufType[new_record.num];
        for (int i = 0; i < new_record.num; i++) {
            if (i == col_id) {
                new_record.datas[i] = val_buftype;
            } else {
                new_record.datas[i] = old_record->datas[i];
            }
            if (current_tableHeader.col_type[i] == TYPE_CHAR || current_tableHeader.col_type[i] == TYPE_VARCHAR) {
                if (new_record.datas[i] != nullptr && strlen((char*)new_record.datas[i]) > current_tableHeader.col_length[i]) {
                    fprintf(stderr, "[Error] String too long.\n");
                    if (val_buftype != nullptr)
                        delete[] val_buftype;
                    return;
                }
            }
        }
        //check primary
        if (current_tableHeader.primary_num > 0) {
            if (!current_primaryManager->CheckNotNull(&new_record)) {
                fprintf(stderr, "[Error] Primary key cannot be null.\n");
                if (val_buftype != nullptr)
                    delete[] val_buftype;
                continue;
            }
            if (current_primaryManager->Exists(&new_record, rid)) {
                fprintf(stderr, "[Error] Primary key duplicated.\n");
                if (val_buftype != nullptr)
                    delete[] val_buftype;
                delete old_record;
                continue;
            }
        }
        //check not null
        bool flag = false;
        for (int i = 0; i < current_tableHeader.col_num; i++) {
            if (getBit(&current_tableHeader.bitmap_notnull, i)) {
                if (new_record.datas[i] == nullptr) {
                    fprintf(stderr, "[Error] Not null constraint violation.\n");
                    if (val_buftype != nullptr)
                        delete[] val_buftype;
                    delete old_record;
                    flag = true;
                    break;
                }
            }
        }
        if (flag)
            continue;
        //check unique
        for (int i = 0; i < current_tableHeader.col_num; i++) {
            if (getBit(&current_tableHeader.bitmap_unique, i)) {
                if (current_uniqueManagers[i]->Exists(&new_record, rid)) {
                    fprintf(stderr, "[Error] Unique constraint violation.\n");
                    if (val_buftype != nullptr)
                        delete[] val_buftype;
                    delete old_record;
                    flag = true;
                    break;
                }
            }
        }
        if (flag)
            continue;
        //check foreign
        for (int i = 0; i < current_tableHeader.foreign_num; i++) {
            TableManager* table_ref = DatabaseManager::get_instance()->get_table_manager(current_tableHeader.foreign_key_ref_table[i]);
            Record* part = current_recordManager->GetParser()->toRecordPart(&new_record, current_tableHeader.foreign_key_column[i], current_tableHeader.foreign_key_col_num[i]);
            if (!table_ref->GetPrimaryManager()->ExistsPart(part, -1)) {
                fprintf(stderr, "[Error] Foreign constraint violation.\n");
                delete part;
                if (val_buftype != nullptr)
                    delete[] val_buftype;
                delete old_record;
                flag = true;
                break;
            }
            delete part;
        }
        if (flag)
            continue;
        //check check
        expression::replace_column(current_tableHeader.table_name, column, rid, new_record.datas[col_id], new_record.type[col_id]);
        for (int i = 0; i < current_tableHeader.check_constaint_num; i++) {
            if (!expression::expr_to_bool(expression::eval(checks[i]))) {
                fprintf(stderr, "[Error] Check constraint violation.\n");
                delete old_record;
                flag = true;
                break;
            }
        }
        if (flag)
            continue;
        current_recordManager->UpdateRecord(rid, &new_record);
        if (current_tableHeader.primary_num > 0) {
            current_primaryManager->Delete(old_record);
            current_primaryManager->Insert(&new_record);
        }
        for (int i = 0; i < current_tableHeader.col_num; i++) {
            if (getBit(&current_tableHeader.bitmap_unique, i)) {
                current_uniqueManagers[i]->Delete(old_record);
                current_uniqueManagers[i]->Insert(&new_record);
            }
        }
        for (int i = 0; i < current_tableHeader.index_num; i++) {
            current_indexManagers[i]->Delete(old_record);
            current_indexManagers[i]->Insert(&new_record);
        }
        delete old_record;
    }
    expression::clear_cache(true);
}
void TableManager::Delete(expr_node_t* where)
{
    if (is_referred()) {
        fprintf(stderr, "[Error] Deletion is not allowed when table is referred.\n");
        return;
    }
    expression::set_current_table(std::string(current_tableHeader.table_name));
    std::set<int> rids;
    if (!DatabaseManager::get_instance()->select_from_one_table(current_tableHeader.table_name, where, rids)) {
        expression::clear_cache(true);
        return;
    }
    for (int rid : rids) {
        Record* old_record = current_recordManager->GetRecord(rid);
        current_recordManager->DeleteRecord(rid);
        if (current_tableHeader.primary_num > 0) {
            current_primaryManager->Delete(old_record);
        }
        for (int i = 0; i < current_tableHeader.col_num; i++) {
            if (getBit(&current_tableHeader.bitmap_unique, i)) {
                current_uniqueManagers[i]->Delete(old_record);
            }
        }
        for (int i = 0; i < current_tableHeader.index_num; i++) {
            current_indexManagers[i]->Delete(old_record);
        }
        delete old_record;
        current_tableHeader.records_num--;
    }
    expression::clear_cache(true);
}
void TableManager::Cache(std::set<int>& rids)
{
    if (rids.empty()) {
        //iterate over all record and fill in rids
        Record* record;
        if (current_recordManager->InitializeIteration()) {
            do {
                record = current_recordManager->CurrentRecord();
                Cache(record, record->rid);
                rids.insert(record->rid);
                delete record;
            } while (current_recordManager->NextRecord());
        }
    } else {
        //iterate over all record in rids
        Record* record;
        for (int rid : rids) {
            record = current_recordManager->GetRecord(rid);
            Cache(record, rid);
            delete record;
        }
    }
}
void TableManager::Cache(Record* record, int rid)
{
    for (int i = 0; i < current_tableHeader.col_num; i++) {
        expression::cache_column(std::string(current_tableHeader.table_name), std::string(current_tableHeader.col_name[i]), rid, record->datas[i], current_tableHeader.col_type[i]);
    }
}
int TableManager::GetColumnType(int col)
{
    return current_tableHeader.col_type[col];
}
void TableManager::Refer()
{
    current_tableHeader.referred_num++;
}
void TableManager::DeRefer()
{
    current_tableHeader.referred_num--;
}
bool TableManager::is_referred()
{
    return current_tableHeader.referred_num > 0;
}
TableHeader* TableManager::get_header()
{
    return &current_tableHeader;
}
void TableManager::AddAllForeignKey()
{
    for (int i = 0; i < current_tableHeader.foreign_num; i++) {
        TableManager* tablemanager = DatabaseManager::get_instance()->get_table_manager(current_tableHeader.foreign_key_ref_table[i]);
        tablemanager->Refer();
    }
}
void TableManager::DropAllForeignKey()
{
    for (int i = 0; i < current_tableHeader.foreign_num; i++) {
        TableManager* tablemanager = DatabaseManager::get_instance()->get_table_manager(current_tableHeader.foreign_key_ref_table[i]);
        tablemanager->DeRefer();
    }
}
IndexManager* TableManager::GetPrimaryManager()
{
    return current_primaryManager;
}
RecordManager* TableManager::GetRecordManager()
{
    return current_recordManager;
}