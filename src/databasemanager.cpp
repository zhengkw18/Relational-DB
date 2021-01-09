#include "databasemanager.h"
#include "expression.h"
#include <algorithm>
#include <map>
#include <unistd.h>
DatabaseManager::DatabaseManager()
{
    current_database_name = nullptr;
    FILE* databasesfile = fopen(".databases", "r");
    if (databasesfile == nullptr) {
        databasesfile = fopen(".databases", "wb+");
        current_databases.database_num = 0;
        fwrite(&current_databases, sizeof(Databases), 1, databasesfile);
        fclose(databasesfile);
        databasesfile = fopen(".databases", "r");
    }
    fread(&current_databases, sizeof(Databases), 1, databasesfile);
    fclose(databasesfile);
}
DatabaseManager::~DatabaseManager()
{
    CloseDB();
    FILE* databasesfile = fopen(".databases", "wb+");
    fwrite(&current_databases, sizeof(Databases), 1, databasesfile);
    fclose(databasesfile);
}
void DatabaseManager::CreateDB(const char* db_name)
{
    if (strlen(db_name) + 1 >= MAX_NAME_LEN) {
        fprintf(stderr, "[Error] Database name too long.\n");
        return;
    }
    int _;
    if (GetDBId(db_name, _)) {
        fprintf(stderr, "[Error] Database already exists.\n");
        return;
    }
    if (current_databases.database_num == MAX_DATABASE_NUM) {
        fprintf(stderr, "[Error] Database too many.\n");
        return;
    }
    if (current_database_name) {
        chdir("..");
    }
    strcpy(current_databases.databases[current_databases.database_num], db_name);
    current_databases.database_num++;
    system(("mkdir " + std::string(db_name)).c_str());
    chdir(db_name);
    FILE* tablesfile = fopen(".tables", "wb+");
    DatabaseHeader tables;
    tables.table_num = 0;
    fwrite(&tables, sizeof(DatabaseHeader), 1, tablesfile);
    fclose(tablesfile);
    chdir("..");
    if (current_database_name) {
        chdir(current_database_name);
    }
}
void DatabaseManager::DropDB(const char* db_name)
{
    int db_id;
    if (!GetDBId(db_name, db_id)) {
        fprintf(stderr, "[Error] Database not found.\n");
        return;
    }
    if (current_database_name) {
        if (strcmp(current_database_name, db_name) == 0) {
            CloseDB();
            system(("rm -rf " + std::string(db_name)).c_str());
        } else {
            chdir("..");
            system(("rm -rf " + std::string(db_name)).c_str());
            chdir(current_database_name);
        }
    } else {
        system(("rm -rf " + std::string(db_name)).c_str());
    }
    for (int i = db_id; i < current_databases.database_num - 1; i++) {
        strcpy(current_databases.databases[i], current_databases.databases[i + 1]);
    }
    current_databases.database_num--;
}
void DatabaseManager::UseDB(const char* db_name)
{
    int db_id;
    if (!GetDBId(db_name, db_id)) {
        fprintf(stderr, "[Error] Database not found.\n");
        return;
    }
    if (current_database_name && strcmp(current_database_name, db_name) == 0)
        return;
    CloseDB();
    chdir(db_name);
    FILE* tablesfile = fopen(".tables", "r");
    fread(&current_databaseheader, sizeof(DatabaseHeader), 1, tablesfile);
    fclose(tablesfile);
    current_database_name = new char[strlen(db_name) + 1];
    strcpy(current_database_name, db_name);
    for (int i = 0; i < current_databaseheader.table_num; i++) {
        current_tables[i] = new TableManager(current_databaseheader.tables[i]);
    }
}
void DatabaseManager::CloseDB()
{
    if (current_database_name) {
        for (int i = 0; i < current_databaseheader.table_num; i++) {
            delete current_tables[i];
        }
        delete[] current_database_name;
        current_database_name = nullptr;
        FILE* tablesfile = fopen(".tables", "wb+");
        fwrite(&current_databaseheader, sizeof(DatabaseHeader), 1, tablesfile);
        fclose(tablesfile);
        chdir("..");
    }
}

void DatabaseManager::ShowTables(DatabaseHeader& header)
{
    fprintf(stdout, "Tables:\n");
    for (int i = 0; i < header.table_num; i++) {
        fprintf(stdout, "%s\n", header.tables[i]);
    }
}
void DatabaseManager::ShowDB(const char* db_name)
{
    int db_id;
    if (!GetDBId(db_name, db_id)) {
        fprintf(stderr, "[Error] Database not found.\n");
        return;
    }
    DatabaseHeader tables;
    if (current_database_name) {
        if (strcmp(current_database_name, db_name) == 0) {
            ShowTables(current_databaseheader);
        } else {
            chdir("..");
            chdir(db_name);
            FILE* tablesfile = fopen(".tables", "r");
            DatabaseHeader tables;
            fread(&tables, sizeof(DatabaseHeader), 1, tablesfile);
            fclose(tablesfile);
            ShowTables(tables);
            chdir("..");
            chdir(current_database_name);
        }
    } else {
        chdir(db_name);
        FILE* tablesfile = fopen(".tables", "r");
        DatabaseHeader tables;
        fread(&tables, sizeof(DatabaseHeader), 1, tablesfile);
        fclose(tablesfile);
        ShowTables(tables);
        chdir("..");
    }
}
void DatabaseManager::ShowDBs()
{
    fprintf(stdout, "Databases:\n");
    for (int i = 0; i < current_databases.database_num; i++) {
        fprintf(stdout, "%s\n", current_databases.databases[i]);
    }
}

void DatabaseManager::DatabaseManager::CreateTable(const table_def_t* table)
{
    if (!AssertUsing())
        return;
    if (strlen(table->name) + 1 >= MAX_NAME_LEN) {
        fprintf(stderr, "[Error] Table name too long.\n");
        return;
    }
    int _;
    if (GetTableId(table->name, _)) {
        fprintf(stderr, "[Error] Table already exists.\n");
        return;
    }
    if (current_databaseheader.table_num == MAX_TABLE_NUM) {
        fprintf(stderr, "[Error] Table too many.\n");
        return;
    }
    TableHeader* tableheader = TableManager::InitHeader(table->name, table->fields, table->constraints);
    if (tableheader == nullptr)
        return;
    TableManager::CreateTable(tableheader);
    delete tableheader;
    strcpy(current_databaseheader.tables[current_databaseheader.table_num], table->name);
    current_tables[current_databaseheader.table_num] = new TableManager(table->name);
    current_tables[current_databaseheader.table_num]->AddAllForeignKey();
    current_databaseheader.table_num++;
}
void DatabaseManager::ShowTable(const char* table_name)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    current_tables[table_id]->Show();
}
void DatabaseManager::DropTable(const char* table_name)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    if (current_tables[table_id]->is_referred()) {
        fprintf(stderr, "[Error] Table is referred.\n");
        return;
    }
    current_tables[table_id]->DropAllForeignKey();
    delete current_tables[table_id];
    TableManager::DropTable(table_name);
    for (int i = table_id; i < current_databaseheader.table_num - 1; i++) {
        current_tables[i] = current_tables[i + 1];
        memcpy(current_databaseheader.tables[i], current_databaseheader.tables[i + 1], MAX_NAME_LEN);
    }
    current_databaseheader.table_num--;
}
void DatabaseManager::CopyFrom(const char* table_name, const char* path)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    TableManager* table_manager = current_tables[table_id];
    FILE* file = fopen(path, "r");
    if (!file) {
        fprintf(stderr, "[Error] File not found.\n");
        return;
    }
    char* buffer = new char[1000001];
    while (fgets(buffer, 1e6, file)) {
        std::vector<std::string> items;
        int l = strlen(buffer);
        int last = 0;
        for (int i = 0; i < l; i++) {
            if (buffer[i] == '|') {
                items.push_back(std::string(buffer + last, i - last));
                last = i + 1;
            }
        }
        if (items.size() > 0) {
            linked_list_t* val_list = (linked_list_t*)malloc(sizeof(linked_list_t));
            val_list->next = NULL;
            expr_node_t* expr = (expr_node_t*)calloc(1, sizeof(expr_node_t));
            switch (table_manager->get_header()->col_type[0]) {
            case TYPE_INT:
                expr->term_type = TERM_INT;
                expr->val_i = atoi(items[0].c_str());
                break;
            case TYPE_FLOAT:
                expr->term_type = TERM_FLOAT;
                expr->val_f = atof(items[0].c_str());
                break;
            case TYPE_DATE:
                expr->term_type = TERM_DATE;
                expr->val_s = strdup(items[0].c_str());
                break;
            case TYPE_CHAR:
                expr->term_type = TERM_STRING;
                expr->val_s = strdup(items[0].c_str());
                break;
            case TYPE_VARCHAR:
                expr->term_type = TERM_STRING;
                expr->val_s = strdup(items[0].c_str());
                break;
            default:
                break;
            }
            val_list->data = expr;
            for (int i = 1; i < items.size(); i++) {
                linked_list_t* val_list_new = (linked_list_t*)malloc(sizeof(linked_list_t));
                val_list_new->next = val_list;
                expr_node_t* expr = (expr_node_t*)calloc(1, sizeof(expr_node_t));
                switch (table_manager->get_header()->col_type[i]) {
                case TYPE_INT:
                    expr->term_type = TERM_INT;
                    expr->val_i = atoi(items[i].c_str());
                    break;
                case TYPE_FLOAT:
                    expr->term_type = TERM_FLOAT;
                    expr->val_f = atof(items[i].c_str());
                    break;
                case TYPE_DATE:
                    expr->term_type = TERM_DATE;
                    expr->val_s = strdup(items[i].c_str());
                    break;
                case TYPE_CHAR:
                    expr->term_type = TERM_STRING;
                    expr->val_s = strdup(items[i].c_str());
                    break;
                case TYPE_VARCHAR:
                    expr->term_type = TERM_STRING;
                    expr->val_s = strdup(items[i].c_str());
                    break;
                default:
                    break;
                }
                val_list_new->data = expr;
                val_list = val_list_new;
            }
            table_manager->Insert(nullptr, val_list);
            free_linked_list<expr_node_t>(val_list, free_exprnode);
        }
    }
    delete[] buffer;
}
void DatabaseManager::Insert(const insert_info_t* insert_info)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(insert_info->table, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    current_tables[table_id]->Insert(insert_info->columns, insert_info->values);
}
void DatabaseManager::Update(const update_info_t* update_info)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(update_info->table, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    current_tables[table_id]->Update(update_info->column, update_info->where, update_info->value);
}
void DatabaseManager::Select(const select_info_t* select_info)
{
    if (!AssertUsing())
        return;
    std::vector<std::string> tables;
    std::vector<int> table_ids;
    for (const linked_list_t* link_ptr = select_info->tables; link_ptr; link_ptr = link_ptr->next) {
        tables.push_back(std::string((char*)link_ptr->data));
        int table_id;
        if (!GetTableId((char*)link_ptr->data, table_id)) {
            fprintf(stderr, "[Error] Table not found.\n");
            return;
        }
        table_ids.push_back(table_id);
    }
    int table_num = tables.size();
    if (table_num == 1) {
        expression::set_current_table(tables[0]);
        std::set<int> rids;
        if (!select_from_one_table(tables[0].c_str(), select_info->where, rids)) {
            return;
        }
        bool select_all = (select_info->exprs == nullptr);
        std::vector<expr_node_t*> exprs;
        if (select_all) {
            TableHeader* header = current_tables[table_ids[0]]->get_header();
            for (int i = 0; i < header->col_num; i++) {
                column_ref_t* col_ref = (column_ref_t*)malloc(sizeof(column_ref_t));
                col_ref->table = NULL;
                col_ref->column = strdup(header->col_name[i]);
                expr_node_t* expr = (expr_node_t*)calloc(1, sizeof(expr_node_t));
                expr->column_ref = col_ref;
                expr->term_type = TERM_COLUMN_REF;
                exprs.push_back(expr);
            }
        } else {
            for (const linked_list_t* link_ptr = select_info->exprs; link_ptr; link_ptr = link_ptr->next) {
                std::vector<column_ref_t*> expr_col_refs;
                expression::extract_col_refs((expr_node_t*)link_ptr->data, expr_col_refs);
                for (column_ref_t* col_ref : expr_col_refs) {
                    if (col_ref->table != nullptr && strcmp(col_ref->table, tables[0].c_str()) != 0) {
                        fprintf(stderr, "[Error] Table not found.\n");
                        expression::clear_cache(true);
                        return;
                    }
                    int id = current_tables[table_ids[0]]->GetColumnID(col_ref->column);
                    if (id == -1) {
                        fprintf(stderr, "[Error] Column not found.\n");
                        expression::clear_cache(true);
                        return;
                    }
                }
                exprs.push_back((expr_node_t*)link_ptr->data);
            }
            std::reverse(exprs.begin(), exprs.end());
        }
        bool aggregate = false;
        for (expr_node_t* expr : exprs) {
            if (expression::is_aggregate(expr)) {
                aggregate = true;
                break;
            }
        }
        if (aggregate) {
            if (exprs.size() > 1) {
                fprintf(stderr, "[Error] More than one aggregation is not supported.\n");
                expression::clear_cache(true);
                return;
            }
            expr_node_t* expr = exprs[0];
            column_ref_t* col_ref;
            int col_type;
            if (expr->op != OPERATOR_COUNT) {
                col_ref = expr->left->column_ref;
                if (col_ref == nullptr) {
                    fprintf(stderr, "[Error] Must specify column.\n");
                    expression::clear_cache(true);
                    return;
                }
                col_type = current_tables[table_ids[0]]->GetColumnType(current_tables[table_ids[0]]->GetColumnID(col_ref->column));
                if (col_type != TYPE_INT && col_type != TYPE_FLOAT) {
                    fprintf(stderr, "[Error] Type not supported.\n");
                    expression::clear_cache(true);
                    return;
                }
            }
            std::printf("%s ", expression::to_string(expr).c_str());
            std::puts("");
            int val_i;
            float val_f;
            switch (expr->op) {
            case OPERATOR_COUNT:
                std::printf("%d", rids.size());
                break;
            case OPERATOR_MIN:
                if (col_type == TYPE_INT) {
                    val_i = std::numeric_limits<int>::max();
                    for (int rid : rids) {
                        expression::set_current_rid(tables[0], rid);
                        Expression ret = expression::eval(expr->left);
                        if (ret.val_i < val_i)
                            val_i = ret.val_i;
                    }
                    std::printf("%d", val_i);
                } else if (col_type == TYPE_FLOAT) {
                    val_f = std::numeric_limits<float>::max();
                    for (int rid : rids) {
                        expression::set_current_rid(tables[0], rid);
                        Expression ret = expression::eval(expr->left);
                        if (ret.val_f < val_f)
                            val_f = ret.val_f;
                    }
                    std::printf("%f", val_f);
                }
                break;
            case OPERATOR_MAX:
                if (col_type == TYPE_INT) {
                    val_i = std::numeric_limits<int>::min();
                    for (int rid : rids) {
                        expression::set_current_rid(tables[0], rid);
                        Expression ret = expression::eval(expr->left);
                        if (ret.val_i > val_i)
                            val_i = ret.val_i;
                    }
                    std::printf("%d", val_i);
                } else if (col_type == TYPE_FLOAT) {
                    val_f = std::numeric_limits<float>::min();
                    for (int rid : rids) {
                        expression::set_current_rid(tables[0], rid);
                        Expression ret = expression::eval(expr->left);
                        if (ret.val_f > val_f)
                            val_f = ret.val_f;
                    }
                    std::printf("%f", val_f);
                }
                break;
            case OPERATOR_AVG:
                if (col_type == TYPE_INT) {
                    val_i = 0;
                    for (int rid : rids) {
                        expression::set_current_rid(tables[0], rid);
                        Expression ret = expression::eval(expr->left);
                        val_i += ret.val_i;
                    }
                    val_f = ((double)val_i) / rids.size();
                    std::printf("%f", val_f);
                } else if (col_type == TYPE_FLOAT) {
                    val_f = 0;
                    for (int rid : rids) {
                        expression::set_current_rid(tables[0], rid);
                        Expression ret = expression::eval(expr->left);
                        val_f += ret.val_f;
                    }
                    val_f = val_f / rids.size();
                    std::printf("%f", val_f);
                }
                break;
            case OPERATOR_SUM:
                if (col_type == TYPE_INT) {
                    val_i = 0;
                    for (int rid : rids) {
                        expression::set_current_rid(tables[0], rid);
                        Expression ret = expression::eval(expr->left);
                        val_i += ret.val_i;
                    }
                    std::printf("%d", val_i);
                } else if (col_type == TYPE_FLOAT) {
                    val_f = 0;
                    for (int rid : rids) {
                        expression::set_current_rid(tables[0], rid);
                        Expression ret = expression::eval(expr->left);
                        val_f += ret.val_f;
                    }
                    std::printf("%f", val_f);
                }
                break;
            default:
                break;
            }
            std::puts("");
        } else {
            for (expr_node_t* expr : exprs) {
                std::printf("%s ", expression::to_string(expr).c_str());
            }
            std::puts("");
            for (int rid : rids) {
                expression::set_current_rid(tables[0], rid);
                for (expr_node_t* expr : exprs) {
                    expression::print_Expr(expression::eval(expr));
                    std::printf(" ");
                }
                std::puts("");
            }
            if (select_all) {
                for (int i = 0; i < exprs.size(); i++) {
                    free_exprnode(exprs[i]);
                }
            }
        }

    } else {
        std::vector<expr_node_t*> and_exprs;
        expression::extract_and_cond(select_info->where, and_exprs);
        //extract one/two table constraint
        std::vector<std::vector<expr_node_t*>> one_table_expr(table_num, std::vector<expr_node_t*>());
        std::vector<std::vector<expr_node_t*>> two_table_expr(table_num * (table_num - 1) / 2, std::vector<expr_node_t*>());
        std::vector<expr_node_t*> expr_remain;
        for (expr_node_t* and_expr : and_exprs) {
            std::set<int> table_in_ref;
            std::vector<column_ref_t*> col_refs;
            expression::extract_col_refs(and_expr, col_refs);
            for (column_ref_t* col_ref : col_refs) {
                int id = -1;
                for (int i = 0; i < table_num; i++) {
                    if (col_ref->table != nullptr && strcmp(col_ref->table, tables[i].c_str()) == 0) {
                        id = i;
                        break;
                    }
                }
                if (id == -1) {
                    fprintf(stderr, "[Error] Table not found.\n");
                    return;
                }
                TableManager* table_manager = current_tables[table_ids[id]];
                if (table_manager->GetColumnID(col_ref->column) == -1) {
                    fprintf(stderr, "[Error] Column not found.\n");
                    return;
                }
                table_in_ref.insert(id);
            }
            if (table_in_ref.size() == 1) {
                one_table_expr[*table_in_ref.begin()].push_back(and_expr);
            } else if (table_in_ref.size() == 2) {
                int cnt = 0;
                int ids[2];
                for (int id : table_in_ref) {
                    ids[cnt++] = id;
                }
                int a, b;
                if (ids[0] < ids[1]) {
                    a = ids[0];
                    b = ids[1];
                } else {
                    a = ids[1];
                    b = ids[0];
                }
                two_table_expr[table_num * a - a * (a + 1) / 2 + b - a - 1].push_back(and_expr);
            } else {
                expr_remain.push_back(and_expr);
            }
        }
        //one table filter
        std::vector<std::set<int>> one_table_rids(table_num, std::set<int>());
        for (int i = 0; i < table_num; i++) {
            if (one_table_expr[i].size() == 0) {
                select_from_one_table(tables[i].c_str(), nullptr, one_table_rids[i]);
            } else {
                if (!select_from_one_table(tables[i].c_str(), one_table_expr[i], one_table_rids[i])) {
                    expression::clear_cache(true);
                    return;
                }
            }
        }
        //two table filter
        //(a, b) -> n*a-a(a+1)/2+b-a-1, a<b
        std::vector<std::unordered_multimap<int, int>> two_table_rids(table_num * (table_num - 1) / 2, std::unordered_multimap<int, int>());
        for (int a = 0; a < table_num - 1; a++) {
            for (int b = a + 1; b < table_num; b++) {
                int index = table_num * a - a * (a + 1) / 2 + b - a - 1;
                if (two_table_expr[index].size() > 0) {
                    set<int> newset_a, newset_b;
                    std::vector<expr_node_t*> two_table_binary;
                    std::vector<expr_node_t*> two_table_remain;
                    for (expr_node_t* expr : two_table_expr[index]) {
                        if (expr->left->term_type == TERM_COLUMN_REF && expr->right->term_type == TERM_COLUMN_REF && (expr->op == OPERATOR_EQ)) {
                            two_table_binary.push_back(expr);
                        } else {
                            two_table_remain.push_back(expr);
                        }
                    }
                    if (one_table_rids[a].size() > one_table_rids[b].size()) {
                        std::unordered_map<expr_node_t*, std::vector<Expression>> expr_to_expressions_a;
                        for (expr_node_t* expr : two_table_binary) {
                            std::vector<Expression> expressions_a;
                            expr_node_t *expr_a, *expr_b;
                            if (strcmp(expr->left->column_ref->table, tables[b].c_str()) == 0) {
                                expr_a = expr->right;
                                expr_b = expr->left;
                            } else {
                                expr_a = expr->left;
                                expr_b = expr->right;
                            }
                            IndexManager* im = current_tables[table_ids[a]]->GetIndexManager(current_tables[table_ids[a]]->GetColumnID(expr_a->column_ref->column));
                            if (im != nullptr) {
                                continue;
                            }
                            for (int rid_a : one_table_rids[a]) {
                                expressions_a.push_back(cache[std::make_tuple(expr_a->column_ref->table, expr_a->column_ref->column, rid_a)]);
                            }
                            std::sort(expressions_a.begin(), expressions_a.end());
                            expr_to_expressions_a.insert(std::make_pair(expr, expressions_a));
                        }
                        for (int rid_b : one_table_rids[b]) {
                            std::set<int> rid_as = one_table_rids[a];
                            for (expr_node_t* expr : two_table_binary) {
                                std::set<int> rid_as_temp;

                                expr_node_t *expr_a, *expr_b;
                                if (strcmp(expr->left->column_ref->table, tables[b].c_str()) == 0) {
                                    expr_a = expr->right;
                                    expr_b = expr->left;
                                } else {
                                    expr_a = expr->left;
                                    expr_b = expr->right;
                                }
                                IndexManager* im = current_tables[table_ids[a]]->GetIndexManager(current_tables[table_ids[a]]->GetColumnID(expr_a->column_ref->column));
                                if (im != nullptr) {
                                    Expression expression_b = cache[std::make_tuple(expr_b->column_ref->table, expr_b->column_ref->column, rid_b)];
                                    BufType val = expression::expr_to_buftype(expression_b);
                                    im->get_rid_eq(val, rid_as_temp);
                                    std::set<int> result;
                                    std::set_intersection(rid_as.begin(), rid_as.end(), rid_as_temp.begin(), rid_as_temp.end(), std::inserter(result, result.begin()));
                                    rid_as = result;
                                    delete[] val;
                                } else {
                                    std::vector<Expression> expressions_a = expr_to_expressions_a[expr];
                                    Expression expression_b = cache[std::make_tuple(expr_b->column_ref->table, expr_b->column_ref->column, rid_b)];
                                    int pos = std::lower_bound(expressions_a.begin(), expressions_a.end(), expression_b) - expressions_a.begin();
                                    for (int i = pos; i < expressions_a.size(); i++) {
                                        if (expressions_a[i] == expression_b) {
                                            rid_as_temp.insert(expressions_a[i].rid);
                                        } else {
                                            break;
                                        }
                                    }
                                    std::set<int> result;
                                    std::set_intersection(rid_as.begin(), rid_as.end(), rid_as_temp.begin(), rid_as_temp.end(), std::inserter(result, result.begin()));
                                    rid_as = result;
                                }
                            }
                            for (int rid_a : rid_as) {
                                expression::set_current_rid(tables[a], rid_a);
                                expression::set_current_rid(tables[b], rid_b);
                                bool valid = true;
                                for (expr_node_t* expr : two_table_remain) {
                                    if (!expression::expr_to_bool(expression::eval(expr))) {
                                        valid = false;
                                        break;
                                    }
                                }
                                if (valid) {
                                    two_table_rids[index].insert(std::make_pair(rid_a, rid_b));
                                    newset_a.insert(rid_a);
                                    newset_b.insert(rid_b);
                                }
                            }
                        }
                    } else {
                        std::unordered_map<expr_node_t*, std::vector<Expression>> expr_to_expressions_b;
                        for (expr_node_t* expr : two_table_binary) {
                            std::vector<Expression> expressions_b;
                            expr_node_t *expr_a, *expr_b;
                            if (strcmp(expr->left->column_ref->table, tables[b].c_str()) == 0) {
                                expr_a = expr->right;
                                expr_b = expr->left;
                            } else {
                                expr_a = expr->left;
                                expr_b = expr->right;
                            }
                            IndexManager* im = current_tables[table_ids[b]]->GetIndexManager(current_tables[table_ids[b]]->GetColumnID(expr_b->column_ref->column));
                            if (im != nullptr) {
                                continue;
                            }
                            for (int rid_b : one_table_rids[b]) {
                                expressions_b.push_back(cache[std::make_tuple(expr_b->column_ref->table, expr_b->column_ref->column, rid_b)]);
                            }
                            std::sort(expressions_b.begin(), expressions_b.end());
                            expr_to_expressions_b.insert(std::make_pair(expr, expressions_b));
                        }
                        for (int rid_a : one_table_rids[a]) {
                            std::set<int> rid_bs = one_table_rids[b];
                            for (expr_node_t* expr : two_table_binary) {
                                std::set<int> rid_bs_temp;

                                expr_node_t *expr_a, *expr_b;
                                if (strcmp(expr->left->column_ref->table, tables[b].c_str()) == 0) {
                                    expr_a = expr->right;
                                    expr_b = expr->left;
                                } else {
                                    expr_a = expr->left;
                                    expr_b = expr->right;
                                }
                                IndexManager* im = current_tables[table_ids[b]]->GetIndexManager(current_tables[table_ids[b]]->GetColumnID(expr_b->column_ref->column));
                                if (im != nullptr) {
                                    Expression expression_a = cache[std::make_tuple(expr_a->column_ref->table, expr_a->column_ref->column, rid_a)];
                                    BufType val = expression::expr_to_buftype(expression_a);
                                    im->get_rid_eq(val, rid_bs_temp);
                                    std::set<int> result;
                                    std::set_intersection(rid_bs.begin(), rid_bs.end(), rid_bs_temp.begin(), rid_bs_temp.end(), std::inserter(result, result.begin()));
                                    rid_bs = result;
                                    delete[] val;
                                } else {
                                    std::vector<Expression> expressions_b = expr_to_expressions_b[expr];
                                    Expression expression_a = cache[std::make_tuple(expr_a->column_ref->table, expr_a->column_ref->column, rid_a)];
                                    int pos = std::lower_bound(expressions_b.begin(), expressions_b.end(), expression_a) - expressions_b.begin();
                                    for (int i = pos; i < expressions_b.size(); i++) {
                                        if (expressions_b[i] == expression_a) {
                                            rid_bs_temp.insert(expressions_b[i].rid);
                                        } else {
                                            break;
                                        }
                                    }
                                    std::set<int> result;
                                    std::set_intersection(rid_bs.begin(), rid_bs.end(), rid_bs_temp.begin(), rid_bs_temp.end(), std::inserter(result, result.begin()));
                                    rid_bs = result;
                                }
                            }
                            for (int rid_b : rid_bs) {
                                expression::set_current_rid(tables[a], rid_a);
                                expression::set_current_rid(tables[b], rid_b);
                                bool valid = true;
                                for (expr_node_t* expr : two_table_remain) {
                                    if (!expression::expr_to_bool(expression::eval(expr))) {
                                        valid = false;
                                        break;
                                    }
                                }
                                if (valid) {
                                    two_table_rids[index].insert(std::make_pair(rid_a, rid_b));
                                    newset_a.insert(rid_a);
                                    newset_b.insert(rid_b);
                                }
                            }
                        }
                    }
                    one_table_rids[a] = newset_a;
                    one_table_rids[b] = newset_b;
                }
            }
        }
        std::set<RID> rids;
        //merge
        for (int rid0 : one_table_rids[0]) {
            RID rid;
            rid.rids[0] = rid0;
            rids.insert(rid);
        }
        for (int b = 1; b < table_num; b++) {
            std::set<RID> rids_temp;
            for (RID rid : rids) {
                std::set<int> ridsi;
                bool flag = true;
                for (int a = 0; a < b; a++) {
                    int index = table_num * a - a * (a + 1) / 2 + b - a - 1;
                    if (two_table_expr[index].size() == 0)
                        continue;
                    std::set<int> rids_map;
                    auto pr = two_table_rids[index].equal_range(rid.rids[a]);
                    if (pr.first != std::end(two_table_rids[index])) {
                        for (auto iter = pr.first; iter != pr.second; ++iter) {
                            rids_map.insert(iter->second);
                        }
                    }
                    if (flag) {
                        flag = false;
                        ridsi.insert(rids_map.begin(), rids_map.end());
                    } else {
                        std::set<int> result;
                        std::set_intersection(ridsi.begin(), ridsi.end(), rids_map.begin(), rids_map.end(), std::inserter(result, result.begin()));
                        ridsi = result;
                    }
                }
                if (flag) {
                    ridsi = one_table_rids[b];
                }
                for (int ridi : ridsi) {
                    RID rid_new = rid;
                    rid_new.rids[b] = ridi;
                    rids_temp.insert(rid_new);
                }
            }
            rids = rids_temp;
        }
        //apply remaining filters
        std::set<RID> rids_valid;
        for (RID rid : rids) {
            for (int i = 0; i < table_num; i++) {
                expression::set_current_rid(tables[i], rid.rids[i]);
            }
            bool valid = true;
            for (expr_node_t* expr : expr_remain) {
                if (!expression::expr_to_bool(expression::eval(expr))) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                rids_valid.insert(rid);
            }
        }
        //select
        bool select_all = (select_info->exprs == nullptr);
        std::vector<expr_node_t*> exprs;
        if (select_all) {
            for (int i = 0; i < table_num; i++) {
                TableHeader* header = current_tables[table_ids[i]]->get_header();
                for (int j = 0; j < header->col_num; j++) {
                    column_ref_t* col_ref = (column_ref_t*)malloc(sizeof(column_ref_t));
                    col_ref->table = strdup(header->table_name);
                    col_ref->column = strdup(header->col_name[j]);
                    expr_node_t* expr = (expr_node_t*)calloc(1, sizeof(expr_node_t));
                    expr->column_ref = col_ref;
                    expr->term_type = TERM_COLUMN_REF;
                    exprs.push_back(expr);
                }
            }
        } else {
            for (const linked_list_t* link_ptr = select_info->exprs; link_ptr; link_ptr = link_ptr->next) {
                std::vector<column_ref_t*> expr_col_refs;
                expression::extract_col_refs((expr_node_t*)link_ptr->data, expr_col_refs);
                for (column_ref_t* col_ref : expr_col_refs) {
                    int id = -1;
                    for (int i = 0; i < table_num; i++) {
                        if (col_ref->table != nullptr && strcmp(col_ref->table, tables[i].c_str()) == 0) {
                            id = i;
                            break;
                        }
                    }
                    if (id == -1) {
                        fprintf(stderr, "[Error] Table not found.\n");
                        expression::clear_cache(true);
                        return;
                    }
                    int col_id = current_tables[table_ids[id]]->GetColumnID(col_ref->column);
                    if (col_id == -1) {
                        fprintf(stderr, "[Error] Column not found.\n");
                        expression::clear_cache(true);
                        return;
                    }
                }
                exprs.push_back((expr_node_t*)link_ptr->data);
            }
            std::reverse(exprs.begin(), exprs.end());
        }
        bool aggregate = false;
        for (expr_node_t* expr : exprs) {
            if (expression::is_aggregate(expr)) {
                aggregate = true;
                break;
            }
        }
        if (aggregate) {
            if (exprs.size() > 1) {
                fprintf(stderr, "[Error] More than one aggregation is not supported.\n");
                expression::clear_cache(true);
                return;
            }
            expr_node_t* expr = exprs[0];
            column_ref_t* col_ref;
            int col_type;
            if (expr->op != OPERATOR_COUNT) {
                col_ref = expr->left->column_ref;
                if (col_ref == nullptr) {
                    fprintf(stderr, "[Error] Must specify column.\n");
                    expression::clear_cache(true);
                    return;
                }
                int id = -1;
                for (int i = 0; i < table_num; i++) {
                    if (strcmp(col_ref->table, tables[i].c_str()) == 0) {
                        id = i;
                        break;
                    }
                }
                col_type = current_tables[table_ids[id]]->GetColumnType(current_tables[table_ids[id]]->GetColumnID(col_ref->column));
                if (col_type != TYPE_INT && col_type != TYPE_FLOAT) {
                    fprintf(stderr, "[Error] Type not supported.\n");
                    expression::clear_cache(true);
                    return;
                }
            }
            std::printf("%s ", expression::to_string(expr).c_str());
            std::puts("");
            int val_i;
            float val_f;
            switch (expr->op) {
            case OPERATOR_COUNT:
                std::printf("%d", rids_valid.size());
                break;
            case OPERATOR_MIN:
                if (col_type == TYPE_INT) {
                    val_i = std::numeric_limits<int>::max();
                    for (RID rid : rids_valid) {
                        for (int i = 0; i < table_num; i++) {
                            expression::set_current_rid(tables[i], rid.rids[i]);
                        }
                        Expression ret = expression::eval(expr->left);
                        if (ret.val_i < val_i)
                            val_i = ret.val_i;
                    }
                    std::printf("%d", val_i);
                } else if (col_type == TYPE_FLOAT) {
                    val_f = std::numeric_limits<float>::max();
                    for (RID rid : rids_valid) {
                        for (int i = 0; i < table_num; i++) {
                            expression::set_current_rid(tables[i], rid.rids[i]);
                        }
                        Expression ret = expression::eval(expr->left);
                        if (ret.val_f < val_f)
                            val_f = ret.val_f;
                    }
                    std::printf("%f", val_f);
                }
                break;
            case OPERATOR_MAX:
                if (col_type == TYPE_INT) {
                    val_i = std::numeric_limits<int>::min();
                    for (RID rid : rids_valid) {
                        for (int i = 0; i < table_num; i++) {
                            expression::set_current_rid(tables[i], rid.rids[i]);
                        }
                        Expression ret = expression::eval(expr->left);
                        if (ret.val_i > val_i)
                            val_i = ret.val_i;
                    }
                    std::printf("%d", val_i);
                } else if (col_type == TYPE_FLOAT) {
                    val_f = std::numeric_limits<float>::min();
                    for (RID rid : rids_valid) {
                        for (int i = 0; i < table_num; i++) {
                            expression::set_current_rid(tables[i], rid.rids[i]);
                        }
                        Expression ret = expression::eval(expr->left);
                        if (ret.val_f > val_f)
                            val_f = ret.val_f;
                    }
                    std::printf("%f", val_f);
                }
                break;
            case OPERATOR_AVG:
                if (col_type == TYPE_INT) {
                    val_i = 0;
                    for (RID rid : rids_valid) {
                        for (int i = 0; i < table_num; i++) {
                            expression::set_current_rid(tables[i], rid.rids[i]);
                        }
                        Expression ret = expression::eval(expr->left);
                        val_i += ret.val_i;
                    }
                    val_f = ((double)val_i) / rids.size();
                    std::printf("%f", val_f);
                } else if (col_type == TYPE_FLOAT) {
                    val_f = 0;
                    for (RID rid : rids_valid) {
                        for (int i = 0; i < table_num; i++) {
                            expression::set_current_rid(tables[i], rid.rids[i]);
                        }
                        Expression ret = expression::eval(expr->left);
                        val_f += ret.val_f;
                    }
                    val_f = val_f / rids.size();
                    std::printf("%f", val_f);
                }
                break;
            case OPERATOR_SUM:
                if (col_type == TYPE_INT) {
                    val_i = 0;
                    for (RID rid : rids_valid) {
                        for (int i = 0; i < table_num; i++) {
                            expression::set_current_rid(tables[i], rid.rids[i]);
                        }
                        Expression ret = expression::eval(expr->left);
                        val_i += ret.val_i;
                    }
                    std::printf("%d", val_i);
                } else if (col_type == TYPE_FLOAT) {
                    val_f = 0;
                    for (RID rid : rids_valid) {
                        for (int i = 0; i < table_num; i++) {
                            expression::set_current_rid(tables[i], rid.rids[i]);
                        }
                        Expression ret = expression::eval(expr->left);
                        val_f += ret.val_f;
                    }
                    std::printf("%f", val_f);
                }
                break;
            default:
                break;
            }
            std::puts("");
        } else {
            for (expr_node_t* expr : exprs) {
                std::printf("%s ", expression::to_string(expr).c_str());
            }
            std::puts("");
            for (RID rid : rids_valid) {
                for (int i = 0; i < table_num; i++) {
                    expression::set_current_rid(tables[i], rid.rids[i]);
                }
                for (expr_node_t* expr : exprs) {
                    expression::print_Expr(expression::eval(expr));
                    std::printf(" ");
                }
                std::puts("");
            }
            if (select_all) {
                for (int i = 0; i < exprs.size(); i++) {
                    free_exprnode(exprs[i]);
                }
            }
        }
    }
    expression::clear_cache(true);
}
void DatabaseManager::Delete(const delete_info_t* delete_info)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(delete_info->table, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    current_tables[table_id]->Delete(delete_info->where);
}
void DatabaseManager::AlterTable_Rename(const char* table_name, const char* new_name)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    if (current_tables[table_id]->is_referred()) {
        fprintf(stderr, "[Error] The table is referred.\n");
        return;
    }
    int _;
    if (GetTableId(new_name, _)) {
        fprintf(stderr, "[Error] Table name duplicated.\n");
        return;
    }
    if (strlen(new_name) + 1 >= MAX_NAME_LEN) {
        fprintf(stderr, "[Error] Table name too long.\n");
        return;
    }
    delete current_tables[table_id];
    strcpy(current_databaseheader.tables[table_id], new_name);
    std::string table_name_str = table_name;
    system(("ls " + table_name_str + "* | sed 's/^" + table_name_str + "//' | xargs -i mv " + table_name_str + "{} " + std::string(new_name) + "{}").c_str());
    current_tables[table_id] = new TableManager(new_name);
}
void DatabaseManager::AlterTable_AddPrimary(const char* table_name, const linked_list_t* column_list)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    current_tables[table_id]->AddPrimary(column_list);
}
void DatabaseManager::AlterTable_DropPrimary(const char* table_name)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    current_tables[table_id]->DropPrimary();
}
void DatabaseManager::AlterTable_AddUnique(const char* table_name, const char* field_name)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    current_tables[table_id]->AddUnique(field_name);
}
void DatabaseManager::AlterTable_DropUnique(const char* table_name, const char* field_name)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    current_tables[table_id]->DropUnique(field_name);
}
void DatabaseManager::AlterTable_AddCol(const char* table_name, const field_item_t* field)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    TableHeader* newheader = TableManager::AddCol(current_tables[table_id]->get_header(), field);
    if (newheader == nullptr)
        return;
    std::string name_temp = table_name;
    name_temp = "_" + name_temp;
    AlterTable_Rename(table_name, name_temp.c_str());
    TableManager* old_tm = current_tables[table_id];
    strcpy(current_databaseheader.tables[table_id], table_name);
    TableManager::CreateTable(newheader);
    delete newheader;
    TableManager* new_tm = new TableManager(table_name);
    current_tables[table_id] = new_tm;
    RecordManager* old_rm = old_tm->GetRecordManager();
    Record* record;
    if (old_rm->InitializeIteration()) {
        do {
            record = old_rm->CurrentRecord();
            std::vector<expr_node_t*> vals;
            for (int i = 0; i < record->num; i++) {
                vals.push_back(expression::column_to_expr_node(record->datas[i], record->type[i]));
            }
            new_tm->Insert(vals);
            for (int i = 0; i < record->num; i++) {
                free_exprnode(vals[i]);
            }
            delete record;
        } while (old_rm->NextRecord());
    }
    delete old_tm;
    TableManager::DropTable(name_temp.c_str());
}
void DatabaseManager::AlterTable_DropCol(const char* table_name, const char* field_name)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    TableManager* current_table = current_tables[table_id];
    TableHeader* newheader = TableManager::DeleteCol(current_table->get_header(), field_name);
    if (newheader == nullptr)
        return;
    int col_id = current_table->GetColumnID(field_name);
    std::string name_temp = table_name;
    name_temp = "_" + name_temp;
    AlterTable_Rename(table_name, name_temp.c_str());
    TableManager* old_tm = current_tables[table_id];
    strcpy(current_databaseheader.tables[table_id], table_name);
    TableManager::CreateTable(newheader);
    delete newheader;
    TableManager* new_tm = new TableManager(table_name);
    current_tables[table_id] = new_tm;
    RecordManager* old_rm = old_tm->GetRecordManager();
    Record* record;
    if (old_rm->InitializeIteration()) {
        do {
            record = old_rm->CurrentRecord();
            std::vector<expr_node_t*> vals;
            for (int i = 0; i < record->num; i++) {
                if (i == col_id)
                    continue;
                vals.push_back(expression::column_to_expr_node(record->datas[i], record->type[i]));
            }
            new_tm->Insert(vals);
            for (int i = 0; i < record->num - 1; i++) {
                free_exprnode(vals[i]);
            }
            delete record;
        } while (old_rm->NextRecord());
    }
    delete old_tm;
    TableManager::DropTable(name_temp.c_str());
}
void DatabaseManager::AlterTable_AddForeign(const char* table_name, const char* name, const linked_list_t* cols, const char* name_ref, const linked_list_t* cols_ref)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    current_tables[table_id]->AddForeign(name, cols, name_ref, cols_ref);
}
void DatabaseManager::AlterTable_DropForeign(const char* table_name, const char* name)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    current_tables[table_id]->DropForeign(name);
}
void DatabaseManager::AlterTable_CreateIndex(const char* table_name, const char* index_name, const linked_list_t* column_list)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    current_tables[table_id]->CreateIndex(index_name, column_list);
}
void DatabaseManager::AlterTable_DropIndex(const char* table_name, const char* index_name)
{
    if (!AssertUsing())
        return;
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return;
    }
    current_tables[table_id]->DropIndex(index_name);
}
bool DatabaseManager::AssertUsing()
{
    if (!current_database_name) {
        fprintf(stderr, "[Error] No database is used.\n");
        return false;
    }
    return true;
}

bool DatabaseManager::GetTableId(const char* table_name, int& id)
{
    for (int i = 0; i < current_databaseheader.table_num; i++) {
        if (strcmp(table_name, current_databaseheader.tables[i]) == 0) {
            id = i;
            return true;
        }
    }
    return false;
}

bool DatabaseManager::GetDBId(const char* db_name, int& id)
{
    for (int i = 0; i < current_databases.database_num; i++) {
        if (strcmp(db_name, current_databases.databases[i]) == 0) {
            id = i;
            return true;
        }
    }
    return false;
}

bool DatabaseManager::select_from_one_table(const char* table_name, expr_node_t* where, std::set<int>& rids)
{
    int table_id;
    if (!GetTableId(table_name, table_id)) {
        fprintf(stderr, "[Error] Table not found.\n");
        return false;
    }
    TableManager* tablemanager = current_tables[table_id];
    std::vector<column_ref_t*> col_refs;
    expression::extract_col_refs(where, col_refs);
    std::map<std::string, int> col_name_to_id;
    for (column_ref_t* col_ref : col_refs) {
        if (col_ref->table != nullptr && strcmp(col_ref->table, table_name) != 0) {
            fprintf(stderr, "[Error] Table not found.\n");
            return false;
        }
        int id = tablemanager->GetColumnID(col_ref->column);
        if (id == -1) {
            fprintf(stderr, "[Error] Column not found.\n");
            return false;
        }
        col_name_to_id[std::string(col_ref->column)] = id;
    }
    std::vector<expr_node_t*> and_exprs;
    expression::extract_and_cond(where, and_exprs);
    std::vector<expr_node_t*> and_exprs_remain;
    bool flag = true;
    for (expr_node_t* expr : and_exprs) {
        std::vector<column_ref_t*> and_col_refs;
        expression::extract_col_refs(expr, and_col_refs);
        IndexManager* indexmanager;
        if (and_col_refs.size() != 1) {
            and_exprs_remain.push_back(expr);
            continue;
        }

        if (expr->left->term_type == TERM_COLUMN_REF) {
            if ((indexmanager = tablemanager->GetIndexManager(col_name_to_id[std::string(expr->left->column_ref->column)])) == nullptr) {
                and_exprs_remain.push_back(expr);
                continue;
            }
            Expression expression = expression::eval(expr->right);
            int col_type = tablemanager->GetColumnType(col_name_to_id[std::string(expr->left->column_ref->column)]);
            if (!expression::type_compatible(col_type, expression)) {
                fprintf(stderr, "[Error] Type incompatible.\n");
                return false;
            }
            BufType data = expression::expr_to_buftype(expression);
            std::set<int> index_rids;
            switch (expr->op) {
            case OPERATOR_EQ:
                indexmanager->get_rid_eq(data, index_rids);
                break;
            case OPERATOR_NEQ:
                indexmanager->get_rid_neq(data, index_rids);
                break;
            case OPERATOR_GT:
                indexmanager->get_rid_gt(data, index_rids);
                break;
            case OPERATOR_GEQ:
                indexmanager->get_rid_gte(data, index_rids);
                break;
            case OPERATOR_LT:
                indexmanager->get_rid_lt(data, index_rids);
                break;
            case OPERATOR_LEQ:
                indexmanager->get_rid_lte(data, index_rids);
                break;
            default:
                and_exprs_remain.push_back(expr);
                continue;
            }
            if (rids.empty()) {
                rids.insert(index_rids.begin(), index_rids.end());
            } else {
                std::set<int> result;
                std::set_intersection(rids.begin(), rids.end(), index_rids.begin(), index_rids.end(), std::inserter(result, result.begin()));
                rids = result;
            }
            flag = false;
            if (data != nullptr)
                delete[] data;
        } else if (expr->right->term_type == TERM_COLUMN_REF) {
            if ((indexmanager = tablemanager->GetIndexManager(col_name_to_id[std::string(expr->right->column_ref->column)])) == nullptr) {
                and_exprs_remain.push_back(expr);
                continue;
            }
            Expression expression = expression::eval(expr->left);
            int col_type = tablemanager->GetColumnType(col_name_to_id[std::string(expr->right->column_ref->column)]);
            if (!expression::type_compatible(col_type, expression)) {
                fprintf(stderr, "[Error] Type incompatible.\n");
                return false;
            }
            BufType data = expression::expr_to_buftype(expression);
            std::set<int> index_rids;
            switch (expr->op) {
            case OPERATOR_EQ:
                indexmanager->get_rid_eq(data, index_rids);
                break;
            case OPERATOR_NEQ:
                indexmanager->get_rid_neq(data, index_rids);
                break;
            case OPERATOR_GT:
                indexmanager->get_rid_lt(data, index_rids);
                break;
            case OPERATOR_GEQ:
                indexmanager->get_rid_lte(data, index_rids);
                break;
            case OPERATOR_LT:
                indexmanager->get_rid_gt(data, index_rids);
                break;
            case OPERATOR_LEQ:
                indexmanager->get_rid_gte(data, index_rids);
                break;
            default:
                and_exprs_remain.push_back(expr);
                continue;
            }
            if (rids.empty()) {
                rids.insert(index_rids.begin(), index_rids.end());
            } else {
                std::set<int> result;
                std::set_intersection(rids.begin(), rids.end(), index_rids.begin(), index_rids.end(), std::inserter(result, result.begin()));
                rids = result;
            }
            flag = false;
            if (data != nullptr)
                delete[] data;
        }
    }
    if (flag || !rids.empty())
        tablemanager->Cache(rids);
    std::set<int> valid_rids;
    for (int rid : rids) {
        bool valid = true;
        for (expr_node_t* expr : and_exprs_remain) {
            expression::set_current_rid(std::string(table_name), rid);
            if (!expression::expr_to_bool(expression::eval(expr))) {
                valid = false;
                break;
            }
        }
        if (valid)
            valid_rids.insert(rid);
    }
    rids = valid_rids;
    return true;
}
bool DatabaseManager::select_from_one_table(const char* table_name, std::vector<expr_node_t*> and_exprs, std::set<int>& rids)
{
    int table_id;
    GetTableId(table_name, table_id);
    TableManager* tablemanager = current_tables[table_id];
    std::vector<column_ref_t*> col_refs;
    for (expr_node_t* and_expr : and_exprs) {
        expression::extract_col_refs(and_expr, col_refs);
    }
    std::map<std::string, int> col_name_to_id;
    for (column_ref_t* col_ref : col_refs) {
        int id = tablemanager->GetColumnID(col_ref->column);
        col_name_to_id[std::string(col_ref->column)] = id;
    }
    std::vector<expr_node_t*> and_exprs_remain;
    bool flag = true;
    for (expr_node_t* expr : and_exprs) {
        std::vector<column_ref_t*> and_col_refs;
        expression::extract_col_refs(expr, and_col_refs);
        IndexManager* indexmanager;
        if (and_col_refs.size() != 1) {
            and_exprs_remain.push_back(expr);
            continue;
        }

        if (expr->left->term_type == TERM_COLUMN_REF) {
            if ((indexmanager = tablemanager->GetIndexManager(col_name_to_id[std::string(expr->left->column_ref->column)])) == nullptr) {
                and_exprs_remain.push_back(expr);
                continue;
            }
            Expression expression = expression::eval(expr->right);
            int col_type = tablemanager->GetColumnType(col_name_to_id[std::string(expr->left->column_ref->column)]);
            if (!expression::type_compatible(col_type, expression)) {
                fprintf(stderr, "[Error] Type incompatible.\n");
                return false;
            }
            BufType data = expression::expr_to_buftype(expression);
            std::set<int> index_rids;
            switch (expr->op) {
            case OPERATOR_EQ:
                indexmanager->get_rid_eq(data, index_rids);
                break;
            case OPERATOR_NEQ:
                indexmanager->get_rid_neq(data, index_rids);
                break;
            case OPERATOR_GT:
                indexmanager->get_rid_gt(data, index_rids);
                break;
            case OPERATOR_GEQ:
                indexmanager->get_rid_gte(data, index_rids);
                break;
            case OPERATOR_LT:
                indexmanager->get_rid_lt(data, index_rids);
                break;
            case OPERATOR_LEQ:
                indexmanager->get_rid_lte(data, index_rids);
                break;
            default:
                and_exprs_remain.push_back(expr);
                continue;
            }
            if (flag) {
                rids.insert(index_rids.begin(), index_rids.end());
            } else {
                std::set<int> result;
                std::set_intersection(rids.begin(), rids.end(), index_rids.begin(), index_rids.end(), std::inserter(result, result.begin()));
                rids = result;
            }
            flag = false;
            if (data != nullptr)
                delete[] data;
        } else if (expr->right->term_type == TERM_COLUMN_REF) {
            if ((indexmanager = tablemanager->GetIndexManager(col_name_to_id[std::string(expr->right->column_ref->column)])) == nullptr) {
                and_exprs_remain.push_back(expr);
                continue;
            }
            Expression expression = expression::eval(expr->left);
            int col_type = tablemanager->GetColumnType(col_name_to_id[std::string(expr->right->column_ref->column)]);
            if (!expression::type_compatible(col_type, expression)) {
                fprintf(stderr, "[Error] Type incompatible.\n");
                return false;
            }
            BufType data = expression::expr_to_buftype(expression);
            std::set<int> index_rids;
            switch (expr->op) {
            case OPERATOR_EQ:
                indexmanager->get_rid_eq(data, index_rids);
                break;
            case OPERATOR_NEQ:
                indexmanager->get_rid_neq(data, index_rids);
                break;
            case OPERATOR_GT:
                indexmanager->get_rid_lt(data, index_rids);
                break;
            case OPERATOR_GEQ:
                indexmanager->get_rid_lte(data, index_rids);
                break;
            case OPERATOR_LT:
                indexmanager->get_rid_gt(data, index_rids);
                break;
            case OPERATOR_LEQ:
                indexmanager->get_rid_gte(data, index_rids);
                break;
            default:
                and_exprs_remain.push_back(expr);
                continue;
            }
            if (flag) {
                rids.insert(index_rids.begin(), index_rids.end());
            } else {
                std::set<int> result;
                std::set_intersection(rids.begin(), rids.end(), index_rids.begin(), index_rids.end(), std::inserter(result, result.begin()));
                rids = result;
            }
            flag = false;
            if (data != nullptr)
                delete[] data;
        }
    }
    if (flag || !rids.empty())
        tablemanager->Cache(rids);
    std::set<int> valid_rids;
    for (int rid : rids) {
        bool valid = true;
        for (expr_node_t* expr : and_exprs_remain) {
            expression::set_current_rid(std::string(table_name), rid);
            if (!expression::expr_to_bool(expression::eval(expr))) {
                valid = false;
                break;
            }
        }
        if (valid)
            valid_rids.insert(rid);
    }
    rids = valid_rids;
    return true;
}
TableManager* DatabaseManager::get_table_manager(const char* table_name)
{
    int table_id;
    if (GetTableId(table_name, table_id))
        return current_tables[table_id];
    return nullptr;
}