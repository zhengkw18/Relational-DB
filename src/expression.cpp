#include "expression.h"
std::unordered_map<
    std::tuple<std::string, std::string, int>, // (table_name, col_name, rid)
    Expression // expression
    >
    cache;
std::unordered_map<
    std::string, // table_name
    int // rid
    >
    current_rids;
std::string current_table;