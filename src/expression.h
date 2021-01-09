#ifndef EXPRESSION
#define EXPRESSION
#include "../parser/defs.h"
#include "../parser/free.h"
#include "comparator.h"
#include "databasemanager.h"
#include "tuple.h"
#include "utils.h"
#include <iomanip>
#include <sstream>
#include <unordered_map>
extern std::unordered_map<
    std::tuple<std::string, std::string, int>, // (table_name, col_name, rid)
    Expression // expression
    >
    cache;
extern std::unordered_map<
    std::string, // table_name
    int // rid
    >
    current_rids;
extern std::unordered_map<
    select_single_info_t*,
    linked_list_t*>
    in_where_cache;
extern std::string current_table;
extern std::vector<linked_list_t*> copy_values;
namespace expression {
inline term_type_t column_to_term(int type)
{
    switch (type) {
    case TYPE_INT:
        return TERM_INT;
    case TYPE_FLOAT:
        return TERM_FLOAT;
    case TYPE_DATE:
        return TERM_DATE;
    case TYPE_CHAR:
        return TERM_STRING;
    case TYPE_VARCHAR:
        return TERM_STRING;
    default:
        assert(false);
    }
}

inline Expression column_to_expr(BufType data, int type, int rid)
{
    Expression expr;
    expr.rid = rid;
    if (data == nullptr) {
        expr.type = TERM_NULL;
        return expr;
    }
    switch (type) {
    case TYPE_INT:
        expr.val_i = *(int*)data;
        expr.type = TERM_INT;
        break;
    case TYPE_FLOAT:
        expr.val_f = *(float*)data;
        expr.type = TERM_FLOAT;
        break;
    case TYPE_DATE:
        expr.val_i = *(int*)data;
        expr.type = TERM_DATE;
        break;
    case TYPE_CHAR:
        expr.val_s = new char[strlen((char*)data) + 1];
        strcpy(expr.val_s, (char*)data);
        expr.type = TERM_STRING;
        break;
    case TYPE_VARCHAR:
        expr.val_s = new char[strlen((char*)data) + 1];
        strcpy(expr.val_s, (char*)data);
        expr.type = TERM_STRING;
        break;
    default:
        assert(false);
    }
    return expr;
}

inline expr_node_t* column_to_expr_node(BufType data, int type)
{
    expr_node_t* expr = (expr_node_t*)calloc(1, sizeof(expr_node_t));
    expr->op = OPERATOR_NONE;
    if (data == nullptr) {
        expr->term_type = TERM_NULL;
        return expr;
    }
    switch (type) {
    case TYPE_INT:
        expr->val_i = *(int*)data;
        expr->term_type = TERM_INT;
        break;
    case TYPE_FLOAT:
        expr->val_f = *(float*)data;
        expr->term_type = TERM_FLOAT;
        break;
    case TYPE_DATE: {
        char date_buf[32];
        time_t time = *(int*)data;
        auto tm = std::localtime(&time);
        std::strftime(date_buf, 32, DATE_FORMAT, tm);
        expr->val_s = new char[strlen(date_buf) + 1];
        expr->term_type = TERM_DATE;
        break;
    }
    case TYPE_CHAR:
        expr->val_s = new char[strlen((char*)data) + 1];
        strcpy(expr->val_s, (char*)data);
        expr->term_type = TERM_STRING;
        break;
    case TYPE_VARCHAR:
        expr->val_s = new char[strlen((char*)data) + 1];
        strcpy(expr->val_s, (char*)data);
        expr->term_type = TERM_STRING;
        break;
    default:
        assert(false);
    }
    return expr;
}

inline bool expr_to_bool(const Expression& expr)
{
    switch (expr.type) {
    case TERM_INT:
        return expr.val_i != 0;
    case TERM_FLOAT:
        return expr.val_f != 0;
    case TERM_BOOL:
        return expr.val_b;
    case TERM_STRING:
        return expr.val_s[0] != 0; // not empty string
    case TERM_NULL:
        return false;
    default:
        return false;
    }
}

inline bool type_compatible(int col_type, const Expression& val)
{
    switch (val.type) {
    case TERM_NULL:
        return true;
    case TERM_INT:
        return col_type == TYPE_INT;
    case TERM_FLOAT:
        return col_type == TYPE_FLOAT;
    case TERM_STRING:
        return col_type == TYPE_CHAR || col_type == TYPE_VARCHAR;
    case TERM_DATE:
        return col_type == TYPE_DATE;
    default:
        return false;
    }
}
inline void clear_cache(bool delete_string)
{
    if (delete_string) {
        for (auto it = cache.begin(); it != cache.end(); ++it) {
            if (it->second.type == TERM_STRING)
                delete[] it->second.val_s;
        }
    }
    cache.clear();
}

inline void clear_in_where_cache()
{
    for (auto it = in_where_cache.begin(); it != in_where_cache.end(); ++it) {
        free_linked_list<expr_node_t>(it->second, free_exprnode);
    }
    in_where_cache.clear();
}

inline void set_current_table(const std::string table)
{
    current_table = table;
}
inline void replace_column(const std::string table, const std::string col, int rid, const BufType data, int col_type)
{
    std::tuple<std::string, std::string, int> ident = std::make_tuple(table, col, rid);
    Expression new_expr = column_to_expr(data, col_type, rid);
    Expression old_expr = cache[ident];
    if (old_expr.type == TERM_STRING)
        delete[] old_expr.val_s;
    cache[ident] = new_expr;
}
inline void cache_column(const std::string table, const std::string col, int rid, const BufType data, int col_type)
{
    std::tuple<std::string, std::string, int> ident = std::make_tuple(table, col, rid);
    cache.insert(std::make_pair(ident, column_to_expr(data, col_type, rid)));
}
inline void cache_expr(const std::string table, const std::string col, int rid, Expression expr)
{
    std::tuple<std::string, std::string, int> ident = std::make_tuple(table, col, rid);
    cache.insert(std::make_pair(ident, expr));
}
inline void set_current_rid(std::string table, int rid)
{
    current_rids[table] = rid;
}

inline Expression eval_terminal_column_ref(const expr_node_t* expr)
{
    assert(expr->term_type == TERM_COLUMN_REF);
    std::string col = expr->column_ref->column;
    std::string table = (expr->column_ref->table == nullptr) ? current_table : expr->column_ref->table;
    auto rid_it = current_rids.find(table);
    if (rid_it != current_rids.end()) {
        auto it = cache.find(std::make_tuple(table, col, rid_it->second));
        if (it != cache.end()) {
            return it->second;
        }
    }

    assert(false);
}

inline int eval_date(const char* str)
{
    std::tm tm {};
    std::string date(str);
    std::stringstream ss(date);
    ss >> std::get_time(&tm, DATE_FORMAT);
    std::tm tm_orig = tm;
    if (ss.fail())
        return -1;

    int time = std::mktime(&tm);
    if (tm_orig.tm_mday != tm.tm_mday)
        return -1;

    return time;
}

inline Expression eval_terminal(const expr_node_t* expr)
{
    Expression ret;
    ret.type = expr->term_type;
    switch (expr->term_type) {
    case TERM_INT:
        ret.val_i = expr->val_i;
        break;
    case TERM_FLOAT:
        ret.val_f = expr->val_f;
        break;
    case TERM_STRING:
        ret.val_s = expr->val_s;
        break;
    case TERM_BOOL:
        ret.val_b = expr->val_b;
        break;
    case TERM_DATE:
        ret.val_i = eval_date(expr->val_s);
        if (ret.val_i == -1) {
            ret.type = TERM_STRING;
            ret.val_s = expr->val_s;
        }
        break;
    case TERM_NULL:
        break;
    case TERM_COLUMN_REF:
        ret = eval_terminal_column_ref(expr);
        break;
    case TERM_LITERAL_LIST:
        ret.literal_list = expr->literal_list;
        break;
    case TERM_IN_WHERE:
        ret.in_where = expr->in_where;
        break;
    default:
        assert(0);
        break;
    }

    return ret;
}

inline Expression eval_float_operands(operator_type_t op, float a, float b)
{
    Expression ret;
    switch (op) {
    /* arithmetic */
    case OPERATOR_ADD:
        ret.val_f = a + b;
        ret.type = TERM_FLOAT;
        break;
    case OPERATOR_MINUS:
        ret.val_f = a - b;
        ret.type = TERM_FLOAT;
        break;
    case OPERATOR_DIV:
        ret.val_f = a / b;
        ret.type = TERM_FLOAT;
        break;
    case OPERATOR_MUL:
        ret.val_f = a * b;
        ret.type = TERM_FLOAT;
        break;
    case OPERATOR_NEGATE:
        ret.val_f = -a;
        ret.type = TERM_FLOAT;
        break;
    /* compare */
    case OPERATOR_EQ:
        ret.val_b = a == b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_GEQ:
        ret.val_b = a >= b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_LEQ:
        ret.val_b = a <= b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_NEQ:
        ret.val_b = a != b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_GT:
        ret.val_b = a > b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_LT:
        ret.val_b = a < b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_ISNULL:
        ret.val_b = false;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_NOTNULL:
        ret.val_b = true;
        ret.type = TERM_BOOL;
        break;
    default:
        assert(false);
    }

    return ret;
}

inline Expression eval_date_operands(operator_type_t op, int a, int b)
{
    Expression ret;
    switch (op) {
    /* compare */
    case OPERATOR_EQ:
        ret.val_b = a == b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_GEQ:
        ret.val_b = a >= b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_LEQ:
        ret.val_b = a <= b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_NEQ:
        ret.val_b = a != b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_GT:
        ret.val_b = a > b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_LT:
        ret.val_b = a < b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_ISNULL:
        ret.val_b = false;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_NOTNULL:
        ret.val_b = true;
        ret.type = TERM_BOOL;
        break;
    default:
        assert(false);
    }

    return ret;
}

inline Expression eval_int_operands(operator_type_t op, int a, int b)
{
    Expression ret;
    switch (op) {
    /* arithmetic */
    case OPERATOR_ADD:
        ret.val_i = a + b;
        ret.type = TERM_INT;
        break;
    case OPERATOR_MINUS:
        ret.val_i = a - b;
        ret.type = TERM_INT;
        break;
    case OPERATOR_DIV:
        ret.val_i = a / b;
        ret.type = TERM_INT;
        break;
    case OPERATOR_MUL:
        ret.val_i = a * b;
        ret.type = TERM_INT;
        break;
    case OPERATOR_NEGATE:
        ret.val_i = -a;
        ret.type = TERM_INT;
        break;
    /* compare */
    case OPERATOR_EQ:
        ret.val_b = a == b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_GEQ:
        ret.val_b = a >= b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_LEQ:
        ret.val_b = a <= b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_NEQ:
        ret.val_b = a != b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_GT:
        ret.val_b = a > b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_LT:
        ret.val_b = a < b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_ISNULL:
        ret.val_b = false;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_NOTNULL:
        ret.val_b = true;
        ret.type = TERM_BOOL;
        break;
    default:
        assert(false);
    }

    return ret;
}

inline Expression eval_bool_operands(operator_type_t op, bool a, bool b)
{
    Expression ret;
    switch (op) {
    /* logical */
    case OPERATOR_AND:
        ret.val_b = a & b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_OR:
        ret.val_b = a | b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_EQ:
        ret.val_b = a == b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_NEQ:
        ret.val_b = a != b;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_ISNULL:
        ret.val_b = false;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_NOTNULL:
        ret.val_b = true;
        ret.type = TERM_BOOL;
        break;
    default:
        assert(false);
    }

    return ret;
}

inline Expression eval_string_operands(operator_type_t op, const char* a, const char* b)
{
    Expression ret;
    switch (op) {
    /* compare */
    case OPERATOR_EQ:
        ret.val_b = strcasecmp(a, b) == 0;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_NEQ:
        ret.val_b = strcasecmp(a, b) != 0;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_LIKE:
        ret.val_b = strlike(a, b);
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_ISNULL:
        ret.val_b = false;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_NOTNULL:
        ret.val_b = true;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_GEQ:
        ret.val_b = string_comparer(a, b) >= 0;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_GT:
        ret.val_b = string_comparer(a, b) > 0;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_LEQ:
        ret.val_b = string_comparer(a, b) <= 0;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_LT:
        ret.val_b = string_comparer(a, b) < 0;
        ret.type = TERM_BOOL;
        break;
    default:
        assert(false);
    }

    return ret;
}

inline Expression eval_null_operands(operator_type_t op)
{
    Expression ret;
    switch (op) {
    /* compare */
    case OPERATOR_ISNULL:
        ret.val_b = true;
        ret.type = TERM_BOOL;
        break;
    case OPERATOR_NOTNULL:
        ret.val_b = false;
        ret.type = TERM_BOOL;
        break;
    default:
        ret.type = TERM_NULL;
        break;
    }

    return ret;
}

inline bool eval_in_expression(Expression left, linked_list_t* literal_list)
{
    for (linked_list_t* l_ptr = literal_list; l_ptr; l_ptr = l_ptr->next) {
        expr_node_t* val = (expr_node_t*)l_ptr->data;
        assert(val->op == OPERATOR_NONE);
        switch (left.type) {
        case TERM_INT:
            if (val->term_type == TERM_INT) {
                if (left.val_i == val->val_i)
                    return true;
            } else {
                assert(false);
            }
            break;
        case TERM_FLOAT:
            if (val->term_type == TERM_FLOAT) {
                if (left.val_f == val->val_f)
                    return true;
            } else {
                assert(false);
            }
            break;
        case TERM_STRING:
            if (val->term_type == TERM_STRING || val->term_type == TERM_DATE) {
                if (std::strcmp(left.val_s, val->val_s) == 0)
                    return true;
            } else {
                assert(false);
            }
            break;
        case TERM_DATE:
            if (val->term_type == TERM_DATE) {
                Expression right = eval_terminal(val);
                if (left.val_i == right.val_i)
                    return true;
            } else {
                assert(false);
            }
            break;
        case TERM_NULL:
            return false;
        default:
            assert(false);
        }
    }

    return false;
}

inline Expression eval(const expr_node_t* expr)
{
    assert(expr != nullptr);
    if (expr->op == OPERATOR_NONE) {
        // terminator
        return eval_terminal(expr);
    }

    assert(expr->term_type == TERM_NONE);

    // non-terminator
    bool is_unary = (expr->op & OPERATOR_UNARY);
    Expression left = eval(expr->left);
    Expression right = is_unary ? Expression() : eval(expr->right);

    if (!is_unary && right.type == TERM_NULL) {
        if (expr->op == OPERATOR_OR) {
            return left;
        }
        Expression ret;
        ret.type = TERM_NULL;
        return ret;
    }
    if (!is_unary && left.type == TERM_NULL && expr->op == OPERATOR_OR) {
        return right;
    }
    if (expr->op == OPERATOR_IN) {
        bool ret = eval_in_expression(left, right.literal_list);
        Expression expr;
        expr.type = TERM_BOOL;
        expr.val_b = ret;
        return expr;
    } else if (expr->op == OPERATOR_IN_WHERE) {
        linked_list_t* vals;
        if (in_where_cache.count(right.in_where) > 0) {
            vals = in_where_cache[right.in_where];
        } else {
            vals = DatabaseManager::get_instance()->select_in_where(right.in_where->table, right.in_where->where, right.in_where->expr);
            in_where_cache.insert(std::make_pair(right.in_where, vals));
            // for (linked_list_t* l_ptr = vals; l_ptr; l_ptr = l_ptr->next) {
            //     expr_node_t* val = (expr_node_t*)l_ptr->data;
            //     assert(val->op == OPERATOR_NONE);
            //     switch (val->term_type) {
            //     case TERM_INT:
            //         printf("%d\n", val->val_i);
            //         break;
            //     default:
            //         assert(false);
            //     }
            // }
        }
        bool ret = eval_in_expression(left, vals);
        Expression expr;
        expr.type = TERM_BOOL;
        expr.val_b = ret;
        return expr;
    }

    if (!is_unary && left.type != right.type && left.type != TERM_NULL)
        assert(false);
    switch (left.type) {
    case TERM_INT:
        return eval_int_operands(expr->op, left.val_i, right.val_i);
    case TERM_FLOAT:
        return eval_float_operands(expr->op, left.val_f, right.val_f);
    case TERM_DATE:
        return eval_date_operands(expr->op, left.val_i, right.val_i);
    case TERM_BOOL:
        return eval_bool_operands(expr->op, left.val_b, right.val_b);
    case TERM_STRING:
        return eval_string_operands(expr->op, left.val_s, right.val_s);
    case TERM_NULL:
        return eval_null_operands(expr->op);
    default:
        assert(false);
    }
}

inline bool is_aggregate(const expr_node_t* expr)
{
    return expr->op == OPERATOR_COUNT
        || expr->op == OPERATOR_SUM
        || expr->op == OPERATOR_AVG
        || expr->op == OPERATOR_MIN
        || expr->op == OPERATOR_MAX;
}

inline std::string to_string(const expr_node_t* expr)
{
    if (!expr)
        return "*";
    if (expr->op == OPERATOR_NONE) {
        // terminator
        switch (expr->term_type) {
        case TERM_INT: {
            std::ostringstream ss;
            ss << expr->val_i;
            return ss.str();
        }
        case TERM_FLOAT: {
            std::ostringstream ss;
            ss << expr->val_f;
            return ss.str();
        }
        case TERM_BOOL:
            return expr->val_b ? "TRUE" : "FALSE";
        case TERM_STRING:
            return "'" + std::string(expr->val_s) + "'";
        case TERM_COLUMN_REF:
            if (expr->column_ref->table) {
                return std::string(expr->column_ref->table) + "." + expr->column_ref->column;
            } else {
                return expr->column_ref->column;
            }
        case TERM_NULL:
            return "NULL";
        case TERM_DATE: {
            return "'" + std::string(expr->val_s) + "'";
        }
        case TERM_LITERAL_LIST: {
            std::string str = "(";
            std::vector<expr_node_t*> exprs;
            for (linked_list_t* l_ptr = expr->literal_list; l_ptr; l_ptr = l_ptr->next) {
                exprs.push_back((expr_node_t*)l_ptr->data);
            }
            for (int i = 0; i < exprs.size(); i++) {
                if (i != 0)
                    str += ",";
                str += expression::to_string(exprs[i]);
            }
            str += ")";
            return str;
        }
        default:
            return "";
        }
    } else {
        // non-terminator
        std::string str = to_string(expr->left);
        if (expr->op & OPERATOR_UNARY) {
            switch (expr->op) {
            case OPERATOR_SUM:
                return "SUM(" + str + ")";
            case OPERATOR_AVG:
                return "AVG(" + str + ")";
            case OPERATOR_MIN:
                return "MIN(" + str + ")";
            case OPERATOR_MAX:
                return "MAX(" + str + ")";
            case OPERATOR_COUNT:
                return "COUNT(" + str + ")";
            case OPERATOR_NEGATE:
                return "-" + str;
            case OPERATOR_ISNULL:
                return str + " IS NULL";
            case OPERATOR_NOTNULL:
                return str + "IS NOT NULL";
            case OPERATOR_NOT:
                return "NOT " + str;
            default:
                return str;
            }
        }

        switch (expr->op) {
        case OPERATOR_ADD:
            str += '+';
            break;
        case OPERATOR_MINUS:
            str += '-';
            break;
        case OPERATOR_DIV:
            str += '/';
            break;
        case OPERATOR_MUL:
            str += '*';
            break;
        case OPERATOR_AND:
            str += " AND ";
            break;
        case OPERATOR_OR:
            str += " OR ";
            break;
        case OPERATOR_EQ:
            str += '=';
            break;
        case OPERATOR_GEQ:
            str += ">=";
            break;
        case OPERATOR_LEQ:
            str += "<=";
            break;
        case OPERATOR_NEQ:
            str += "!=";
            break;
        case OPERATOR_GT:
            str += '>';
            break;
        case OPERATOR_LT:
            str += '<';
            break;
        case OPERATOR_IN:
            str += " IN ";
            break;
        case OPERATOR_LIKE:
            str += " LIKE ";
            break;
        default:
            str += ' ';
            break;
        }

        return str + to_string(expr->right);
    }
}
inline std::string to_poland(const expr_node_t* expr)
{
    int count = 0;
    std::ostringstream os;
    os << expr->op << " ";
    if (expr->op == OPERATOR_NONE) {
        os << expr->term_type << " ";
        switch (expr->term_type) {
        case TERM_INT:
            os << expr->val_i << " ";
            break;
        case TERM_FLOAT:
            os << expr->val_f << " ";
            break;
        case TERM_STRING:
            os << expr->val_s << " ";
            break;
        case TERM_BOOL:
            os << expr->val_b << " ";
            break;
        case TERM_NULL:
            break;
        case TERM_DATE:
            os << expr->val_s << " ";
            break;
        case TERM_COLUMN_REF:
            if (expr->column_ref->table) {
                os << 2 << " "
                   << expr->column_ref->table << " "
                   << expr->column_ref->column << " ";
            } else {
                os << 1 << " "
                   << expr->column_ref->column << " ";
            }
            break;
        case TERM_LITERAL_LIST:
            for (linked_list_t* l_ptr = expr->literal_list; l_ptr; l_ptr = l_ptr->next)
                ++count;
            os << count << " ";
            for (linked_list_t* l_ptr = expr->literal_list; l_ptr; l_ptr = l_ptr->next) {
                os << to_poland((expr_node_t*)l_ptr->data);
            }
            break;
        default:
            assert(0);
            break;
        }
    } else {
        bool is_unary = (expr->op & OPERATOR_UNARY);
        os << to_poland(expr->left);
        if (!is_unary) {
            os << to_poland(expr->right);
        }
    }
    return os.str();
}

inline char* load_string(std::istream& is)
{
    std::string str;
    is >> str;
    char* buf = (char*)malloc(str.size() + 1);
    std::strcpy(buf, str.c_str());
    return buf;
}

inline expr_node_t* from_poland(std::istream& is)
{
    expr_node_t* expr = (expr_node_t*)calloc(1, sizeof(expr_node_t));
    int tmp;
    is >> tmp;
    expr->op = (operator_type_t)tmp;
    if (expr->op == OPERATOR_NONE) {
        is >> tmp;
        expr->term_type = (term_type_t)tmp;
        switch (expr->term_type) {
        case TERM_INT:
            is >> expr->val_i;
            break;
        case TERM_FLOAT:
            is >> expr->val_f;
            break;
        case TERM_STRING:
            expr->val_s = load_string(is);
            break;
        case TERM_BOOL:
            is >> expr->val_b;
            break;
        case TERM_NULL:
            break;
        case TERM_DATE:
            expr->val_s = load_string(is);
            break;
        case TERM_COLUMN_REF:
            is >> tmp;
            expr->column_ref = (column_ref_t*)malloc(sizeof(column_ref_t));
            if (tmp == 2) {
                expr->column_ref->table = load_string(is);
                expr->column_ref->column = load_string(is);
            } else {
                expr->column_ref->table = nullptr;
                expr->column_ref->column = load_string(is);
            }
            break;
        case TERM_LITERAL_LIST:
            is >> tmp;
            expr->literal_list = NULL;
            for (int i = 0; i != tmp; ++i) {
                linked_list_t* l_ptr = (linked_list_t*)malloc(sizeof(linked_list_t));
                l_ptr->next = expr->literal_list;
                l_ptr->data = from_poland(is);
                expr->literal_list = l_ptr;
            }
            break;
        default:
            assert(0);
            break;
        }
    } else {
        bool is_unary = (expr->op & OPERATOR_UNARY);
        expr->left = from_poland(is);
        if (!is_unary)
            expr->right = from_poland(is);
    }

    return expr;
}
inline void extract_and_cond(expr_node_t* cond, std::vector<expr_node_t*>& and_cond)
{
    if (cond == nullptr)
        return;
    if (cond->op == OPERATOR_AND) {
        extract_and_cond(cond->left, and_cond);
        extract_and_cond(cond->right, and_cond);
    } else {
        and_cond.push_back(cond);
    }
}
inline void extract_col_refs(expr_node_t* expr, std::vector<column_ref_t*>& col_refs)
{
    if (expr == nullptr)
        return;
    if (expr->op == OPERATOR_NONE) {
        switch (expr->term_type) {
        case TERM_COLUMN_REF:
            col_refs.push_back(expr->column_ref);
            break;
        case TERM_LITERAL_LIST:
            for (linked_list_t* l_ptr = expr->literal_list; l_ptr; l_ptr = l_ptr->next) {
                extract_col_refs((expr_node_t*)l_ptr->data, col_refs);
            }
            break;
        default:
            break;
        }
    } else {
        bool is_unary = (expr->op & OPERATOR_UNARY);
        extract_col_refs(expr->left, col_refs);
        if (!is_unary) {
            extract_col_refs(expr->right, col_refs);
        }
    }
}
inline std::string poland_to_string(std::istream& is)
{
    expr_node_t* expr = from_poland(is);
    std::string re = to_string(expr);
    free_exprnode(expr);
    return re;
}
inline BufType expr_to_buftype(Expression& expr)
{
    BufType ret;
    switch (expr.type) {
    case TERM_INT:
        ret = new uint[1];
        memcpy(ret, &expr.val_i, 1 * sizeof(int));
        break;
    case TERM_FLOAT:
        ret = new uint[1];
        memcpy(ret, &expr.val_f, 1 * sizeof(int));
        break;
    case TERM_DATE:
        ret = new uint[1];
        memcpy(ret, &expr.val_i, 1 * sizeof(int));
        break;
    case TERM_STRING:
        ret = new uint[ceil(strlen(expr.val_s) + 1, 4)];
        strcpy((char*)ret, expr.val_s);
        break;
    case TERM_NULL:
        ret = nullptr;
        break;
    default:
        assert(false);
    }
    return ret;
}
inline void print_Expr(Expression expr)
{
    switch (expr.type) {
    case TERM_INT:
        std::printf("%d", expr.val_i);
        break;
    case TERM_FLOAT:
        std::printf("%f", expr.val_f);
        break;
    case TERM_STRING:
        std::printf("%s", expr.val_s);
        break;
    case TERM_BOOL:
        std::printf("%s", expr.val_b ? "TRUE" : "FALSE");
        break;
    case TERM_DATE: {
        char date_buf[32];
        time_t time = expr.val_i;
        auto tm = std::localtime(&time);
        std::strftime(date_buf, 32, DATE_FORMAT, tm);
        std::printf("%s", date_buf);
        break;
    }
    case TERM_NULL:
        std::printf("NULL");
        break;
    default:
        assert(false);
    }
}
inline Expression expr_to_Expr(const expr_node_t* expr)
{
    Expression ret;
    ret.type = expr->term_type;
    switch (expr->term_type) {
    case TERM_INT:
        ret.val_i = expr->val_i;
        break;
    case TERM_FLOAT:
        ret.val_f = expr->val_f;
        break;
    case TERM_STRING:
        ret.val_s = strdup(expr->val_s);
        break;
    case TERM_DATE:
        ret.val_i = eval_date(expr->val_s);
        if (ret.val_i == -1) {
            ret.type = TERM_STRING;
            ret.val_s = expr->val_s;
        }
        break;
    case TERM_NULL:
        break;
    default:
        assert(0);
        break;
    }

    return ret;
}
inline expr_node_t* Expr_to_expr(const Expression expr)
{
    expr_node_t* ret = (expr_node_t*)calloc(1, sizeof(expr_node_t));
    ret->term_type = expr.type;
    switch (expr.type) {
    case TERM_INT:
        ret->val_i = expr.val_i;
        break;
    case TERM_FLOAT:
        ret->val_f = expr.val_f;
        break;
    case TERM_STRING:
        ret->val_s = strdup(expr.val_s);
        break;
    case TERM_DATE: {
        char date_buf[32];
        time_t time = expr.val_i;
        auto tm = std::localtime(&time);
        std::strftime(date_buf, 32, DATE_FORMAT, tm);
        ret->val_s = strdup(date_buf);
        break;
    }
    case TERM_NULL:
        break;
    default:
        assert(0);
        break;
    }

    return ret;
}
}
#endif