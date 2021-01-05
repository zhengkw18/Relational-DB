#ifndef UTILS
#define UTILS
#include "../filesystem/utils/pagedef.h"
#include "../parser/defs.h"
#include "comparator.h"
#include <assert.h>
#include <string.h>

inline int ceil(int num, int divider)
{
    return (num == 0) ? 0 : (num - 1) / divider + 1;
}

inline void resetBitmap(uint* bitmap, int size)
{
    int len = ceil(size, 32);
    memset(bitmap, 0, len * sizeof(uint));
}

inline void setBit(uint* bitmap, int pos, bool val)
{
    if (val)
        bitmap[pos >> 5] |= 1u << (pos & 31);
    else
        bitmap[pos >> 5] &= ~(1u << (pos & 31));
}

inline bool getBit(const uint* bitmap, int pos)
{
    return (bitmap[pos >> 5] & (1u << (pos & 31))) > 0;
}

inline int getLowest1(unsigned int v)
{
    v ^= v - 1;
    v = (v & 0x55555555) + ((v >> 1) & 0x55555555);
    v = (v & 0x33333333) + ((v >> 2) & 0x33333333);
    v = (v + (v >> 4)) & 0x0F0F0F0F;
    v += v >> 8;
    v += v >> 16;

    return (v & 0x3F) - 1;
}

inline int getNextBit0(const uint* bitmap, int size, int start)
{
    int i = start;
    while ((i & 31) != 0 && i < size) {
        if (!getBit(bitmap, i))
            return i;
        i++;
    }
    while (i < size) {
        if (bitmap[i >> 5] == 0xffffffffu)
            i += 32;
        else {
            i += getLowest1(~bitmap[i >> 5]);
            if (i < size)
                return i;
        }
    }
    return -1;
}

inline int getNextBit1(const uint* bitmap, int size, int start)
{
    int i = start;
    while ((i & 31) != 0 && i < size) {
        if (getBit(bitmap, i))
            return i;
        i++;
    }
    while (i < size) {
        if (bitmap[i >> 5] == 0x00000000u)
            i += 32;
        else {
            i += getLowest1(bitmap[i >> 5]);
            if (i < size)
                return i;
        }
    }
    return -1;
}

#define MAX_VARCHAR_PAGE_NUM 60000

//int integer bigint
#define TYPE_INT 1
//float decimal
#define TYPE_FLOAT 2
#define TYPE_DATE 3
//char不能太长
#define TYPE_CHAR 4
//varchar长度不限
#define TYPE_VARCHAR 5

#define MAX_NAME_LEN 32
#define MAX_DEFAULT_LEN 128
#define MAX_CHECK_CONSTRAINT_NUM 16
#define MAX_CHECK_CONSTRAINT_LEN 256
#define MAX_INDEX_NUM 16
#define MAX_TABLE_NUM 16
#define MAX_DATABASE_NUM 16
#define MAX_FOREIGN_NUM 5

#define DATE_FORMAT "%Y-%m-%d"

#define MAX_SELECT_TABLE_NUM 5

struct RID {
    int rids[MAX_SELECT_TABLE_NUM];
    bool operator<(const RID& right) const
    {
        for (int i = 0; i < MAX_SELECT_TABLE_NUM; i++) {
            if (rids[i] < right.rids[i])
                return true;
            if (rids[i] > right.rids[i])
                return false;
        }
        return false;
    }
};
struct Expression {
    union {
        char* val_s;
        int val_i;
        float val_f;
        bool val_b;
        linked_list_t* literal_list;
    };

    term_type_t type;
    int rid;
    bool operator<(const Expression& right) const
    {
        if (type == TERM_NULL) {
            if (right.type == TERM_NULL) {
                return false;
            } else {
                return true;
            }
        } else if (right.type == TERM_NULL) {
            return false;
        }
        switch (type) {
        case TERM_INT:
            return integer_comparer(val_i, right.val_i) < 0;
        case TERM_FLOAT:
            return float_comparer(val_f, right.val_f) < 0;
        case TERM_STRING:
            return string_comparer(val_s, right.val_s) < 0;
        default:
            return false;
        }
    }
    bool operator==(const Expression& right) const
    {
        if (type == TERM_NULL) {
            if (right.type == TERM_NULL) {
                return true;
            } else {
                return false;
            }
        } else if (right.type == TERM_NULL) {
            return false;
        }
        switch (type) {
        case TERM_INT:
            return integer_comparer(val_i, right.val_i) == 0;
        case TERM_FLOAT:
            return float_comparer(val_f, right.val_f) == 0;
        case TERM_STRING:
            return string_comparer(val_s, right.val_s) == 0;
        default:
            return false;
        }
    }
};
#endif