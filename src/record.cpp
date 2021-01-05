#include "record.h"
#include "comparator.h"
#include "utils.h"
#include <assert.h>
RecordParser::RecordParser(int* _type, int* _maxlen, int num, VarcharManager* _varcharmanager)
{
    cols_num = num;
    bitmap_len = ceil(num, 32);
    type = new int[num];
    memcpy(type, _type, num * sizeof(int));
    maxlen = new int[num];
    memcpy(maxlen, _maxlen, num * sizeof(int));
    aligned_len = new int[num];
    tot_len = bitmap_len;
    for (int i = 0; i < num; i++) {
        if (type[i] == TYPE_CHAR)
            aligned_len[i] = ceil(maxlen[i] + 1, 4);
        else
            aligned_len[i] = 1;
        tot_len += aligned_len[i];
    }
    varcharmanager = _varcharmanager;
}
RecordParser::~RecordParser()
{
    delete[] type, maxlen, aligned_len;
}
Record* RecordParser::toRecord(int rid, const BufType data)
{
    Record* record = new Record;
    record->iscopy = 1;
    record->rid = rid;
    record->num = cols_num;
    record->type = new int[cols_num];
    memcpy(record->type, type, cols_num * sizeof(int));
    record->datas = new BufType[cols_num];
    int offset = bitmap_len;
    for (int i = 0; i < cols_num; i++) {
        if (getBit(data, i)) { //null
            record->datas[i] = nullptr;
            offset += aligned_len[i];
            continue;
        }
        switch (record->type[i]) {
        case TYPE_INT:
            record->datas[i] = new uint[1];
            memcpy(record->datas[i], data + offset, 1 * sizeof(int));
            offset += 1;
            break;
        case TYPE_FLOAT:
            record->datas[i] = new uint[1];
            memcpy(record->datas[i], data + offset, 1 * sizeof(int));
            offset += 1;
            break;
        case TYPE_DATE:
            record->datas[i] = new uint[1];
            memcpy(record->datas[i], data + offset, 1 * sizeof(int));
            offset += 1;
            break;
        case TYPE_CHAR:
            record->datas[i] = new uint[aligned_len[i]];
            memcpy(record->datas[i], data + offset, aligned_len[i] * sizeof(int));
            offset += aligned_len[i];
            break;
        case TYPE_VARCHAR: {
            int len;
            BufType buf = new uint[ceil(maxlen[i] + 1, 4)];
            int pageID = *(data + offset);
            varcharmanager->ReadVarchar(pageID, buf, len);
            *((char*)buf + len) = 0;
            record->datas[i] = buf;
            offset += 1;
            break;
        }
        default:
            assert(false);
        }
    }
    return record;
}
Record* RecordParser::toRecordPart(Record* full, int* cols, int cols_num)
{
    Record* record = new Record;
    record->iscopy = 1;
    record->rid = full->rid;
    record->num = cols_num;
    record->type = new int[cols_num];
    record->datas = new BufType[cols_num];
    for (int i = 0; i < cols_num; i++) {
        record->type[i] = type[cols[i]];
        if (full->datas[cols[i]] == nullptr) {
            record->datas[i] == nullptr;
            continue;
        }
        switch (type[cols[i]]) {
        case TYPE_INT:
            record->datas[i] = new uint[1];
            memcpy(record->datas[i], full->datas[cols[i]], 1 * sizeof(int));
            break;
        case TYPE_FLOAT:
            record->datas[i] = new uint[1];
            memcpy(record->datas[i], full->datas[cols[i]], 1 * sizeof(int));
            break;
        case TYPE_DATE:
            record->datas[i] = new uint[1];
            memcpy(record->datas[i], full->datas[cols[i]], 1 * sizeof(int));
            break;
        case TYPE_CHAR:
            record->datas[i] = new uint[aligned_len[i]];
            memcpy(record->datas[i], full->datas[cols[i]], aligned_len[i] * sizeof(int));
            break;
        case TYPE_VARCHAR: {
            record->datas[i] = new uint[ceil(strlen((char*)full->datas[cols[i]]) + 1, 4)];
            strcpy((char*)record->datas[i], (char*)full->datas[cols[i]]);
            break;
        }
        default:
            assert(false);
        }
    }
    return record;
}
void RecordParser::DeleteVarchar(const BufType data)
{
    int offset = bitmap_len;
    for (int i = 0; i < cols_num; i++) {
        if (getBit(data, i)) { //null
            offset += aligned_len[i];
            continue;
        }
        switch (type[i]) {
        case TYPE_VARCHAR: {
            int pageID = *(data + offset);
            varcharmanager->DeleteVarchar(pageID);
            offset += 1;
            break;
        }
        default:
            offset += aligned_len[i];
            break;
        }
    }
}

Record* RecordParser::toRecord_nocopy(int rid, const BufType data)
{
    Record* record = new Record;
    record->iscopy = 0;
    record->rid = rid;
    record->num = cols_num;
    record->type = type;
    record->datas = new BufType[cols_num];
    int offset = bitmap_len;
    for (int i = 0; i < cols_num; i++) {
        if (getBit(data, i)) { //null
            record->datas[i] = nullptr;
        } else {
            record->datas[i] = data + offset;
        }
        offset += aligned_len[i];
    }
    return record;
}

BufType RecordParser::toData(const Record* record, bool with_rid)
{
    BufType data, buf;
    if (with_rid) {
        buf = new uint[tot_len + 1];
        *buf = record->rid;
        data = buf + 1;
    } else {
        buf = new uint[tot_len];
        data = buf;
    }
    int offset = bitmap_len;
    for (int i = 0; i < record->num; i++) {
        if (record->datas[i] == nullptr) {
            setBit(data, i, 1);
            offset += aligned_len[i];
            continue;
        } else {
            setBit(data, i, 0);
        }
        switch (record->type[i]) {
        case TYPE_INT:
            memcpy(data + offset, record->datas[i], 1 * sizeof(int));
            offset += 1;
            break;
        case TYPE_FLOAT:
            memcpy(data + offset, record->datas[i], 1 * sizeof(int));
            offset += 1;
            break;
        case TYPE_DATE:
            memcpy(data + offset, record->datas[i], 1 * sizeof(int));
            offset += 1;
            break;
        case TYPE_CHAR:
            memcpy(data + offset, record->datas[i], aligned_len[i] * sizeof(int));
            offset += aligned_len[i];
            break;
        case TYPE_VARCHAR:
            int pageID;
            varcharmanager->AddVarchar(pageID, record->datas[i], strlen((char*)record->datas[i]));
            *(data + offset) = pageID;
            offset += 1;
            break;
        default:
            assert(false);
            break;
        }
    }
    return buf;
}

int RecordParser::compare(Record* record1, Record* record2, bool compare_rid)
{
    for (int i = 0; i < record1->num; i++) {
        int res = compare(record1->type[i], record1->datas[i], record2->datas[i]);
        if (res != 0)
            return res;
    }
    if (compare_rid)
        return integer_comparer(record1->rid, record2->rid);
    else
        return 0;
}

int RecordParser::compare(int type, BufType data1, BufType data2)
{
    if (data1 == nullptr) {
        if (data2 == nullptr) {
            return 0;
        } else {
            return -1;
        }
    } else if (data2 == nullptr) {
        return 1;
    }
    switch (type) {
    case TYPE_INT:
        return integer_binary_comparer((char*)data1, (char*)data2);
    case TYPE_FLOAT:
        return float_binary_comparer((char*)data1, (char*)data2);
    case TYPE_DATE:
        return integer_binary_comparer((char*)data1, (char*)data2);
    case TYPE_CHAR:
        return string_comparer((char*)data1, (char*)data2);
    case TYPE_VARCHAR:
        return string_comparer((char*)data1, (char*)data2);
    default:
        assert(false);
        break;
    }
}