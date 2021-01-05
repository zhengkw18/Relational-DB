#ifndef RECORD
#define RECORD
#include "../filesystem/utils/pagedef.h"
#include "varcharmanager.h"
struct Record {
    int iscopy;
    int rid;
    int num;
    int* type;
    BufType* datas;
    ~Record()
    {
        if (iscopy) {
            delete[] type;
            for (int i = 0; i < num; i++) {
                if (datas[i] != nullptr)
                    delete[] datas[i];
            }
        }
        delete[] datas;
    }
};

class RecordParser {
private:
    int cols_num;
    int bitmap_len;
    int tot_len;
    int* type;
    int* maxlen;
    int* aligned_len;
    VarcharManager* varcharmanager;

public:
    //传入的指针会被复制一份
    //返回的指针需要外部删除
    RecordParser(int* _type, int* _maxlen, int num, VarcharManager* _varcharmanager);
    ~RecordParser();
    Record* toRecord(int rid, const BufType data);
    Record* toRecord_nocopy(int rid, const BufType data);
    Record* toRecordPart(Record* full, int* cols, int cols_num);
    BufType toData(const Record* record, bool with_rid);
    void DeleteVarchar(const BufType data);

    static int compare(Record* record1, Record* record2, bool compare_rid);
    static int compare(int type, BufType data1, BufType data2);
};
#endif