#ifndef RECORD_MANAGER
#define RECORD_MANAGER
#include "../filesystem/pf.h"
#include "record.h"
struct TableHeader;
struct Record_FileHeader {
    int col_num;
    int col_type[MAX_COL_NUM];
    int col_length[MAX_COL_NUM];
    int recordSize;
    int recordPerPage;
    int firstFreePage;
    int pageNum;
    int bitmapStart;
    int bitmapSize;
    int recordStart;
};

struct Record_PageHeader {
    int nextFreePage;
    int recordNum;
};

class RecordManager {
private:
    FileManager* fileManager;
    BufPageManager* bufPageManager;
    int current_fileID;
    Record_FileHeader current_fileHeader;
    RecordParser* recordparser;
    // rid = (pageID - 1) * recordPerPage + slotID
    int current_pageID, current_slotID;
    bool SearchNext();

public:
    RecordManager(const char* filename, VarcharManager* varcharmanager);
    ~RecordManager();
    static bool CreateFile(const char* filename, const TableHeader* tableheader);
    static bool DeleteFile(const char* filename);

    Record* GetRecord(int rid);
    void InsertRecord(int& rid, const Record* record);
    bool UpdateRecord(int rid, const Record* record);
    bool DeleteRecord(int rid);

    bool InitializeIteration();
    Record* CurrentRecord();
    int current_rid();
    bool NextRecord();
    RecordParser* GetParser();
};

#endif