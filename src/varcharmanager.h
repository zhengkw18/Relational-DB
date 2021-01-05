#ifndef VARCHARMANAGER
#define VARCHARMANAGER
#include "../filesystem/pf.h"
#include "utils.h"

struct Varchar_FileHeader {
    int firstFreePage;
    int pageNum;
    uint bitmap[MAX_VARCHAR_PAGE_NUM >> 5];
};

struct Varchar_PageHeader {
    int hasNext;
    int dataLen;
    int next;
};
#define VARCHAR_MAX_LEN_PER_PAGE ((PAGE_SIZE - sizeof(Varchar_PageHeader)) / sizeof(int))
class VarcharManager {
private:
    FileManager* fileManager;
    BufPageManager* bufPageManager;
    int current_fileID;
    Varchar_FileHeader current_fileHeader;

public:
    VarcharManager(const char* filename);
    ~VarcharManager();
    static bool CreateFile(const char* filename);
    static bool DeleteFile(const char* filename);
    void AddVarchar(int& pageID, const BufType data, int len);
    bool ReadVarchar(int pageID, BufType data, int& len);
    bool DeleteVarchar(int pageID);
};

#endif