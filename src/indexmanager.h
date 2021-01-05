#ifndef INDEXMANAGER
#define INDEXMANAGER
#include "../filesystem/pf.h"
#include "record.h"
#include "utils.h"
#include <set>
struct TableHeader;
struct Index_FileHeader {
    int col_num;
    int cols[MAX_COL_NUM];
    int type[MAX_COL_NUM];
    int maxlen[MAX_COL_NUM];
    int m;
    int childStart;
    int keyStart;
    int keyLen;
    int rootPage;
    int nodeNum;
};

struct Index_PageHeader {
    int isLeaf;
    int keyNum;
    int prev;
    int next;
    int parent;
};

struct Index_Node {
    int index;
    Index_PageHeader* header;
    BufType child;
    BufType key;
};

class IndexManager {
private:
    FileManager* fileManager;
    BufPageManager* bufPageManager;
    int current_fileID;
    Index_FileHeader current_fileHeader;
    RecordParser* recordparser;
    int current_node, current_key;

    Index_Node* getNode(int id);
    Record* getRecord(Index_Node* node, int i);
    void setRecord(Index_Node* node, int i, Record* record);
    void setRecord(Index_Node* node, int i, Index_Node* node_src, int i_src);
    void setRecord(Index_Node* node, int i, BufType src);
    int getChildIndex(int child, Index_Node* parent);
    Record* truncate(Record* fullversion);

public:
    IndexManager(const char* filename);
    ~IndexManager();
    static bool CreateIndex(const char* filename, int num, const int* cols, const TableHeader* tableheader);
    static bool DeleteIndex(const char* filename);
    void Insert(Record* fullversion);
    void Delete(Record* fullversion);
    bool LowerBound(BufType record);
    bool UpperBound(BufType record);
    bool Exists(Record* fullversion, int exclude_rid);
    bool ExistsPart(Record* partversion, int exclude_rid);
    bool Prev();
    bool Next();
    bool CheckNotNull(Record* fullversion);
    Record* GetCurrent();
    void get_rid_gt(BufType record, std::set<int>& rids);
    void get_rid_lt(BufType record, std::set<int>& rids);
    void get_rid_gte(BufType record, std::set<int>& rids);
    void get_rid_lte(BufType record, std::set<int>& rids);
    void get_rid_eq(BufType record, std::set<int>& rids);
    void get_rid_neq(BufType record, std::set<int>& rids);
    void get_rid(std::set<int>& rids);
};
#endif