#include <vector>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "bpt.hpp"
#include <sstream>
#include <assert.h>
#include <list>

//parsing - join 쌍  && tableread 목적
//query optimizer - 1. 테이블 잘 이어지도록 순서 설정, 2. 파이프라이닝 형태로 조인
//join - hash?

using std:: vector;
using std:: stringstream;
using std:: string;

int buffer_pool = 0;
int LRU_start = -1;
int LRU_end = -1;
int table_list[Tid_MAX+1] = {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
buffer_table buf_table;


table_read result;
vector<table_read> pre_read;
vector< vector<hash_set> > reserved_hash_table(10);
pagenum_t table_size[Tid_MAX+1] = {0,0,0,0,0,0,0,0,0,0,0};
int64_t max_value[Tid_MAX+1][16] = {-1,};

volatile int tid_count;
vector<trx_table> trx_list;
pthread_mutex_t lock_table_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t trx_begin_mutex = PTHREAD_MUTEX_INITIALIZER;
lock_hash_table* lock_hash_t;


int64_t join(const char *query){
    
    int64_t position_list[Tid_MAX+1] = {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
    vector<join_operater> pt;
    pt = query_order(query_optimizer(parse(pt, query)),position_list);
    
    make_join(pt, 0, position_list);
    
    uint i = 0;
    uint j = 0;
    int64_t sum = 0;
    
    if(result.table.empty())
        return 0;
    else{
        for(j = 0; j < 11; j++){
            if(position_list[j] != -1)
                for(i = 0; i < result.table.size(); i++)
                    sum += result.table[i][position_list[j]];
        }
        return sum;
    }
    return 0;
}

vector<join_operater> parse(vector<join_operater> pt, const char *query){
    
    join_operater jop;
    string qbuffer(query);
    stringstream query_buffer(qbuffer);
    
    while(1) {
        int ltid, lcol, rtid, rcol;
        char equal, point, empersand;
        query_buffer >> ltid >> point >> lcol;
        assert(point == '.');
        query_buffer >> equal;
        assert(equal == '=');
        query_buffer >> rtid >> point >> rcol;
        assert(point == '.');
        jop.left_tid = ltid;
        jop.left_col = lcol;
        jop.right_tid = rtid;
        jop.right_col = rcol;
        pt.push_back(jop);
        if(!(query_buffer >> empersand) || empersand != '&')
            break;
        empersand = '\0';
    }
    return pt;
}

vector<join_operater> query_optimizer(vector<join_operater> pt){
    int i = 0;
    pagenum_t min_size = table_size[pt[0].left_tid] + table_size[pt[0].right_tid];
    int pos = 0;
    
    for(i = 1; i < pt.size(); i++)
        if(min_size > table_size[pt[i].left_tid] + table_size[pt[i].right_tid]){
            min_size = table_size[pt[i].left_tid] + table_size[pt[i].right_tid];
            pos = i;
        }
    
    if(pos != 0){
        join_operater temp = pt[pos];
        pt.erase(pt.begin()+pos);
        pt.insert(pt.begin(), temp);
    }
    return pt;
}

vector<join_operater> query_order(vector<join_operater> pt, int64_t position_list[]){
    
    int i = 1;
    int64_t result = 0;
    bool check[10] = {false,false,false,false,false,false,false,false,false,false};
    check[pt[0].left_tid-1] = true;
    check[pt[0].right_tid-1] = true;
    position_list[pt[0].left_tid] = 0;
    position_list[pt[0].right_tid] = pre_read[pt[0].left_tid-1].table[0].size();
    result = pre_read[pt[0].left_tid-1].table[0].size() + pre_read[pt[0].right_tid-1].table[0].size();
    
    while(i < pt.size())
        if(check[pt[i].left_tid-1] == false && check[pt[i].right_tid-1] == false)
            pt = swap_join_op(pt, i);
        else{
            if (check[pt[i].left_tid-1] == false){
                check[pt[i].left_tid-1] = true;
                pt[i] = change_join_op(pt[i]);
                
                position_list[pt[i].right_tid] = result;
                result += pre_read[pt[i].right_tid-1].table[0].size();
            }
            else if(check[pt[i].right_tid-1] == false){
                check[pt[i].right_tid-1] = true;
                
                position_list[pt[i].right_tid] = result;
                result += pre_read[pt[i].right_tid-1].table[0].size();
            }
            i++;
        }
    return pt;
}


join_operater change_join_op(join_operater pt){
    int t_id = pt.left_tid;
    int col = pt.left_col;
    
    pt.left_tid = pt.right_tid;
    pt.left_col = pt.right_col;
    
    pt.right_tid = t_id;
    pt.right_col = col;
    
    return pt;
}

vector<join_operater> swap_join_op(vector<join_operater> pt, pagenum_t index){
    join_operater temp = pt[index];
    pt.erase(pt.begin()+index);
    pt.push_back(temp);
    return pt;
}

table_read tableread(int table_id){
    
    page_header* header_page = buffer_read_header_page(table_id);
    pagenum_t n_offset = header_page->root_offset;
    pagenum_t header_col_num = header_page->col_num - 1;
    buffer_put_page(table_id, 0, false);
    
    page* leaf_page = buffer_read_node_page(table_id, n_offset/PSIZE);
    
    while (leaf_page->isleaf != LEAF) {
        buffer_put_page(table_id, n_offset/PSIZE, false);
        n_offset = leaf_page->another_offset;
        leaf_page = buffer_read_node_page(table_id, n_offset/PSIZE);
    }
    
    //------------------------leftmost_leaf_page_finding----------------------------
    
    table_read t_read;
    vector<int64_t> record;
    int64_t max[16] = {-1,};
    
    int i = 0;
    int j = 0;
    
    while(i >= 0){
        for (i = 0; i < leaf_page->key_num; i++) {
            record.push_back(leaf_page->record[i].key);
            if(max[1] < leaf_page->record[i].key)
                max[1] = leaf_page->record[i].key;
            for(j = 0; j < header_col_num; j++){
                record.push_back(leaf_page->record[i].value[j]);
                if(max[j+2] < leaf_page->record[i].value[j])
                    max[j+2] = leaf_page->record[i].value[j];
            }
            
            t_read.table.push_back(record);
            record.clear();
        }
        
        if(leaf_page->another_offset == 0)
            break;
        else{
            buffer_put_page(table_id, n_offset/PSIZE, false);
            n_offset = leaf_page->another_offset;
            leaf_page = buffer_read_node_page(table_id, n_offset/PSIZE);
        }
    }
    for(i = 1; i < 16; i++)
        max_value[table_id][i] = max[i];
    buffer_put_page(table_id, n_offset/PSIZE, false);
    return t_read;
}



void make_join(vector<join_operater> pt, pagenum_t index, int64_t position_list[]){
    
    table_read left_table;
    table_read right_table;
    int64_t code_size = 10;
    
    if(index < pt.size()){
        
        if(index == 0){
            left_table = pre_read[pt[index].left_tid-1];
            result = left_table;
        }else
            left_table = result;
        
        right_table = pre_read[pt[index].right_tid-1];
        code_size = max_value[pt[index].left_tid][pt[index].left_col];
        
        hash_join(make_hash_table(left_table, code_size, position_list[pt[index].left_tid] + pt[index].left_col - 1), right_table, code_size, pt[index].right_col);
        
        if(result.table.empty())
            index = pt.size();
        
        make_join(pt, ++index, position_list);
    }
}

void hash_join(vector< vector<hash_set> > hash_table, table_read table, int64_t code_size, int right_col){
    
    uint i = 0;
    uint j = 0;
    uint col_position = right_col-1;
    vector< vector<int64_t> > v;
    vector<int64_t> record;
    int64_t hashcode_result;
    
    for (i = 0; i < table.table.size(); i++) {
        hashcode_result = hashcode(table.table[i][col_position], code_size);
        if (!hash_table[hashcode_result].empty())
            for (j = 0; j < hash_table[hashcode_result].size(); j++){
                if(hash_table[hashcode_result][j].value == table.table[i][col_position]){
                    record.insert(record.begin(), result.table[hash_table[hashcode_result][j].row_num].begin(),result.table[hash_table[hashcode_result][j].row_num].end());
                    record.insert(record.begin()+result.table[hash_table[hashcode_result][j].row_num].size(), table.table[i].begin(), table.table[i].end());
                    v.push_back(record);
                    record.clear();
                }
            }
    }
    
    result.table = v;
}

vector< vector<hash_set> > make_hash_table(table_read table, int64_t code_size, pagenum_t col_p){
    vector< vector<hash_set> > hash_table(code_size);
    pagenum_t col_position = 0;
    hash_set set;
    
    col_position = col_p;
    
    uint i = 0;
    for(i = 0; i < table.table.size(); i++){
        set.value = table.table[i][col_position];
        set.row_num = i;
        hash_table[hashcode(set.value,code_size)].push_back(set);
    }
    
    return hash_table;
}

int64_t hashcode(int64_t value, int64_t code_size){ return value%code_size; }

int64_t hashsize(int64_t code_size){
    
    int count = 0;
    int size = 1;
    while(code_size != 0){
        code_size /= 10;
        count++;
    }
    for (; count > 0; count--)
        size *= 10;
    return size;
}

int open_table(char* pathname, int num_column){
    
    int i = 1;
    while (i < Tid_MAX+1){
        if(table_list[i] == -1)
            break;
        i++;
    }
    if (i == 11){
        printf("table id is full\n");
        return -1;
    }
    // 2 <= num_column <= 16
    mode_t mode = S_IRUSR | S_IWUSR| S_IRGRP| S_IWGRP| S_IROTH| S_IWOTH;
    table_list[i] = open(pathname, O_RDWR);
    //pre_read.push_back(tableread(i));
    //table_size[i] = pre_read[i-1].table.size();
    
    if(table_list[i] < 0){
        table_list[i] = open(pathname, O_CREAT | O_RDWR, mode);
        page_header* header_page = buffer_read_new_header(i);
        header_page->free_offset = 0;
        header_page->root_offset = 0;
        header_page->page_num = 1;
        header_page->col_num = num_column;
        buffer_put_page(i, 0, true);
        buffer_alloc_page(i);
    }
    
    return i;
}

int close_table(int table_id){
    
    buffer_manager *buffer = buf_table.buffer;
    
    if(table_id < 0){
        printf("table_id is negative\n");
        return -1;
    }
    for (int i = 0; i < buffer_pool; i++)
        if (buffer[i].is_pinned != 0) {
            printf("%luth page is not unpinned\n",buffer[i].page_num);
        }else if (buffer[i].table_id == table_id)
            delete_buffer(buffer[i].whatp, i);
    table_list[table_id] = -1;
    return 0;
}

int shutdown_db(){
    
    for (int tid = 0; tid < 10; tid++)
        if (table_list[tid] != -1)
            close_table(tid);
    free(buf_table.buffer);
    return 0;
}

int init_db(int num_buf){
    
    tid_count = 0;
    trx_table first_trx;
    trx_list.push_back(first_trx);
    lock_hash_t = (lock_hash_table*)malloc(sizeof(lock_hash_table)*num_buf);
    
    
    buffer_pool = num_buf;
    LRU_start = -1;
    LRU_end = -1;
    buf_table.buffer = (buffer_manager*)malloc(sizeof(buffer_manager)*num_buf);
    pthread_mutex_init(&buf_table.buffer_entire_mutex, NULL);
    if(buf_table.buffer == NULL)
        return -1;
    
    for (int i = 0; i < num_buf; i++) {
        buf_table.buffer[i].next = -1;
        buf_table.buffer[i].prev = -1;
        buf_table.buffer[i].table_id = -1;
        buf_table.buffer[i].is_pinned = 0;
        buf_table.buffer[i].is_dirty = false;
        buf_table.buffer[i].whatp = 0;
        buf_table.buffer[i].no_steal = false;
        pthread_mutex_init(&buf_table.buffer[i].buffer_each_mutex, NULL);
    }
    return 0;
}

int request_page(int table_id, pagenum_t pagenum, int whatp){ //있으면 그대로 반환 없으면 만듬
    int i = 0;
    int j = -1;
    
    
    if(table_id < 0){
        printf("This table is not openned\n");
        return -1;
    }
    
    
    i = 0;
    while(i < buffer_pool){
        if(buf_table.buffer[i].page_num == pagenum && buf_table.buffer[i].table_id == table_id)
            break;
        if(j == -1 && buf_table.buffer[i].table_id == -1)
            j = i;
        i++;
    }
    
    
    if (i != buffer_pool){ // 이미 있을경우 - 1. 하나만 있음, 2. 여러개 있음
        buf_table.buffer[i].is_pinned++;
        if (i == LRU_start)//하나만 있음 또는 제일앞임
            return i;
        
        if(i == LRU_end){
            buf_table.buffer[buf_table.buffer[i].prev].next = buf_table.buffer[i].next;
            LRU_end = buf_table.buffer[i].prev;
        }
        else if(i != LRU_start && i != LRU_end){
            buf_table.buffer[buf_table.buffer[i].prev].next = buf_table.buffer[i].next;
            buf_table.buffer[buf_table.buffer[i].next].prev = buf_table.buffer[i].prev;
        }
        
        buf_table.buffer[i].prev = buf_table.buffer[LRU_start].prev; //제일앞에 넣음
        buf_table.buffer[LRU_start].prev = i;
        buf_table.buffer[i].next = LRU_start;
        LRU_start = i;
        
        return i;
    }
    else{ //없을 경우 - 1. 자리가 있음, 2. 자리가 없음
        if(j != -1){//1.LRU 최초 생성, 2.다른 LRU 이미있음
            buf_table.buffer[j].is_pinned++;
            buf_table.buffer[j].page_num = pagenum;
            buf_table.buffer[j].is_dirty = false;
            buf_table.buffer[j].whatp = whatp;
            
            if(LRU_start == -1 && LRU_end == -1){
                buf_table.buffer[j].next = -1;
                buf_table.buffer[j].prev = -1;
                LRU_start = j;
                LRU_end = j;
            }else{
                buf_table.buffer[j].prev = buf_table.buffer[LRU_start].prev;
                buf_table.buffer[LRU_start].prev = j;
                buf_table.buffer[j].next = LRU_start;
                LRU_start = j;
            }
            
            return j;
            
        }else{ //자리없떵
            int end = LRU_end;
            while (buf_table.buffer[end].is_pinned != 0 && buf_table.buffer[end].no_steal == true && end != -1) //LRU end가 쓰고있는 상황이면 뒤로 옮김
                end = buf_table.buffer[end].prev;
            if(end == -1){
                printf("All buffer is pinned\n");
                return end;
            }
            if(delete_buffer(whatp,end) != 0){
                printf("delete buffer error\n");
                return -1;
            }
            
            buf_table.buffer[end].is_pinned++;
            buf_table.buffer[end].page_num = pagenum;
            buf_table.buffer[end].is_dirty = false;
            buf_table.buffer[i].whatp = whatp;
            
            buf_table.buffer[end].prev = buf_table.buffer[LRU_start].prev;
            buf_table.buffer[LRU_start].prev = end;
            buf_table.buffer[end].next = LRU_start;
            LRU_start = end;
            
            return end;
        }
    }
    return -1;
}

int delete_buffer(int whatp, int end){ // 뺄 버퍼 종류랑 배열번호만 알면 뺄 수 있음
    
    buffer_manager *buffer = buf_table.buffer;
    
    if(buffer[end].is_dirty == true){ //수정사항이 있을 경우
        if(whatp == headerp)
            file_write_header_page(buffer[end].table_id, buffer[end].header_node);
        else if (whatp == nodep)
            file_write_node_page(buffer[end].table_id, buffer[end].page_num*PSIZE, buffer[end].another_node);
        else if (whatp == freep)
            file_write_free_page(buffer[end].table_id, buffer[end].page_num*PSIZE, buffer[end].free_node);
    }
    
    if(whatp == headerp){
        free(buffer[end].header_node);
        buffer[end].header_node = NULL;
    }
    else if (whatp == nodep){
        free(buffer[end].another_node);
        buffer[end].another_node = NULL;
    }
    else if (whatp == freep){
        free(buffer[end].free_node);
        buffer[end].free_node = NULL;
    }
    
    
    
    buffer[end].is_pinned = 0;
    buffer[end].is_dirty = false;
    buffer[end].table_id = -1;
    
    if (LRU_start != LRU_end) {
        if (end == LRU_start){
            buffer[buffer[end].next].prev = buffer[end].prev;
            LRU_start = buffer[end].next;
        }
        else if(end == LRU_end){
            buffer[buffer[end].prev].next = buffer[end].next;
            LRU_end = buffer[end].prev;
        }
        else if(end != LRU_start && end != LRU_end){
            buffer[buffer[end].prev].next = buffer[end].next;
            buffer[buffer[end].next].prev = buffer[end].prev;
        }
    }
    buffer[end].next = -1;
    buffer[end].prev = -1;
    
    return 0;
}


page_header* buffer_read_header_page(int table_id){
    
    buffer_manager *buffer = buf_table.buffer;
    int buffer_num = request_page(table_id, 0, headerp);
    if(buffer[buffer_num].table_id == -1){
        buffer[buffer_num].table_id = table_id;
        buffer[buffer_num].header_node = (page_header*)malloc(PSIZE);
        file_read_header_page(table_id, buffer[buffer_num].header_node);
    }
    
    
    return buffer[buffer_num].header_node;
}

page_header* buffer_read_new_header(int table_id){
    
    buffer_manager *buffer = buf_table.buffer;
    int new_num = request_page(table_id, 0, headerp);
    buffer[new_num].table_id = table_id;
    buffer[new_num].header_node = (page_header*)malloc(PSIZE);
    
    return buffer[new_num].header_node;
}

page_free* buffer_read_free_page(int table_id, pagenum_t pagenum){
    
    buffer_manager *buffer = buf_table.buffer;
    int buffer_num = request_page(table_id, pagenum, freep);
    if(buffer[buffer_num].table_id == -1){
        buffer[buffer_num].table_id = table_id;
        buffer[buffer_num].free_node = (page_free*)malloc(PSIZE);
        file_read_free_page(table_id, pagenum*PSIZE, buffer[buffer_num].free_node);
    }
    
    return buffer[buffer_num].free_node;
}

page_free* buffer_read_new_free(int table_id, pagenum_t pagenum){
    
    buffer_manager *buffer = buf_table.buffer;
    int new_num = request_page(table_id, pagenum, freep);
    buffer[new_num].table_id = table_id;
    buffer[new_num].free_node = (page_free*)malloc(PSIZE);
    
    return buffer[new_num].free_node;
}

page* buffer_read_node_page(int table_id, pagenum_t pagenum){
    
    buffer_manager *buffer = buf_table.buffer;
    int buffer_num = request_page(table_id, pagenum, nodep);
    
    if(buffer[buffer_num].whatp == freep){
        if(buffer[buffer_num].is_dirty == true){
            file_write_free_page(table_id, (pagenum_t)pagenum*PSIZE, buffer[buffer_num].free_node);
            buffer[buffer_num].is_dirty = false;
        }
        /*free(buffer[buffer_num].free_node);
         buffer[buffer_num].free_node = NULL;
         buffer[buffer_num].another_node = (page*)malloc(PSIZE);*/
        buffer[buffer_num].table_id = table_id;
        buffer[buffer_num].whatp = nodep;
        return buffer[buffer_num].another_node;
    }
    
    if(buffer[buffer_num].table_id == -1){
        buffer[buffer_num].table_id = table_id;
        buffer[buffer_num].another_node = (page*)malloc(PSIZE);
        file_read_node_page(table_id, pagenum*PSIZE, buffer[buffer_num].another_node);
    }
    
    return buffer[buffer_num].another_node;
}

void buffer_put_page(int table_id, pagenum_t pagenum, bool dirty){
    buffer_manager *buffer = buf_table.buffer;
    int i = 0;
    for (i = LRU_start; i != -1; i = buffer[i].next){
        if (buffer[i].table_id == table_id && buffer[i].page_num == pagenum)
            break;
    }
    if(dirty == true)
        buffer[i].is_dirty = true;
    buffer[i].is_pinned--;
    //printf("%d %llu %d\n",buffer[i].is_pinned, buffer[i].page_num, buffer[i].whatp);
}

void buffer_alloc_page(int table_id){
    
    page_header* header_page = buffer_read_header_page(table_id);
    header_page->free_offset = (header_page->page_num)*PSIZE;
    
    page_free* free_page = buffer_read_new_free(table_id, header_page->page_num);
    
    for(int i = 0; i < 30; i++){
        free_page->next = (header_page->page_num+1)*PSIZE;
        buffer_put_page(table_id, header_page->page_num++, true);
        free_page = buffer_read_new_free(table_id, header_page->page_num);
    }
    free_page->next = 0;
    buffer_put_page(table_id, header_page->page_num++, true);
    
    buffer_put_page(table_id, 0, true);
}

void buffer_free_page(int table_id, pagenum_t offset){
    
    buffer_manager *buffer = buf_table.buffer;
    page_header* header_page = buffer_read_header_page(table_id);
    
    int free_page = request_page(table_id, offset/PSIZE, freep);
    buffer[free_page].table_id = table_id;
    buffer[free_page].whatp = freep;
    free(buffer[free_page].another_node);
    
    buffer[free_page].free_node = (page_free*)malloc(PSIZE);
    buffer[free_page].free_node->next = header_page->free_offset;
    header_page->free_offset = offset;
    
    buffer_put_page(table_id, header_page->free_offset/PSIZE, true);
    buffer_put_page(table_id, 0, true);
}




//****************************************************************************************************
//****************************************************************************************************
//****************************************************************************************************
//****************************************************************************************************



void file_read_header_page(int table_id, page_header* dest){
    if(pread(table_list[table_id], dest, PSIZE,0) == -1)
        printf("FRHeader read error\n");
    //if (fdatasync(table_list[table_id]) == -1)
    //    printf("FRHeader datasync error\n");
}

void file_write_header_page(int table_id, page_header* src){
    if(pwrite(table_list[table_id], src, PSIZE,0) == -1)
        printf("FWHeader write error\n");
    //if (fdatasync(table_list[table_id]) == -1)
    //    printf("FWHeader datasync error\n");
}

void file_read_free_page(int table_id, pagenum_t offset, page_free* dest){
    if(pread(table_list[table_id], dest, PSIZE,offset) == -1)
        printf("FR read error\n");
    //if (fdatasync(table_list[table_id]) == -1)
    //    printf("FR datasync error\n");
}

void file_write_free_page(int table_id, pagenum_t offset, page_free* src){
    if(pwrite(table_list[table_id], src, PSIZE,offset) == -1)
        printf("FW write error\n");
    //if (fdatasync(table_list[table_id]) == -1)
    //    printf("FW datasync error\n");
}

void file_write_node_page(int table_id, pagenum_t offset, page* src){
    if(pwrite(table_list[table_id], src, PSIZE,offset) == -1)
        printf("FWph write error\n");
    //if (fdatasync(table_list[table_id]) == -1)
    //    printf("FWph datasync error\n");
}

void file_read_node_page(int table_id, pagenum_t offset, page* dest){
    if(pread(table_list[table_id], dest, PSIZE,offset) == -1)
        printf("FRph read error\n");
    // if (fdatasync(table_list[table_id]) == -1)
    //     printf("FRph datasync error\n");
}

int cut(int length){
    if(length%2 == 0)
        return length/2;
    else
        return length/2+1;
}

int get_neighbor_index(page* internal, pagenum_t offset){
    
    int i;
    
    if(internal->another_offset == offset)
        return -2;
    
    for(i=0; i < internal->key_num; i++)
        if(internal->child[i].page_offset == offset)
            return i-1;
    
    printf("get_neighbor_index error\n");
    return 0;
}

int get_left_index(page* parent, pagenum_t left_offset){
    int left_index = 0;
    if(parent->another_offset == left_offset)
        return -1;
    while(left_index < parent->key_num && parent->child[left_index].page_offset != left_offset)
        left_index++;
    return left_index;
}

//********************************************************************************************************************************************
pagenum_t find_leaf_offset(int table_id, int64_t key){
    
    page_header* header_page = buffer_read_header_page(table_id);
    //printf("root p : %llu, key : %llu\n",header_page->root_offset/PSIZE,key);
    pagenum_t n = header_page->root_offset; //루트 오프셋
    if(n == 0){
        buffer_put_page(table_id, 0, false);
        return n;
    }
    
    page* root = buffer_read_node_page(table_id, n/PSIZE);
    if(root->isleaf == LEAF){
        buffer_put_page(table_id, n/PSIZE, false);
        buffer_put_page(table_id, 0, false);
        return n;
    }
    int i = 0;
    
    while(root->isleaf != LEAF){
        i = 0;
        while(i < root->key_num){
            if(key >= root->child[i].key)
                i++;
            else{
                i--;
                break;
            }
        }
        if(i == root->key_num)
            i--;
        
        if(i == -1){
            buffer_put_page(table_id, n/PSIZE, false);
            n = root->another_offset;
            root = buffer_read_node_page(table_id,n/PSIZE);
        }else{
            buffer_put_page(table_id, n/PSIZE, false);
            n = root->child[i].page_offset;
            root = buffer_read_node_page(table_id, n/PSIZE);
        }
    }
    buffer_put_page(table_id, n/PSIZE, false);
    buffer_put_page(table_id, 0, false);
    return n;
}

leaf find_record(int table_id, int64_t key){
    
    page_header* header_page = buffer_read_header_page(table_id);
    struct leaf r;
    r.key = 0;
    r.value[0] = 0;
    
    if(header_page->root_offset == 0){
        buffer_put_page(table_id, 0, false);
        return r;
    }
    
    int i = 0;
    pagenum_t leaf_pnum = find_leaf_offset(table_id, key)/PSIZE;
    page* leaf = buffer_read_node_page(table_id, leaf_pnum);
    
    for(i = 0; i < leaf->key_num; i++)
        if(leaf->record[i].key == key)
            break;
    
    if(i == leaf->key_num){
        buffer_put_page(table_id, leaf_pnum, false);
        buffer_put_page(table_id, 0, false);
        return r;
        
    }else{
        r.key = leaf->record[i].key;
        memcpy(r.value, leaf->record[i].value, (header_page->col_num-1)*sizeof(int64_t));
        buffer_put_page(table_id, leaf_pnum, false);
        buffer_put_page(table_id, 0, false);
        return r;
    }
}

void make_new_tree(int table_id, int64_t key, int64_t* value, page_header* header_page){
    
    page_free* free_page = buffer_read_free_page(table_id, header_page->free_offset/PSIZE);
    
    header_page->root_offset = header_page->free_offset;
    header_page->free_offset = free_page->next;
    
    buffer_put_page(table_id, header_page->root_offset/PSIZE, false);
    page* leaf_page = buffer_read_node_page(table_id, header_page->root_offset/PSIZE);
    leaf_page->parent_offset = 0;
    leaf_page->isleaf = LEAF;
    leaf_page->key_num = 0;
    leaf_page->another_offset = 0;
    leaf_page->record[leaf_page->key_num].key = key;
    
    memcpy(leaf_page->record[leaf_page->key_num++].value, value, (header_page->col_num-1)*sizeof(int64_t));
    buffer_put_page(table_id, header_page->root_offset/PSIZE, true);
}

void insert_into_node(page* parent, pagenum_t parent_offset, pagenum_t right_offset, int left_index, int64_t key){
    int i = 0;
    
    for(i = parent->key_num; i > left_index+1; i--){
        parent->child[i].key = parent->child[i-1].key;
        parent->child[i].page_offset = parent->child[i-1].page_offset;
    }
    parent->child[i].key = key;
    parent->child[i].page_offset = right_offset;
    parent->key_num++;
}

void insert_into_node_after_splitting(int table_id, page* parent, pagenum_t parent_offset,pagenum_t right_offset, int left_index, int64_t key){
    
    int i,j,split;
    int64_t k_prime;
    page* right_page;
    int64_t temp_key[iorder];
    pagenum_t temp_offset[iorder];
    
    page_header* header_page = buffer_read_header_page(table_id);
    page_free* free_page = buffer_read_free_page(table_id, header_page->free_offset/PSIZE);
    pagenum_t offset = header_page->free_offset;
    header_page->free_offset = free_page->next;
    
    buffer_put_page(table_id, 0, true);
    right_page = buffer_read_node_page(table_id, offset/PSIZE);
    buffer_put_page(table_id, offset/PSIZE, false);
    
    for(i = 0, j = 0; i < parent->key_num; i++, j++){ //어차피 parent가 왼쪽이라 another offset 옮길 필요 x
        if(j == left_index+1) j++;
        temp_offset[j] = parent->child[i].page_offset;
        temp_key[j] = parent->child[i].key;
    }
    
    temp_offset[left_index+1] = right_offset;
    temp_key[left_index+1] = key;
    
    split = cut(iorder-1);
    
    right_page->parent_offset = 0;
    right_page->isleaf = INTERNAL;
    right_page->key_num = 0;
    right_page->another_offset = 0;
    parent->key_num = 0;
    
    for(i = 0; i < split; i++){
        parent->child[i].page_offset = temp_offset[i];
        parent->child[i].key = temp_key[i];
        parent->key_num++;
    }
    k_prime = temp_key[split];
    right_page->another_offset = temp_offset[split];
    for(++i,j=0; i<iorder; i++,j++){
        right_page->child[j].key = temp_key[i];
        right_page->child[j].page_offset = temp_offset[i];
        right_page->key_num++;
    }
    
    right_page->parent_offset = parent->parent_offset;
    
    
    page* head;
    for(i = -1; i < right_page->key_num; i++){
        if(i == -1){
            head = buffer_read_node_page(table_id, right_page->another_offset/PSIZE);
            head->parent_offset = offset;
            buffer_put_page(table_id, right_page->another_offset/PSIZE, true);
        }else{
            head = buffer_read_node_page(table_id, right_page->child[i].page_offset/PSIZE);
            head->parent_offset = offset;
            buffer_put_page(table_id, right_page->child[i].page_offset/PSIZE, true);
        }
    }
    if(insert_into_parent(table_id, parent_offset,parent,k_prime,offset,right_page) != 0)
        printf("ERROR\n");
    buffer_put_page(table_id, offset/PSIZE, true);
}

void insert_into_new_root(int table_id, pagenum_t left_offset, page* left,pagenum_t right_offset, int64_t key, page* right, page_header* header_page){
    
    page_free* free_page = buffer_read_free_page(table_id, header_page->free_offset/PSIZE);
    pagenum_t offset = header_page->free_offset;
    header_page->free_offset = free_page->next;
    
    page* root = buffer_read_node_page(table_id, offset/PSIZE);
    buffer_put_page(table_id, offset/PSIZE, false);
    
    root->parent_offset = 0;
    root->isleaf = INTERNAL;
    root->key_num = 0;
    root->another_offset = 0;
    
    root->child[0].key = key;
    root->child[0].page_offset = right_offset;
    root->another_offset = left_offset;
    root->key_num++;
    root->parent_offset = 0;
    left->parent_offset = offset;
    right->parent_offset = offset;
    header_page->root_offset = offset;
    
    buffer_put_page(table_id, header_page->root_offset/PSIZE, true);
}

int insert_into_parent(int table_id, pagenum_t left_offset, page* left, int64_t key, pagenum_t right_offset, page* right){
    
    int left_index;
    
    page_header* header_page = buffer_read_header_page(table_id);
    
    if(header_page->free_offset == 0)
        buffer_alloc_page(table_id);
    
    if(left->parent_offset == 0){
        insert_into_new_root(table_id, left_offset, left,right_offset, key, right, header_page);
        buffer_put_page(table_id, 0, true);
        return 0;
    }
    
    page* parent = buffer_read_node_page(table_id, left->parent_offset/PSIZE);
    pagenum_t parent_pnum = left->parent_offset/PSIZE;
    
    left_index = get_left_index(parent,left_offset);
    
    if(parent->key_num < iorder-1){
        insert_into_node(parent, left->parent_offset, right_offset, left_index, key);
        buffer_put_page(table_id, 0, false);
        buffer_put_page(table_id, left->parent_offset/PSIZE, true);
        return 0;
    }
    insert_into_node_after_splitting(table_id, parent,left->parent_offset,right_offset,left_index,key);
    buffer_put_page(table_id, 0, true);
    buffer_put_page(table_id, parent_pnum, true);
    return 0;
}

void insert_into_leaf(int table_id, pagenum_t offset, page* leaf, int64_t key, int64_t* value, pagenum_t num_col){
    int i;
    int insertion_point = 0;
    
    while(insertion_point < leaf->key_num && leaf->record[insertion_point].key < key)
        insertion_point++;
    
    for(i = leaf->key_num; i > insertion_point; i--){
        leaf->record[i].key = leaf->record[i-1].key;
        memcpy(leaf->record[i].value,leaf->record[i-1].value,sizeof(int64_t)*16);
    }
    leaf->record[insertion_point].key = key;
    memcpy(leaf->record[insertion_point].value,value,sizeof(int64_t)*num_col);
    leaf->key_num++;
}

void insert_into_leaf_after_splitting(int table_id, pagenum_t leaf_offset, page* leaf, int64_t key, int64_t* value){
    
    page_header* header_page = buffer_read_header_page(table_id);
    page_free* free_page = buffer_read_free_page(table_id, header_page->free_offset/PSIZE);
    pagenum_t offset = header_page->free_offset;
    header_page->free_offset = free_page->next;
    
    page* new_leaf = buffer_read_node_page(table_id, offset/PSIZE);
    buffer_put_page(table_id, offset/PSIZE, false);
    
    new_leaf->parent_offset = 0;
    new_leaf->isleaf = LEAF;
    new_leaf->key_num = 0;
    new_leaf->another_offset = 0;
    
    int insertion_index, split, i, j;
    int64_t new_key;
    int64_t temp_key[lorder];
    int64_t temp_val[lorder][16];
    
    
    insertion_index = 0;
    while(insertion_index < lorder-1 && leaf->record[insertion_index].key < key){
        insertion_index++;
    }
    
    for(i = 0, j = 0; i <leaf->key_num; i++,j++){
        if(j == insertion_index) j++;
        temp_key[j] = leaf->record[i].key;
        memcpy(temp_val[j],leaf->record[i].value,sizeof(int64_t)*(header_page->col_num-1));
    }
    
    temp_key[insertion_index] = key;
    memcpy(temp_val[insertion_index],value,sizeof(int64_t)*(header_page->col_num-1));
    
    leaf->key_num = 0;
    
    split = cut(lorder - 1);
    
    for(i = 0; i < split; i++){
        leaf->record[i].key = temp_key[i];
        memcpy(leaf->record[i].value,temp_val[i],sizeof(int64_t)*(header_page->col_num-1));
        leaf->key_num++;
    }
    
    for(i = split,j = 0; i < lorder; i++, j++){
        new_leaf->record[j].key = temp_key[i];
        memcpy(new_leaf->record[j].value,temp_val[i],sizeof(int64_t)*(header_page->col_num-1));
        new_leaf->key_num++;
    }
    
    buffer_put_page(table_id, 0, true);
    
    new_leaf->another_offset = leaf->another_offset;
    leaf->another_offset = offset;
    
    new_leaf->parent_offset = leaf->parent_offset;
    new_key = new_leaf->record[0].key;
    
    if(insert_into_parent(table_id, leaf_offset, leaf, new_key, offset, new_leaf) != 0)
        printf("error occur\n");
    buffer_put_page(table_id, offset/PSIZE, true);
}

//*************************************************************************************************************

void coalesce_page(int table_id, pagenum_t neighbor_offset, page* neighbor, page* n, int neighbor_index, int64_t k_prime){
    
    uint i, j;
    int neighbor_insertion_index, n_end;
    page* parent = buffer_read_node_page(table_id, neighbor->parent_offset/PSIZE);
    pagenum_t parent_pnum = neighbor->parent_offset/PSIZE;
    
    if(neighbor->key_num + n->key_num + 1 < iorder){
        page* temp;
        if(neighbor_index == -2){
            temp = n;
            n = neighbor;
            neighbor = temp;
        }
        
        neighbor_insertion_index = neighbor->key_num;
        neighbor->child[neighbor_insertion_index].key = k_prime;
        neighbor->key_num++;
        n_end = n->key_num;
        
        neighbor->child[neighbor_insertion_index].page_offset = n->another_offset;
        
        page* head = buffer_read_node_page(table_id, n->another_offset/PSIZE);
        head->parent_offset = neighbor_offset;
        buffer_put_page(table_id, n->another_offset/PSIZE, true);
        
        for(i = neighbor_insertion_index+1,j=0; j < n_end; i++,j++){
            neighbor->child[i].key = n->child[j].key;
            neighbor->child[i].page_offset = n->child[j].page_offset;
            neighbor->key_num++;
            
            head = buffer_read_node_page(table_id, n->child[j].page_offset/PSIZE);
            head->parent_offset = neighbor_offset;
            buffer_put_page(table_id, n->child[j].page_offset/PSIZE, true);
        }
        
        if(neighbor_index == -2)
            neighbor_index++;
        
        if(delete_entry(table_id, n->parent_offset,parent,k_prime,parent->child[neighbor_index+1].page_offset) != 0)
            printf("delete_entry after coalesce error\n");
    }else{
        
        int64_t temp_key[iorder];
        pagenum_t temp_offset[iorder];
        pagenum_t split = cut(iorder - 1);
        pagenum_t k_prime_index = neighbor_index == -2 ? 0 : neighbor_index+1;
        page* head;//free
        
        if (neighbor_index == -2){
            //first_offset = n->another_offset;
            
            temp_key[0] = k_prime;
            temp_offset[0] = neighbor->another_offset;
            
            for (i = 1, j = 0; j < iorder-1; i++,j++) {
                temp_key[i] = neighbor->child[j].key;
                temp_offset[i] = neighbor->child[j].page_offset;
                neighbor->key_num--;
            }
            
            for (i = 0; i < split; i++) {
                n->child[i].key = temp_key[i];
                n->child[i].page_offset = temp_offset[i];
                n->key_num++;
                head = buffer_read_node_page(table_id, n->child[i].page_offset/PSIZE);
                head->parent_offset = parent->another_offset;
                buffer_put_page(table_id, n->child[i].page_offset/PSIZE, true);
            }
            
            parent->child[k_prime_index].key = temp_key[i];
            neighbor->another_offset = temp_offset[i];
            
            for (++i,j = 0; i < iorder; i++,j++) {
                neighbor->child[j].key = temp_key[i];
                neighbor->child[j].page_offset = temp_offset[i];
                neighbor->key_num++;
            }
            
            
        }else{
            
            
            
            for (i = 0, j = 0; j < iorder-1; i++,j++) {
                temp_key[i] = neighbor->child[j].key;
                temp_offset[i] = neighbor->child[j].page_offset;
                neighbor->key_num--;
            }
            temp_key[i] = k_prime;
            temp_offset[i] = n->another_offset;
            
            for (i = 0; i < split; i++) {
                neighbor->child[i].key = temp_key[i];
                neighbor->child[i].page_offset = temp_offset[i];
                neighbor->key_num++;
            }
            
            parent->child[k_prime_index].key = temp_key[i];
            n->another_offset = temp_offset[i];
            
            head = buffer_read_node_page(table_id, n->another_offset/PSIZE);
            head->parent_offset = parent->child[neighbor_index+1].page_offset;
            buffer_put_page(table_id, n->another_offset/PSIZE, true);
            
            for (++i,j = 0; i < iorder; i++,j++) {
                n->child[j].key = temp_key[i];
                n->child[j].page_offset = temp_offset[i];
                n->key_num++;
                head = buffer_read_node_page(table_id, n->child[j].page_offset/PSIZE);
                head->parent_offset = parent->child[neighbor_index+1].page_offset;
                buffer_put_page(table_id, n->child[j].page_offset/PSIZE, true);
            }
        }
    }
    buffer_put_page(table_id, parent_pnum, true);
}

int adjust_root(int table_id, pagenum_t offset, page* some_page, page_header* header_page){
    
    if(some_page->isleaf == LEAF){
        if(some_page->key_num > 0)
            return 0;
        
        header_page->root_offset = 0;
        buffer_free_page(table_id, offset);
        return 0;
    }else{
        if(some_page->key_num > 0)
            return 0;
        header_page->root_offset = some_page->another_offset;
        
        page* head = buffer_read_node_page(table_id, some_page->another_offset/PSIZE);
        head->parent_offset = 0;
        buffer_put_page(table_id, some_page->another_offset/PSIZE, true);
        buffer_free_page(table_id, offset);
        return 0;
    }
    //printf("adjust_root error\n");
    return 0;
}

void remove_entry_from_page(int table_id, pagenum_t offset, page* some_page, int64_t key, pagenum_t entry_offset, pagenum_t num_col){
    
    int i = 0;
    if(some_page->isleaf == LEAF){
        
        while(some_page->record[i].key != key)
            i++;
        for(++i; i < some_page->key_num; i++){
            some_page->record[i-1].key = some_page->record[i].key;
            memcpy(some_page->record[i-1].value, some_page->record[i].value, sizeof(int64_t)*num_col);
        }
        some_page->key_num--;
        
    }else{
        pagenum_t leaf_offset = 0;
        int leaf_index = -5;
        while(some_page->child[i].key != key)
            i++;
        for(++i; i < some_page->key_num; i++)
            some_page->child[i-1].key = some_page->child[i].key;
        
        i = 0;
        if(some_page->another_offset == entry_offset)
            i = -1;
        else
            while(some_page->child[i].page_offset != entry_offset)
                i++;
        
        for(++i; i < some_page->key_num; i++){
            if(leaf_index == -5)
                leaf_index = i-2;
            if(i == 0){
                if(leaf_offset == 0)
                    leaf_offset = some_page->another_offset;
                some_page->another_offset = some_page->child[i].page_offset;
            }else{
                if(leaf_offset == 0)
                    leaf_offset = some_page->child[i-1].page_offset;
                some_page->child[i-1].page_offset = some_page->child[i].page_offset;
            }
        }
        some_page->key_num--;
        
        buffer_free_page(table_id,leaf_offset); //봐야됨
        
        page* head;
        if(leaf_index == -1){
            head = buffer_read_node_page(table_id, some_page->another_offset/PSIZE);
            if(head->isleaf == LEAF){
                head->another_offset = some_page->child[0].page_offset;
                buffer_put_page(table_id, some_page->another_offset/PSIZE, true);
            }
        }
        else if(leaf_index >= 0){
            head = buffer_read_node_page(table_id, some_page->child[leaf_index].page_offset/PSIZE);
            if(head->isleaf == LEAF){
                head->another_offset = some_page->child[leaf_index+1].page_offset;
                buffer_put_page(table_id, some_page->child[leaf_index].page_offset/PSIZE, true);
            }
        }
    }
}

int delete_entry(int table_id, pagenum_t offset, page* some_page, int64_t key, pagenum_t entry){
    
    int neighbor_index;
    int k_prime_index;
    pagenum_t k_prime;
    page_header* header_page = buffer_read_header_page(table_id);
    remove_entry_from_page(table_id, offset, some_page,key,entry,(header_page->col_num-1));
    
    if(header_page->root_offset == offset){
        if(adjust_root(table_id, offset,some_page,header_page) != 0)
            printf("adjust_err\n");
        buffer_put_page(table_id, 0, false);
        return 0;
    }
    
    buffer_put_page(table_id, 0, false);
    
    if(some_page->isleaf == LEAF){
        
        if(some_page->key_num >= 1)
            return 0;
        
        page* parent = buffer_read_node_page(table_id, some_page->parent_offset/PSIZE);
        pagenum_t parent_pnum = some_page->parent_offset/PSIZE;
        
        neighbor_index = get_neighbor_index(parent, offset);
        k_prime_index = neighbor_index == -2 ? 0 : neighbor_index+1;
        k_prime = parent->child[k_prime_index].key;
        
        if(neighbor_index == -2){
            pagenum_t offset = parent->another_offset;
            parent->another_offset = parent->child[0].page_offset;
            parent->child[0].page_offset = offset;
        }
        
        delete_entry(table_id,some_page->parent_offset,parent,k_prime,parent->child[k_prime_index].page_offset);
        
        buffer_put_page(table_id, parent_pnum, true);
        return 0;
        
    }else{
        
        if(some_page->key_num >= 1)
            return 0;
        
        pagenum_t neighbor_offset = 0;
        
        page* parent = buffer_read_node_page(table_id, some_page->parent_offset/PSIZE);
        pagenum_t parent_pnum = some_page->parent_offset/PSIZE;
        page* neighbor;
        pagenum_t neighbor_pnum;
        
        neighbor_index = get_neighbor_index(parent,offset);
        k_prime_index = neighbor_index == -2 ? 0 : neighbor_index+1;
        k_prime = parent->child[k_prime_index].key;
        if(neighbor_index == -2){
            neighbor = buffer_read_node_page(table_id, parent->child[0].page_offset/PSIZE);
            neighbor_offset = offset;
            neighbor_pnum = parent->child[0].page_offset/PSIZE;
        }else if(neighbor_index == -1){
            neighbor = buffer_read_node_page(table_id, parent->another_offset/PSIZE);
            neighbor_offset = parent->another_offset;
            neighbor_pnum = parent->another_offset/PSIZE;
        }else{
            neighbor = buffer_read_node_page(table_id, parent->child[neighbor_index].page_offset/PSIZE);
            neighbor_offset = parent->child[neighbor_index].page_offset;
            neighbor_pnum = parent->child[neighbor_index].page_offset/PSIZE;
        }
        
        coalesce_page(table_id, neighbor_offset,neighbor,some_page,neighbor_index,k_prime);
        
        buffer_put_page(table_id, parent_pnum, true);
        buffer_put_page(table_id, neighbor_pnum, true);
        return 0;
    }
    return -1;
}

//*********************************************************************************************************


int insert(int table_id, int64_t key, int64_t* value){
    
    page_header* header_page = buffer_read_header_page(table_id);
    leaf r = find_record(table_id, key);
    pagenum_t offset = 0;
    if(r.key != 0){
        buffer_put_page(table_id, 0, true);
        printf("key is already in bpt\n");
        return -1;
    }
    if(header_page->root_offset == 0){
        make_new_tree(table_id, key,value,header_page);
        buffer_put_page(table_id, 0, true);
        return 0;
    }
    
    
    offset = find_leaf_offset(table_id, key);
    page* leaf = buffer_read_node_page(table_id, offset/PSIZE);
    
    if(header_page->free_offset == 0)
        buffer_alloc_page(table_id);
    
    if(leaf->key_num < lorder-1){
        insert_into_leaf(table_id, offset,leaf,key,value,(header_page->col_num-1));
        buffer_put_page(table_id, 0, true);
        buffer_put_page(table_id, offset/PSIZE, true);
        return 0;
    }
    
    buffer_put_page(table_id, 0, true);
    insert_into_leaf_after_splitting(table_id, offset,leaf,key,value);
    buffer_put_page(table_id, offset/PSIZE, true);
    return 0;
}

int erase(int table_id, int64_t key){
    
    pagenum_t offset = find_leaf_offset(table_id,key);
    page* leaf_page = buffer_read_node_page(table_id, offset/PSIZE);
    leaf r = find_record(table_id,key);
    if(r.key == key && leaf_page->key_num != 0){
        if(delete_entry(table_id, offset,leaf_page,key,0) != 0){
            printf("delete_entry error\n");
            return -1;
        }
        buffer_put_page(table_id, offset/PSIZE, true);
    }else{
        printf("key is not bpt\n");
        buffer_put_page(table_id, offset/PSIZE, true);
    }
    
    return 0;
}






//*********************************************************************************************************










page* trx_buffer_read_node_page(int table_id, pagenum_t pagenum, int tid, char mode){
    
    pthread_mutex_lock(&buf_table.buffer_entire_mutex);
    
    buffer_manager *buffer = buf_table.buffer;
    int buffer_num = request_page(table_id, pagenum, nodep);
    
    if(buffer[buffer_num].whatp == freep){
        if(buffer[buffer_num].is_dirty == true){
            file_write_free_page(table_id, (pagenum_t)pagenum*PSIZE, buffer[buffer_num].free_node);
            buffer[buffer_num].is_dirty = false;
        }
        
        buffer[buffer_num].table_id = table_id;
        buffer[buffer_num].whatp = nodep;
        
        return buffer[buffer_num].another_node;
        
    }else if(buffer[buffer_num].table_id == -1){
        buffer[buffer_num].table_id = table_id;
        buffer[buffer_num].another_node = (page*)malloc(PSIZE);
        file_read_node_page(table_id, pagenum*PSIZE, buffer[buffer_num].another_node);
    }
    
    
    if(buffer[buffer_num].another_node->isleaf == LEAF)
        pthread_mutex_lock(&buffer[buffer_num].buffer_each_mutex);
    
    
    pthread_mutex_unlock(&buf_table.buffer_entire_mutex);
    
    if(buffer[buffer_num].another_node->isleaf == LEAF){
        
        lock_t* lock_obj = (lock_t*)malloc(sizeof(lock_t));
        
        lock_obj->trx = &trx_list[tid];
        lock_obj->table_id = buffer[buffer_num].table_id;
        lock_obj->cond = NULL;
        lock_obj->cond_mutex = NULL;
        lock_obj->buffer_index = buffer_num;
        
        if(mode == 'S')
            lock_obj->mode = SHARED;
        else
            lock_obj->mode = EXCLUSIVE;
        
        pthread_mutex_lock(&lock_table_mutex);
        if(add_lock_table(tid, mode, lock_obj) == 0){
            abort_trx(tid);
            return NULL;
        }
        
        
    }
    
    return buffer[buffer_num].another_node;
}

page_header* trx_buffer_read_header_page(int table_id, int tid){
    
    pthread_mutex_lock(&buf_table.buffer_entire_mutex);
    
    buffer_manager *buffer = buf_table.buffer;
    int buffer_num = request_page(table_id, 0, headerp);
    if(buffer[buffer_num].table_id == -1){
        buffer[buffer_num].table_id = table_id;
        buffer[buffer_num].header_node = (page_header*)malloc(PSIZE);
        file_read_header_page(table_id, buffer[buffer_num].header_node);
    }
    
    pthread_mutex_unlock(&buf_table.buffer_entire_mutex);
    return buffer[buffer_num].header_node;
}


pagenum_t trx_find_leaf_offset(int table_id, int64_t key, int tid){
    
    page_header* header_page = trx_buffer_read_header_page(table_id, tid);
    
    pagenum_t n = header_page->root_offset; //루트 오프셋
    
    if(n == 0){
        buffer_put_page(table_id, 0, false);
        return n;
    }
    
    page* root = trx_buffer_read_node_page(table_id, n/PSIZE, tid, 'S');
    if(root->isleaf == LEAF){
        buffer_put_page(table_id, n/PSIZE, false);
        buffer_put_page(table_id, 0, false);
        return n;
    }
    int i = 0;
    
    while(root->isleaf != LEAF){
        i = 0;
        while(i < root->key_num){
            if(key >= root->child[i].key)
                i++;
            else{
                i--;
                break;
            }
        }
        if(i == root->key_num)
            i--;
        
        if(i == -1){
            buffer_put_page(table_id, n/PSIZE, false);
            n = root->another_offset;
            root = trx_buffer_read_node_page(table_id,n/PSIZE, tid, 'S');
        }else{
            buffer_put_page(table_id, n/PSIZE, false);
            n = root->child[i].page_offset;
            root = trx_buffer_read_node_page(table_id, n/PSIZE, tid, 'S');
        }
    }
    
    buffer_put_page(table_id, n/PSIZE, false);
    buffer_put_page(table_id, 0, false);
    return n;
}

leaf trx_find_record(int table_id, int64_t key, int tid){
    
    page_header* header_page = trx_buffer_read_header_page(table_id, tid);
    
    struct leaf r;
    r.key = 0;
    r.value[0] = 0;
    
    if(header_page->root_offset == 0){
        buffer_put_page(table_id, 0, false);
        return r;
    }

    int i = 0;
    pagenum_t leaf_pnum = find_leaf_offset(table_id, key)/PSIZE;
    page* leaf = trx_buffer_read_node_page(table_id, leaf_pnum, tid, 'S');
    
    for(i = 0; i < leaf->key_num; i++)
        if(leaf->record[i].key == key)
            break;
    
    if(i == leaf->key_num){
        buffer_put_page(table_id, leaf_pnum, false);
        buffer_put_page(table_id, 0, false);
        return r;
        
    }else{
        r.key = leaf->record[i].key;
        memcpy(r.value, leaf->record[i].value, (header_page->col_num-1)*sizeof(int64_t));
        buffer_put_page(table_id, leaf_pnum, false);
        buffer_put_page(table_id, 0, false);
        return r;
    }
}


int64_t* find(int table_id, int64_t key, int tid, int* result){
    
    leaf r = trx_find_record(table_id, key, tid);
    trx_list[tid].trx_locks[trx_list[tid].trx_locks.size()-1]->mode = DONE;
    if(r.key == key){
        
        int64_t* A = (int64_t*)malloc(sizeof(int64_t)*15);
        memcpy(A, r.value, sizeof(int64_t)*15);
        *result = 1;
        return A;
    }else{
        printf("that key is not here\n");
        *result = 0;
        return 0;
    }
}

int update(int table_id, int64_t key, int64_t *values, int tid, int* result){
    int key_index = 0;
    pagenum_t offset = trx_find_leaf_offset(table_id,key,tid);
    
    page* leaf_page = trx_buffer_read_node_page(table_id, offset/PSIZE, tid, 'X');
    
    trx_list[tid].trx_locks[trx_list[tid].trx_locks.size()-1]->mode = DONE;
    buf_table.buffer[trx_list[tid].trx_locks[trx_list[tid].trx_locks.size()-1]->buffer_index].no_steal = true;
    
    for(key_index = 0; key_index < leaf_page->key_num; key_index++)
        if(leaf_page->record[key_index].key == key)
            break;
    
    
    if(key_index != leaf_page->key_num && leaf_page->key_num != 0){
        
            memcpy(leaf_page->record[key_index].value, values, sizeof(leaf_page->record[key_index].value));
            buffer_put_page(table_id, offset/PSIZE, true);
            *result = 1;
            return 1;
        
       //     printf("Value col_num is different\n");
         //   *result = 0;
           // return 0;
        
    }else{
        printf("key is not in bpt\n");
        buffer_put_page(table_id, offset/PSIZE, false);
        *result = 0;
        return 0;
    }
}


int begin_tx(){
    
    pthread_mutex_lock(&trx_begin_mutex);
    trx_table new_trx;
    new_trx.tid = ++tid_count;
    new_trx.wait_lock = NULL;
    new_trx.state = IDLE;
    trx_list.push_back(new_trx);
    pthread_mutex_unlock(&trx_begin_mutex);
    
    return new_trx.tid;
}

int abort_trx(int tid){
    
    
    int i = 0;
    
    pthread_mutex_lock(&lock_table_mutex);
    
    while (trx_list[tid].trx_locks.size() != 0) {
        
        int buf_index = trx_list[tid].trx_locks[i]->buffer_index;
        int table_id = buf_table.buffer[buf_index].table_id;
        
        if(trx_list[tid].trx_locks[i]->cond != NULL){
            if(trx_list[tid].trx_locks[i]->mode == SHARED){
                if(trx_list[tid].trx_locks[i]->my_index == 0){
                    pthread_cond_signal(trx_list[tid].trx_locks[i]->cond);
                }else{
                    if(lock_hash_t[buf_index].lock_obj_list[table_id][trx_list[tid].trx_locks[i]->my_index - 1]->mode == SHARED){
                        lock_hash_t[buf_index].lock_obj_list[table_id][trx_list[tid].trx_locks[i]->my_index - 1]->cond = trx_list[tid].trx_locks[i]->cond;
                        lock_hash_t[buf_index].lock_obj_list[table_id][trx_list[tid].trx_locks[i]->my_index - 1]->cond_mutex = trx_list[tid].trx_locks[i]->cond_mutex;
                    }else{
                        pthread_cond_signal(trx_list[tid].trx_locks[i]->cond);
                    }
                }
            }else if(trx_list[tid].trx_locks[i]->mode == EXCLUSIVE){
                pthread_cond_signal(trx_list[tid].trx_locks[i]->cond);
            }
        }
        
        if(trx_list[tid].trx_locks[i] == lock_hash_t[buf_index].last_X_mode[table_id])
            lock_hash_t[buf_index].last_X_mode[table_id] = NULL;
        
        buf_table.buffer[trx_list[tid].trx_locks[i]->buffer_index].is_dirty = false;
        buf_table.buffer[buf_index].no_steal = false;
        lock_hash_t[buf_index].lock_obj_list[table_id][trx_list[tid].trx_locks[i]->my_index]->mode = COMMIT;
        
        i++;
    }
    
    pthread_mutex_unlock(&lock_table_mutex);
    return tid;
}


int end_tx(int tid){
    
    int i = 0;
    
    pthread_mutex_lock(&lock_table_mutex);
    
    while (trx_list[tid].trx_locks.size() != 0) {
        
        int buf_index = trx_list[tid].trx_locks[i]->buffer_index;
        int table_id = buf_table.buffer[buf_index].table_id;
        
        if(trx_list[tid].trx_locks[i]->cond != NULL){
            if(trx_list[tid].trx_locks[i]->mode == SHARED){
                if(trx_list[tid].trx_locks[i]->my_index == 0){
                    pthread_cond_signal(trx_list[tid].trx_locks[i]->cond);
                }else{
                    if(lock_hash_t[buf_index].lock_obj_list[table_id][trx_list[tid].trx_locks[i]->my_index - 1]->mode == SHARED){
                        lock_hash_t[buf_index].lock_obj_list[table_id][trx_list[tid].trx_locks[i]->my_index - 1]->cond = trx_list[tid].trx_locks[i]->cond;
                        lock_hash_t[buf_index].lock_obj_list[table_id][trx_list[tid].trx_locks[i]->my_index - 1]->cond_mutex = trx_list[tid].trx_locks[i]->cond_mutex;
                    }else{
                        pthread_cond_signal(trx_list[tid].trx_locks[i]->cond);
                    }
                }
            }else if(trx_list[tid].trx_locks[i]->mode == EXCLUSIVE){
                pthread_cond_signal(trx_list[tid].trx_locks[i]->cond);
            }
        }
        
        if(trx_list[tid].trx_locks[i] == lock_hash_t[buf_index].last_X_mode[table_id])
            lock_hash_t[buf_index].last_X_mode[table_id] = NULL;
        
        buf_table.buffer[buf_index].no_steal = false;
        lock_hash_t[buf_index].lock_obj_list[table_id][trx_list[tid].trx_locks[i]->my_index]->mode = COMMIT;
        
        i++;
    }
    
    pthread_mutex_unlock(&lock_table_mutex);
    return tid;
}


int add_lock_table(int tid, char mode, lock_t* lock_obj){
    
    int buf_index = lock_obj->buffer_index;
    int table_id = buf_table.buffer[buf_index].table_id;
    int64_t index = lock_hash_t[buf_index].lock_obj_list[table_id].size();
    
    lock_obj->my_index = index;
    
    if(mode == 'S'){
        lock_t* last_X_obj = lock_hash_t[buf_index].last_X_mode[table_id];
        
        if(last_X_obj == NULL){
            
            trx_list[tid].state = RUNNING;
            trx_list[tid].wait_lock = NULL;
            trx_list[tid].trx_locks.push_back(lock_obj);
            
            lock_hash_t[buf_index].lock_obj_list[table_id].push_back(lock_obj);
            pthread_mutex_unlock(&buf_table.buffer[buf_index].buffer_each_mutex);
            pthread_mutex_unlock(&lock_table_mutex);
            
        }else{
            
            if(dead_lock_check(tid, last_X_obj) == 0)
                return 0;
            
            trx_list[tid].state = WAITING;
            trx_list[tid].wait_lock = last_X_obj;
            trx_list[tid].trx_locks.push_back(lock_obj);
            
            lock_hash_t[buf_index].lock_obj_list[table_id].push_back(lock_obj);
            
            if(last_X_obj->cond == NULL){
                pthread_cond_t X_cond = PTHREAD_COND_INITIALIZER;
                pthread_mutex_t X_mutex = PTHREAD_COND_INITIALIZER;
                
                last_X_obj->cond = &X_cond;
                last_X_obj->cond_mutex = &X_mutex;
            }
            
            pthread_mutex_unlock(&buf_table.buffer[buf_index].buffer_each_mutex);
            pthread_mutex_unlock(&lock_table_mutex);
            pthread_cond_wait(last_X_obj->cond, last_X_obj->cond_mutex);
            
            trx_list[tid].state = RUNNING;
            trx_list[tid].wait_lock = NULL;
            
        }
        
    }
    else if(mode == 'X'){
    
        
        if(lock_hash_t[buf_index].lock_obj_list[table_id].size() == 0){
            
            trx_list[tid].state = RUNNING;
            trx_list[tid].wait_lock = NULL;
            trx_list[tid].trx_locks.push_back(lock_obj);
            
            lock_hash_t[buf_index].lock_obj_list[table_id].push_back(lock_obj);
            lock_hash_t[buf_index].last_X_mode[table_id] = lock_obj;
            
            pthread_mutex_unlock(&buf_table.buffer[buf_index].buffer_each_mutex);
            pthread_mutex_unlock(&lock_table_mutex);
        }else if(lock_hash_t[buf_index].lock_obj_list[table_id][lock_hash_t[buf_index].lock_obj_list[table_id].size()-1]->mode == COMMIT){
            trx_list[tid].state = RUNNING;
            trx_list[tid].wait_lock = NULL;
            trx_list[tid].trx_locks.push_back(lock_obj);
            
            lock_hash_t[buf_index].lock_obj_list[table_id].push_back(lock_obj);
            lock_hash_t[buf_index].last_X_mode[table_id] = lock_obj;
            
            pthread_mutex_unlock(&buf_table.buffer[buf_index].buffer_each_mutex);
            pthread_mutex_unlock(&lock_table_mutex);
        }
        else{
            
            if(dead_lock_check(tid, lock_hash_t[buf_index].lock_obj_list[table_id][lock_hash_t[buf_index].lock_obj_list[table_id].size()-1]) == 0)
                return 0;
            
            trx_list[tid].state = WAITING;
            trx_list[tid].wait_lock = lock_hash_t[buf_index].lock_obj_list[table_id][lock_hash_t[buf_index].lock_obj_list[table_id].size()-1];
            trx_list[tid].trx_locks.push_back(lock_obj);
            
            lock_hash_t[buf_index].lock_obj_list[table_id].push_back(lock_obj);
            lock_hash_t[buf_index].last_X_mode[table_id] = lock_obj;
            
            lock_t* last_S_obj = trx_list[tid].wait_lock;
            
            if(last_S_obj->cond == NULL){
                
                pthread_cond_t S_cond = PTHREAD_COND_INITIALIZER;
                pthread_mutex_t S_mutex = PTHREAD_COND_INITIALIZER;
                
                last_S_obj->cond = &S_cond;
                last_S_obj->cond_mutex = &S_mutex;
            }
            
            pthread_mutex_unlock(&buf_table.buffer[buf_index].buffer_each_mutex);
            pthread_mutex_unlock(&lock_table_mutex);
            pthread_cond_wait(last_S_obj->cond, last_S_obj->cond_mutex);
            
            trx_list[tid].state = RUNNING;
            trx_list[tid].wait_lock = NULL;
        }
    }
    
    return 1;
}

int dead_lock_check(int tid, lock_t* wait_lock){
    
    int trx_id = wait_lock->trx->tid;

    while(trx_list[trx_id].state == WAITING){
        trx_id = trx_list[trx_id].wait_lock->trx->tid;
        if(tid == trx_id){
            printf("dead lock\n");
            return 0;
        }
    }
    return 1;
}
