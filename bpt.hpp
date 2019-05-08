//
//  Header.h
//  woopt
//
//  Created by mac on 2018. 10. 26..
//  Copyright © 2018년 mac. All rights reserved.
//
#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <stdbool.h>
#include <vector>
#include <pthread.h>
#include <list>


#ifndef Header_h
#define Header_h

#define Tid_MAX 10
#define headerp 1
#define nodep 2
#define freep 3

#define LEAF 1
#define INTERNAL 0
#define FREE 2
#define PSIZE 4096
#define get_offset(pnum) (pnum * PSIZE)
#define get_pnum(offset) (offset / PSIZE)
#define lorder 32
#define iorder 249

using std:: vector;

typedef uint64_t pagenum_t;

typedef struct page_header{
    pagenum_t free_offset;
    pagenum_t root_offset;
    pagenum_t page_num;
    pagenum_t col_num;
    char reserved[PSIZE - 32];
}page_header;

typedef struct leaf{
    int64_t key;
    int64_t value[15];
}leaf;

typedef struct internal{
    int64_t key;
    pagenum_t page_offset;
}internal;

typedef struct page_free{
    pagenum_t next;
    char reserved[PSIZE - 8];
}page_free;

typedef struct page{
    pagenum_t parent_offset;
    int isleaf;
    int key_num;
    char reserved[104];
    pagenum_t another_offset;
    union{
        struct leaf record[lorder-1]; //128 32
        struct internal child[iorder-1]; // 128
    };
}page;

typedef struct buffer_manager{
    union{
        struct page_header *header_node;
        struct page_free *free_node;
        struct page *another_node;
    };
    int table_id;
    pagenum_t page_num;
    bool is_dirty;
    int is_pinned;
    int next;
    int prev;
    int whatp;
    bool no_steal;
    pthread_mutex_t buffer_each_mutex;
}buffer_manager;

typedef struct buffer_table{
    pthread_mutex_t buffer_entire_mutex;
    buffer_manager *buffer;
}buffer_table;

//**************************************************************************************************************


page_header* buffer_read_header_page(int table_id);
page_header* buffer_read_new_header(int table_id);
page_free* buffer_read_free_page(int table_id, pagenum_t pagenum);
page_free* buffer_read_new_free(int table_id, pagenum_t pagenum);
page* buffer_read_node_page(int table_id, pagenum_t pagenum);
void buffer_put_page(int table_id, pagenum_t pagenum, bool dirty);
void buffer_alloc_page(int table_id);
void buffer_free_page(int table_id, pagenum_t offset);
int delete_buffer(int whatp, int end);
int request_page(int table_id, pagenum_t pagenum, int whatp);


int init_db(int num_buf);
int erase(int table_id, int64_t key);
int insert(int table_id, int64_t key, int64_t* value);
int open_table(char* pathname, int num_column);
int close_table(int table_id);
int shutdown_db(void);

//**************************************************************************************************************

void file_read_header_page(int table_id, page_header* dest);
void file_write_header_page(int table_id, page_header* src);
void file_read_free_page(int table_id, pagenum_t offset, page_free* dest);
void file_write_free_page(int table_id, pagenum_t offset, page_free* src);
void file_write_node_page(int table_id, pagenum_t offset, page* src);
void file_read_node_page(int table_id, pagenum_t offset, page* dest);
int cut(int length);
int get_neighbor_index(page* internal, pagenum_t offset);
int get_left_index(page* parent, pagenum_t left_offset);
pagenum_t find_leaf_offset(int table_id, int64_t key);
leaf find_record(int table_id, int64_t key);
void make_new_tree(int table_id, int64_t key, int64_t* value, page_header* header_page);
void insert_into_node(page* parent, pagenum_t parent_offset, pagenum_t right_offset, int left_index, int64_t key);
void insert_into_node_after_splitting(int table_id, page* parent, pagenum_t parent_offset,pagenum_t right_offset, int left_index, int64_t key);
void insert_into_new_root(int table_id, pagenum_t left_offset, page* left,pagenum_t right_offset, int64_t key, page* right, page_header* header_page);
int insert_into_parent(int table_id, pagenum_t left_offset, page* left, int64_t key, pagenum_t right_offset, page* right);
void insert_into_leaf(int table_id, pagenum_t offset, page* leaf, int64_t key, int64_t* value, pagenum_t num_col);
void insert_into_leaf_after_splitting(int table_id, pagenum_t leaf_offset, page* leaf, int64_t key, int64_t* value);
void coalesce_page(int table_id, pagenum_t neighbor_offset, page* neighbor, page* n, int neighbor_index, int64_t k_prime);
int adjust_root(int table_id, pagenum_t offset, page* some_page, page_header* header_page);
void remove_entry_from_page(int table_id, pagenum_t offset, page* some_page, int64_t key, pagenum_t entry_offset,pagenum_t num_col);
int delete_entry(int table_id, pagenum_t offset, page* some_page, int64_t key, pagenum_t entry);

int tid(void);

//************************************************************************************

typedef struct join_operater{
    int left_tid;
    int left_col;
    int right_tid;
    int right_col;
}join_operater;

typedef struct table_read{
    vector< vector<int64_t> > table;
}table_read;

typedef struct join_table{
    join_operater op;
    table_read left_table;
    table_read right_table;
}join_table;

typedef struct hash_set{
    int64_t value;
    int row_num;
}hash_set;

vector<join_operater> parse(vector<join_operater> pt, const char *query);
join_operater change_join_op(join_operater pt);
vector< vector<hash_set> > make_hash_table(table_read table, int64_t code_size, pagenum_t col_p);
table_read tableread(int table_id);
int64_t hashcode(int64_t value, int64_t code_size);
void hash_join(vector< vector<hash_set> > hash_table, table_read table, int64_t code_size, int right_col);
void make_join(vector<join_operater> pt, pagenum_t index, int64_t position_list[]);
int64_t join(const char *query);
vector<join_operater> swap_join_op(vector<join_operater> pt, pagenum_t index);
vector<join_operater> query_optimizer(vector<join_operater> pt);
int64_t hashsize(int64_t code_size);
vector<join_operater> query_order(vector<join_operater> pt, int64_t position_list[]);





using std::list;

enum lock_mode{
    SHARED = 0,
    EXCLUSIVE,
    COMMIT,
    DONE
};

enum trx_state{
    IDLE = 0,
    RUNNING,
    WAITING
};

typedef struct trx_table trx_t;
typedef struct lock_table lock_t;

struct lock_table {
    int table_id;
    int64_t record_id; // or key
    enum lock_mode mode; // SHARED, EXCLUSIVE
    trx_t* trx; // backpointer to lock holder ... // up to your design
    int64_t my_index;
    pthread_mutex_t* cond_mutex;
    pthread_cond_t* cond;
    int buffer_index;
};

struct trx_table {
    int tid;
    enum trx_state state; // IDLE, RUNNING, WAITING
    vector<lock_t*> trx_locks; // list of holding locks
    lock_t* wait_lock; // lock object that trx is waiting ... // up to your design
    
};


typedef struct lock_hash_table {
    
    vector<lock_t*> lock_obj_list[11];
    lock_t* last_X_mode[11];
    
}lock_hash_table;


int dead_lock_check(int tid, lock_t* wait_lock);
int add_lock_table(int tid, char mode, lock_t* lock_obj);
int begin_tx();
int end_tx(int tid);
int update(int table_id, int64_t key, int64_t *values, int tid, int* result);
int64_t* find(int table_id, int64_t key, int tid, int* result);
int abort_trx(int tid);


#endif
