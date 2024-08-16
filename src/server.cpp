#include <iostream>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <cerrno>
#include <ctime>
#include <cmath>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <string>
#include <vector>

#include "hashtable.h" 
#include "common.h"
#include "zset.h"
#include "list.h"
#include "heap.h"
#include "threadpool.h"

static void msg(const char* msg) { std::cerr << msg << "\n"; }
static void die(const char* msg) {

    int err = errno;
    std::cerr << err << " " << msg << "\n";
    abort();
}

static uint64_t get_monotonic_sec(){

    timespec tv = { 0, 0 };
    clock_gettime(CLOCK_MONOTONIC, & tv);
    return uint64_t(tv.tv_sec) * 1000000 + tv.tv_nsec / 1000;
}

// set nonblocking
static void fd_set_nb(int fd){

    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno) {
        
        die("fcntl() error");
        return;
    }

    flags |= O_NONBLOCK;
    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno)
        die("fcntl() error");
}

const size_t max_msg = 4096;
const size_t max_args = 1024;
const uint64_t idle_timeout = 5 * 1000;

enum state  { REQ = 0, RES = 1, END = 2 };
enum res    { OK = 0, RES_ERR = 1, NX = 2 };
enum err    { UNKNOWN = 1, E2_BIG = 2, TYPE = 3, ARG = 4 };
enum        { T_STR = 0, T_ZSET = 1 };

class Conn {
public:
    int fd = -1;
    uint32_t state = 0;

    // reading buffer
    size_t rbuf_size = 0;
    uint8_t rbuf[4 + max_msg]; 
    
    // writing buffer
    size_t wbuf_size = 0;
    size_t wbuf_sent = 0;
    uint8_t wbuf[4 + max_msg];
    uint64_t idle_start = 0;

    // timer
    Dlist idle_list;

    static void put(std::vector<Conn*>&, class Conn*);
    static int32_t accept_new(int);
};

struct Entry {

    struct Hnode node;
    std::string key;
    std::string val;
    uint32_t type = 0;
    
    Zset* set = nullptr;
    size_t heap_idx = -1;
    
};
static struct {
    Hmap db;
    // map of all client connections
    std::vector<Conn*> fd2conn;
    // timers for idle connections
    Dlist idle_list;
    //timers for ttls
    std::vector<Heap_Item> heap;
    // thread pool
    Thread_Pool tp;

} g_data;

static void state_req(Conn* conn);
static void state_res(Conn* conn);
static bool cmd_is(const std::string& word, const char* cmd);


void Conn::put(std::vector<Conn*>& fd2conn, class Conn* conn){

    if (fd2conn.size() <= (size_t)conn->fd)
        fd2conn.resize(conn->fd + 1);

    fd2conn[conn->fd] = conn;
}
int32_t Conn::accept_new(int fd){

    struct sockaddr_in client_addr = {};
    socklen_t socklen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr*)& client_addr, &socklen);
    if (connfd < 0){

        msg("accept() error");
        return -1;
    }

    // set new conn fd to nonblocking mode
    fd_set_nb(connfd);
    class Conn* conn = new Conn;
    if (!conn){
        
        close(connfd);
        return -1;
    }

    conn->fd = connfd;
    conn->state = state::REQ;
    conn->rbuf_size = 0;
    conn->wbuf_size = 0;
    conn->wbuf_sent = 0;
    conn->idle_start = get_monotonic_sec();

    dlist_insert(&g_data.idle_list, &conn->idle_list);
    Conn::put(g_data.fd2conn, conn);
    
    return 0;
}


static int32_t parse_req(const uint8_t* data, size_t len, std::vector<std::string>& out){

    if (len < 4)
        return -1;

    uint32_t n = 0;
    memcpy(&n, &data[0], 4);
    if (n > max_args)
        return -1;

    size_t pos = 4;
    while(n--){

        if(pos + 4 > len)
            return -1;

        uint32_t sz = 0;
        memcpy(&sz, &data[pos], 4);
        if (pos + 4 + sz > len)
            return -1;

        out.push_back(std::string((char*)& data[pos+4], sz));
        pos += 4 + sz;
    }

    if (pos != len)
        return -1;
    
    return 0;
}


static bool entry_eq(Hnode* lhs, Hnode* rhs){

    struct Entry* le = container_of(lhs, struct Entry, node);
    struct Entry* re = container_of(rhs, struct Entry, node);
    return le->key == re->key;
}


static bool str2dbl(const std::string& s, double& out){

    char *endp = NULL;
    out = strtod(s.c_str(), &endp);

    return endp == s.c_str() + s.size() && !std::isnan(out);
}
static bool str2int(const std::string& s, int64_t& out){

    char* endp = nullptr;
    out = strtoll(s.c_str(), &endp, 10);
    
    return endp == s.c_str() + s.size();
}


// set or remove ttl
static void entry_set_ttl(Entry* ent, int64_t ttl){

    if (ttl < 0 && ent->heap_idx != (size_t)-1){

        // erase an item from the heap
        // by replacing it with the last item in the array.
        size_t pos = ent->heap_idx;
        g_data.heap[pos] = g_data.heap.back();
        g_data.heap.pop_back();
        if (pos < g_data.heap.size()) {
            heap_upd(g_data.heap.data(), pos, g_data.heap.size());
        }
        ent->heap_idx = -1;
    }
    else if (ttl >= 0){

        size_t pos = ent->heap_idx;

        if (pos == (size_t)-1) {

            // add an new item to the heap
            Heap_Item item;
            item.ref = &ent->heap_idx;
            g_data.heap.push_back(item);
            pos = g_data.heap.size() - 1;
        }
        g_data.heap[pos].val = get_monotonic_sec() + (uint64_t)ttl * 1000;
        heap_upd(g_data.heap.data(), pos, g_data.heap.size());
    }
}


static void entry_destroy(Entry* ent){

    switch(ent->type){

        case T_ZSET:
            zset_dispose(ent->set);
            delete ent->set;
            break;
    }

    delete ent;
}
static void entry_del_async(void* arg){
    entry_destroy((Entry*)arg);
}
static void entry_del(Entry* ent){

    entry_set_ttl(ent, -1);

    const size_t large_container_size = 10000;
    bool too_big = false;
    switch(ent->type){

    case T_ZSET:
        too_big = map_size(&ent->set->map) > large_container_size;
        break;
    }

    if (too_big)
        threadpool_queue(&g_data.tp, &entry_del_async, ent);
    else 
        entry_destroy(ent);
}


static void out_nil(std::string& out) {
    out.push_back(ser::NIL);
}
static void out_str(std::string& out, const char* s, size_t size){

    out.push_back(ser::STR);
    uint32_t len = (uint32_t)size;
    out.append((char*)&len, 4);
    out.append(s, len);
}
static void out_str(std::string& out, const std::string& val){
    return out_str(out, val.data(), val.size());
}
static void out_int(std::string& out, int64_t val){

    out.push_back(ser::INT);
    out.append((char*)&val, 8);
}
static void out_dbl(std::string& out, double val){

    out.push_back(ser::DBL);
    out.append((char*)&val, 8);
}
static void out_err(std::string& out, int32_t code, const std::string& msg){

    out.push_back(ser::SER_ERR);
    out.append((char*)& code, 4);
    uint32_t len = (uint32_t)msg.size();
    out.append((char*)&len, 4);
    out.append(msg);
}
static void out_arr(std::string& out, uint32_t n){

    out.push_back(ser::ARR);
    out.append((char*)& n, 4);
}


static void do_get(std::vector<std::string>& cmd, std::string& out){

    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());

    Hnode* node = map_find(&g_data.db, &key.node, &entry_eq);
    if (!node)
        return out_nil(out);

    Entry* ent = container_of(node, Entry, node);
    if (ent->type != T_STR)
        return out_err(out, err::TYPE, "expect string type");
    
    return out_str(out, ent->val);
}
static void do_set(std::vector<std::string>& cmd, std::string& out){
    
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());

    Hnode* node = map_find(&g_data.db, &key.node, &entry_eq);
    if (node){

        Entry* ent = container_of(node, Entry, node);

        if (ent->type != T_STR)
            return out_err(out, err::TYPE, "expect string type");
    }
    else {

        Entry* ent = new Entry();
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->val.swap(cmd[2]);
        map_insert(&g_data.db, &ent->node);
    }

    return out_nil(out);
}
static void do_del(std::vector<std::string>& cmd, std::string& out){

    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());

    Hnode* node = map_pop(&g_data.db, &key.node, &entry_eq);
    if (node)
        entry_del(container_of(node, Entry, node));

    return out_int(out, node ? 1 : 0);
}


static void* begin_arr(std::string& out){

    out.push_back(ser::ARR);
    out.append("\0\0\0\0", 4);
    
    return (void*)(out.size() - 4);
}
static void end_arr(std::string& out, void* ctx, uint32_t n){

    size_t pos = (size_t)ctx;
    assert(out[pos - 1] == ser::ARR);
    memcpy(&out[pos], &n, 4);
}


static void do_expire(std::vector<std::string>&cmd, std::string& out){

    int64_t ttl = 0;
    if (!str2int(cmd[2], ttl))
        return out_err(out, err::ARG, "expect int64");

    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());

    Hnode* node = map_find(&g_data.db, &key.node, &entry_eq);
    if (node){

        Entry* ent = container_of(node, Entry, node);
        entry_set_ttl(ent, ttl);
    }

    return out_int(out, node ? 1 : 0);
}
static void do_ttl(std::vector<std::string>& cmd, std::string& out){

    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    Hnode *node = map_find(&g_data.db, &key.node, &entry_eq);
    if (!node) 
        return out_int(out, -2);
    
    Entry *ent = container_of(node, Entry, node);
    if (ent->heap_idx == (size_t)-1) 
        return out_int(out, -1);
    
    uint64_t expire_at = g_data.heap[ent->heap_idx].val;
    uint64_t now_us = get_monotonic_sec();

    return out_int(out, expire_at > now_us ? (expire_at - now_us) / 1000 : 0);
}


static void h_scan(Htab* tab, void (*f)(Hnode*, void*), void* arg){

    if (tab->size == 0)
        return;

    for (size_t i = 0; i < tab->mask + 1; ++i){

        Hnode* node = tab->tab[i];
        while(node){

            f(node, arg);
            node = node->next;
        }
    }
}
static void cb_scan(Hnode* node, void* arg){

    std::string& out = *(std::string*)arg;
    out_str(out, container_of(node, Entry, node)->key);
}


static bool cmd_is(const std::string& word, const char* cmd){
    return 0 == strcasecmp(word.c_str(),cmd);
}


static bool expect_set(std::string& out, std::string& s, Entry** ent){

    Entry key;
    key.key.swap(s);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());

    Hnode* hnode = map_find(&g_data.db, &key.node, &entry_eq);
    if (!hnode){

        out_nil(out);
        return false;
    }

    *ent = container_of(hnode, Entry, node);
    if ((*ent)->type != T_ZSET){

        out_err(out, err::TYPE, "expect zset");
        return false;
    }

    return true;
}

static void do_zadd(std::vector<std::string>& cmd, std::string& out){

    double score = 0;
    if (!str2dbl(cmd[2], score))
        return out_err(out, err::ARG, "expect fp num");

    // look up or create zset
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());
    Hnode* hnode = map_find(&g_data.db, &key.node, &entry_eq);

    Entry* ent = nullptr;
    if (!hnode){

        ent = new Entry();
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->type = T_ZSET;
        ent->set = new Zset();
        
        map_insert(&g_data.db, &ent->node);
    }
    else {

        ent = container_of(hnode, Entry, node);
        if (ent->type != T_ZSET)
            return out_err(out, err::TYPE, "expect zset");
    }

    const std::string& name = cmd[3];
    bool added = zset_add(ent->set, name.data(), name.size(), score);
    
    return out_int(out, (int64_t)added);
}
static void do_zrem(std::vector<std::string>& cmd, std::string& out){

    Entry* ent = nullptr;
    if (!expect_set(out, cmd[1], &ent))
        return;

    const std::string& name = cmd[2];
    Znode* node = zset_pop(ent->set, name.data(), name.size());

    if (node)
        znode_del(node);

    return out_int(out, node ? 1 : 0);
}
static void do_zscore(std::vector<std::string>& cmd, std::string& out){

    Entry* ent = nullptr;
    if (!expect_set(out, cmd[1], &ent))
        return;

    const std::string& name = cmd[2];
    Znode* node = zset_find(ent->set, name.data(), name.size());
    return node ? out_dbl(out, node->score) : out_nil(out);
}
static void do_zquery(std::vector<std::string>& cmd, std::string& out){

    // parse args
    double score = 0;
    if (!str2dbl(cmd[2], score)) 
        return out_err(out, err::ARG, "expect fp number");

    const std::string &name = cmd[3];
    int64_t offset = 0;
    int64_t limit = 0;
    if (!str2int(cmd[4], offset)) 
        return out_err(out, err::ARG, "expect int");

    if (!str2int(cmd[5], limit)) 
        return out_err(out, err::ARG, "expect int");
    

    // get the zset
    Entry *ent = NULL;
    if (!expect_set(out, cmd[1], &ent)) {

        if (out[0] == ser::NIL) {

            out.clear();
            out_arr(out, 0);
        }
        return;
    }

    // look up the tuple
    if (limit <= 0) 
        return out_arr(out, 0);
    
    Znode *node = zset_query(ent->set, score, name.data(), name.size());
    node = znode_offset(node, offset);

    // output
    void *arr = begin_arr(out);
    uint32_t n = 0;
    while (node && (int64_t)n < limit) {

        out_str(out, node->name, node->len);
        out_dbl(out, node->score);
        node = znode_offset(node, +1);
        n += 2;
    }

    end_arr(out, arr, n);
}


static void do_keys(std::vector<std::string>& cmd, std::string& out){

    (void)cmd;
    out_arr(out, (uint32_t)map_size(&g_data.db));
    h_scan(&g_data.db.t1, &cb_scan, &out);
    h_scan(&g_data.db.t2, &cb_scan, &out);
}
static void do_req(std::vector<std::string>& cmd, std::string& out){
    
    if (cmd.size() == 1 && cmd_is(cmd[0], "keys"))
        do_keys(cmd, out);
    else if (cmd.size() == 2 && cmd_is(cmd[0], "get"))
        do_get(cmd, out);
    else if (cmd.size() == 3 && cmd_is(cmd[0], "set"))
        do_set(cmd, out);
    else if (cmd.size() == 2 && cmd_is(cmd[0], "del"))
        do_del(cmd, out);
    else if (cmd.size() == 4 && cmd_is(cmd[0], "zadd"))
        do_zadd(cmd, out);
    else if (cmd.size() == 3 && cmd_is(cmd[0], "zrem"))
        do_zrem(cmd, out);
    else if (cmd.size() == 3 && cmd_is(cmd[0], "zscore"))
        do_zscore(cmd, out);
    else if (cmd.size() == 6 && cmd_is(cmd[0], "zquery"))
        do_zquery(cmd, out);
    else 
        out_err(out, err::UNKNOWN, "UNKNOWN CMD");
}


static bool try_one_req(Conn* conn){

    // try to parse request from buffer
    // not enough data in buffer
    if (conn->rbuf_size < 4)
        return false;

    uint32_t len = 0;
    memcpy(&len, &conn->rbuf[0], 4);
    if (len > max_msg){

        msg("too long");
        conn->state = state::END;
        return false;
    }

    // not enough data in the buffer
    if (4 + len > conn->rbuf_size)
        return false;

    std::vector<std::string> cmd;
    if (0 != parse_req(&conn->rbuf[4], len, cmd)){

        msg("bad req");
        conn->state = state::END;
        return false;
    }

    std::string out;
    do_req(cmd, out);

    if (4 + out.size() > max_msg){

        out.clear();
        out_err(out, err::E2_BIG, "response is too big");
    }

    uint32_t wlen = (uint32_t)out.size();
    memcpy(&conn->wbuf[0], &wlen, 4);
    memcpy(&conn->wbuf[4], out.data(), out.size());
    conn->wbuf_size = 4 + wlen;

    // remove request from the buffer
    size_t rem = conn->rbuf_size - 4 - len;
    if (rem)
        memmove(conn->rbuf, & conn->rbuf[4 + len], rem);

    conn->rbuf_size = rem;

    //change state
    conn->state = state::RES;
    state_res(conn);

    return (conn->state == state::REQ);
}
static bool try_fill_buf(Conn* conn){

    // tyr fill buff
    assert(conn->rbuf_size < sizeof(conn->rbuf));
    ssize_t rv = 0;

    do {

        size_t cap = sizeof(conn->rbuf) - conn->rbuf_size;
        rv = read(conn->fd, &conn->rbuf[conn->rbuf_size], cap);
    }while (rv < 0 && errno == EAGAIN);

    if (rv < 0 && errno == EAGAIN)
        return false;

    if (rv < 0){

        msg("read() error");
        conn->state = state::END;
        return false;
    }

    if (rv == 0){

        if (conn->rbuf_size > 0)
            msg("unexpected EOF");
        else 
            msg("EOF");

        conn->state = state::REQ;
        return false;
    }

    conn->rbuf_size += (size_t)rv;
    assert(conn->rbuf_size <= sizeof(conn->rbuf));

    while(try_one_req(conn)) {}
    return (conn->state == state::REQ);
}   
static bool try_flush_buf(Conn* conn){

    ssize_t rv = 0;
    do {

        size_t rem = conn->wbuf_size - conn->wbuf_sent;
        rv = write(conn->fd, &conn->wbuf[conn->wbuf_sent], rem);
    }while (rv < 0 && errno == EINTR);

    if (rv < 0 && errno == EAGAIN)
        return false;

    if (rv < 0){

        msg("write() error");
        conn->state = state::END;
        return false;
    }

    conn->wbuf_sent += (size_t)rv;
    assert(conn->wbuf_sent <= conn->wbuf_size);
    if (conn->wbuf_sent == conn->wbuf_size){

        conn->state = state::REQ;
        conn->wbuf_sent = 0;
        conn->wbuf_size = 0;
        return false;
    }

    return true;
}


static void state_req(Conn* conn) {
    while (try_fill_buf(conn)) {}
}
static void state_res(Conn* conn){
    while(try_flush_buf(conn)) {}
}


static void conn_io(Conn* conn){

    if (conn->state == state::REQ)
        state_req(conn);
    else if (conn->state == state::RES)
        state_res(conn);
    else
        assert(0);
}

static uint64_t next_timer(){

    uint64_t now_us = get_monotonic_sec();
    uint64_t next_us = (uint64_t)-1;

    // idle timers
    if (!dlist_empty(&g_data.idle_list)) {

        Conn *next = container_of(g_data.idle_list.next, Conn, idle_list);
        next_us = next->idle_start + idle_timeout * 1000;
    }

    // ttl timers
    if (!g_data.heap.empty() && g_data.heap[0].val < next_us) 
        next_us = g_data.heap[0].val;

    if (next_us == (uint64_t)-1) 
        return 10000;   // no timer, the value doesn't matter

    if (next_us <= now_us)
        return 0;

    return (uint32_t)((next_us - now_us) / 1000);
}

static void conn_done(Conn* conn){

    g_data.fd2conn[conn->fd] = nullptr;
    (void)close(conn->fd);
    dlist_detach(&conn->idle_list);

    free(conn);
}

static bool hnode_same(Hnode* lhs, Hnode* rhs){
    return lhs == rhs;
}

static void process_timers(){

    uint64_t now_us = get_monotonic_sec() + 1000;

    // idle timers
    while (!dlist_empty(&g_data.idle_list)) {

        Conn *next = container_of(g_data.idle_list.next, Conn, idle_list);
        uint64_t next_us = next->idle_start + idle_timeout * 1000;
        if (next_us >= now_us + 1000)
            break;

        printf("removing idle connection: %d\n", next->fd);
        conn_done(next);
    }

    // ttl timers
    const size_t max_works = 2000;
    size_t nworks = 0;

    while(!g_data.heap.empty() && g_data.heap[0].val < now_us){

        Entry* ent = container_of(g_data.heap[0].ref, Entry, heap_idx);
        Hnode* node = map_pop(&g_data.db, &ent->node, &hnode_same);
        assert(node == &ent->node);

        entry_del(ent);

        if (nworks++ >= max_works)
            break;
    }
}

int main(){

    dlist_init(&g_data.idle_list);
    threadpool_init(&g_data.tp, 4);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        die("scoket()");

    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // bind
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);
    
    int rv = bind(fd, (const sockaddr*)& addr, sizeof(addr));
    if(rv) 
        die("bind()");

    // listen
    rv = listen(fd, SOMAXCONN);
    if (rv)
        die("listen()");

    // set listen fd to nonblock mode
    fd_set_nb(fd);

    std::vector<struct pollfd> poll_args;
    while(true){

        poll_args.clear();
        struct pollfd pfd = {fd, POLLIN, 0};
        poll_args.push_back(pfd);

        for (Conn* conn : g_data.fd2conn){

            if (!conn)
                continue;

            struct pollfd pfd = {};
            pfd.fd = conn->fd;
            pfd.events = (conn->state == state::REQ) ? POLLIN : POLLOUT;
            pfd.events = pfd.events | POLLERR;
            poll_args.push_back(pfd);
        }

        int timeout = (int)next_timer();
        int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), timeout);
        if (rv < 0)
            die("poll");

        for (size_t i = 1; i < poll_args.size(); ++i){

            if (poll_args[i].revents){

                Conn* conn = g_data.fd2conn[poll_args[i].fd];
                conn_io(conn);
                if (conn->state == state::END)
                    conn_done(conn);
                
            }
        }

        process_timers();

        if (poll_args[0].revents)
            (void)Conn::accept_new(fd);
    }

    close(fd);
    return 0;
}
