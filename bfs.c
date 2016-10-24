

#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <inttypes.h>
#include <time.h> 

void *zmalloc(size_t size) {
    void *ptr = malloc(size);

    if (!ptr) {
        printf("Out of memory\n");
        exit(-__LINE__);
    }

    memset(ptr, 0, size);
    return ptr;
}

struct raw_edge {
    u_int64_t   from;
    u_int64_t   to;
};

#define BASE_VERTEX_ALLOCATION  (8)
struct vertex {
    // data used for the base graph connections
    u_int64_t   id;
    u_int64_t edge_count;
    u_int64_t allocated_edges;
    struct vertex   **edges;

    // data used for BFS algorithm
    int64_t level;
    struct vertex *prev, *next;
};

// Adjust this up or down depending on the graph size of interest.
// It does not effect BFS speed, but it will effect ID -> vertex mapping speed
#define HASH_MAP_SIZE   (1000001)

struct vertex_id_map {
    u_int64_t id;
    struct vertex *vertex;
    struct vertex_id_map *next;
};

u_int64_t hash_vertex_id(u_int64_t id) {
    return id % HASH_MAP_SIZE;
}

struct vertex_id_map **create_vertex_id_hash() {
    struct vertex_id_map **vertex_id_map;

    vertex_id_map = (struct vertex_id_map **)zmalloc(sizeof(*vertex_id_map) * HASH_MAP_SIZE);
    return vertex_id_map;
}

struct vertex *create_vertex(u_int64_t id) {
    struct vertex *v = (struct vertex *)zmalloc(sizeof(*v));
    v->id = id;
    v->allocated_edges = BASE_VERTEX_ALLOCATION;
    v->edges = (struct vertex   **)zmalloc(sizeof(*v->edges) * BASE_VERTEX_ALLOCATION);

    return v;
}

void insert_vertex_to_id_hash(struct vertex_id_map **vertex_id_map, struct vertex *v) {
    struct vertex_id_map *vm = (struct vertex_id_map *)zmalloc(sizeof(*vm));
    vm->id = v->id;
    vm->vertex = v;
    u_int64_t hash = hash_vertex_id(v->id);
    vm->next = vertex_id_map[hash];
    vertex_id_map[hash] = vm;
}

struct vertex *locate_vertex_from_id(struct vertex_id_map **vertex_id_map, u_int64_t id) {
    struct vertex_id_map *vm = vertex_id_map[hash_vertex_id(id)];

    while (vm) {
        if (vm->id == id)
            return vm->vertex;
        vm = vm->next;
    }

    return NULL;
}

void _add_edge_to_vertex(struct vertex *from, struct vertex *to) {
    if (from->allocated_edges == from->edge_count) {
        struct vertex **vm = (struct vertex **)zmalloc(sizeof(*vm) * from->allocated_edges * 2);
        memcpy(vm, from->edges, sizeof(*vm) * from->allocated_edges);
        free(from->edges);
        from->edges = vm;
        from->allocated_edges = from->allocated_edges * 2;
    }
    from->edges[from->edge_count] = to;
    ++from->edge_count;
}

void add_edge_to_vertex(struct vertex *v1, struct vertex *v2) {
    _add_edge_to_vertex(v1, v2);
    _add_edge_to_vertex(v2, v1);
}

struct vertex_id_map **read_graph(char *filename) {
    FILE *fp = fopen(filename, "rb");

    if (!fp) {
        printf("Failed to open:");
        return NULL;
    }

    /*
    FILE *fw = fopen("graph.txt", "w");
    if (!fw) {
        printf("Failed to open:");
        return NULL;
    }
    */

    fseek(fp, 0, SEEK_END);
    u_int64_t   filesize = ftell(fp);

    u_int64_t edge_count = filesize / (2 * sizeof(u_int64_t));

    struct raw_edge *raw_edges = (struct raw_edge *)zmalloc(sizeof(*raw_edges) * edge_count);

    fseek(fp, 0, SEEK_SET);

    u_int64_t total_read = 0;

    char *base_ptr = (char *)raw_edges;
    while (true) {
        int read_size = fread(base_ptr, 1, filesize, fp);

        total_read += read_size;
        base_ptr += read_size;

        if (total_read == filesize)
            break;

        if (feof(fp)) {
            printf("Failed to read file\n");
            fclose(fp);
            free(raw_edges);
            return NULL;
        }
    }
    fclose(fp);


    struct vertex_id_map **graph = create_vertex_id_hash();

    u_int64_t edge_index;

    for (edge_index = 0; edge_index < edge_count; edge_index++) {
        struct vertex *from_v, *to_v;

        from_v = locate_vertex_from_id(graph, raw_edges[edge_index].from);
        if (!from_v) {
            from_v = create_vertex(raw_edges[edge_index].from);
            insert_vertex_to_id_hash(graph, from_v);
        }

        to_v = locate_vertex_from_id(graph, raw_edges[edge_index].to);
        if (!to_v) {
            to_v = create_vertex(raw_edges[edge_index].to);
            insert_vertex_to_id_hash(graph, to_v);
        }

        add_edge_to_vertex(from_v, to_v);
        //fprintf(fw, "%lld\t%lld\n", raw_edges[edge_index].from, raw_edges[edge_index].to);
    }

    //fclose(fw);
    return graph;
}

void for_all_vertices(struct vertex_id_map **map, void(*func)(struct vertex *, void *), void *arg) {
    u_int64_t i;
    struct vertex_id_map *vm;

    for (i = 0; i < HASH_MAP_SIZE; i++) {
        vm = map[i];
        while (vm) {
            func(vm->vertex, arg);
            vm = vm->next;
        }
    }
}

#define UNVISITED   (-1)
void _clear_bfs_state(struct vertex *v, void *arg) {
    v->level = UNVISITED;
    v->next = NULL;
    v->prev = NULL;
}

void clear_bfs_state(struct vertex_id_map **graph) {
    for_all_vertices(graph, _clear_bfs_state, NULL);
}

void bfs(struct vertex_id_map **graph, int root) {
    u_int64_t i;
    clear_bfs_state(graph);
    struct vertex *start = NULL, *end = NULL;

    struct vertex *v = locate_vertex_from_id(graph, root);

    if (!v)
        return;

    v->level = 0;
    start = v;
    end = v;

    while (start) {
        // dequeue
        v = start;
        start = start->next;
        if (v == end)
            end = NULL;
        v->prev = v->next = NULL;

        for (i = 0; i < v->edge_count; i++) {
            struct vertex *nv = v->edges[i];
            if (nv->level == UNVISITED || nv->level >(v->level + 1))
            {
                // Label
                nv->level = v->level + 1;
                // enqueue
                if (!nv->next && !nv->prev && start != nv && end != nv) {
                    nv->next = NULL;
                    nv->prev = end;
                    if (end)
                        end->next = nv;
                    end = nv;
                    if (!start)
                        start = nv;
                }
            }
        }
    }
}

#define NUM_THREADS  (10)
typedef struct {
    struct vertex_id_map **map;
    u_int64_t start;
    u_int64_t end;
    void(*func)(struct vertex *, void *);
    void *arg;
} hashMapIteratorWorkChunk;

void * parallel_for_all_vertices(void* arg) {
    u_int64_t i;
    struct vertex_id_map *vm;
    hashMapIteratorWorkChunk * workChunk = (hashMapIteratorWorkChunk  *)arg;

    for (i = workChunk->start; i < workChunk->end; i++) {
        vm = workChunk->map[i];
        while (vm) {
            workChunk->func(vm->vertex, workChunk->arg);
            vm = vm->next;
        }
    }

    return NULL;
}

void parallel_clear_bfs_state(struct vertex_id_map **graph) {
    int num_threads = NUM_THREADS;
    pthread_t   *threads;

    if (num_threads > HASH_MAP_SIZE)
    {
        num_threads = HASH_MAP_SIZE;
    }

    threads = (pthread_t *)malloc(sizeof(*threads) * num_threads);
    hashMapIteratorWorkChunk *workChunks = (hashMapIteratorWorkChunk *)malloc(sizeof(hashMapIteratorWorkChunk) * num_threads);

    u_int64_t i, start = 0;
    u_int64_t chunk = HASH_MAP_SIZE / ((u_int64_t)num_threads);
    for (i = 0;i < num_threads;i++)
    {
        workChunks[i].map = graph;
        workChunks[i].func = _clear_bfs_state;
        workChunks[i].arg = NULL;
        workChunks[i].start = start;

        if (start + chunk < HASH_MAP_SIZE)
        {
            workChunks[i].end = start + chunk;
        }
        else
        {
            workChunks[i].end = HASH_MAP_SIZE;
        }

        start = start + chunk;

        if (pthread_create(&threads[i], NULL, parallel_for_all_vertices, &workChunks[i]) != 0)
        {
            perror("Failed to create thread\n");
        }
    }

    // Join threads
    for (i = 0; i < num_threads; i++)
    {
        pthread_join(threads[i], NULL);
    }

    free(threads);
    free(workChunks);
}

typedef struct {
    struct vertex * levelStart;
    struct vertex * levelEnd;
    pthread_mutex_t * levelLock;
    u_int64_t unprocessedCount;
    u_int64_t level;
} levelQueue;

levelQueue * InitLevelQueue(u_int64_t level)
{
    levelQueue * newLevelQueue = (levelQueue*)zmalloc(sizeof(levelQueue));   
    newLevelQueue->level = level;
    return newLevelQueue;
}

void AddToLevelQueue(levelQueue * queue, struct vertex *v)
{
    if (v->level == UNVISITED)
    {
        pthread_mutex_lock(queue->levelLock);
        if (v->level == UNVISITED)
        {
            v->level = queue->level;

            if (queue->levelEnd != NULL)
            {
                queue->levelEnd->next = v;
                v->prev = queue->levelEnd;
                queue->levelEnd = v;
            }
            else
            {
                // first element
                queue->levelStart = queue->levelEnd = v;
            }

            queue->unprocessedCount++;
        }
        pthread_mutex_unlock(queue->levelLock);
    }
}

struct vertex * RemoveFromLevelQueue(levelQueue * queue)
{
    struct vertex * v = NULL;
    pthread_mutex_lock(queue->levelLock);
   
    if (queue->levelStart != NULL)
    {
        if (queue->levelStart == queue->levelEnd)
        {
            v = queue->levelStart;
            queue->levelStart = queue->levelEnd = NULL;
            queue->unprocessedCount = 0;
        }
        else
        {
            v = queue->levelStart;
            queue->levelStart = queue->levelStart->next;
            queue->levelStart->prev = NULL;
            queue->unprocessedCount--;
        }
    }

    pthread_mutex_unlock(queue->levelLock);
    return v;
}

typedef struct {
    levelQueue * currentLevel;
    levelQueue * nextLevel;
} bfsWork;

void * parallel_bfs_work(void* arg) 
{
    u_int64_t i;
    bfsWork * workChunk = (bfsWork  *)arg;

    struct vertex * v = RemoveFromLevelQueue(workChunk->currentLevel);

    while (v)
    {
        for (i = 0; i < v->edge_count; i++)
        {
            struct vertex *nv = v->edges[i];
            AddToLevelQueue(workChunk->nextLevel, nv);
        }

        v = RemoveFromLevelQueue(workChunk->currentLevel);
    }

    return NULL;
}

void parallelbfs(struct vertex_id_map **graph, int root) {
    u_int64_t i;
    parallel_clear_bfs_state(graph);
    struct vertex *start = NULL, *end = NULL;

    struct vertex *v = locate_vertex_from_id(graph, root);

    if (!v)
        return;

    levelQueue * currentLevel = InitLevelQueue(0);
    pthread_mutex_t lock1;
    pthread_mutex_init(&lock1, NULL);
    currentLevel->levelLock = &lock1;
    AddToLevelQueue(currentLevel, v);

    levelQueue * nextLevel = InitLevelQueue(currentLevel->level + 1);
    pthread_mutex_t lock2;
    pthread_mutex_init(&lock2, NULL);
    nextLevel->levelLock = &lock2;

    do
    {
        int num_threads = NUM_THREADS;
        pthread_t   *threads;

        if (num_threads > currentLevel->unprocessedCount)
        {
            num_threads = currentLevel->unprocessedCount;
        }

        threads = (pthread_t *)malloc(sizeof(*threads) * num_threads);
        bfsWork *workChunks = (bfsWork *)malloc(sizeof(bfsWork) * num_threads);

        u_int64_t i, start = 0;
        u_int64_t chunk = HASH_MAP_SIZE / ((u_int64_t)num_threads);
        for (i = 0;i < num_threads;i++)
        {
            workChunks[i].currentLevel = currentLevel;
            workChunks[i].nextLevel = nextLevel;

            if (pthread_create(&threads[i], NULL, parallel_bfs_work, &workChunks[i]) != 0)
            {
                perror("Failed to create thread\n");
            }
        }

        // Join threads
        for (i = 0; i < num_threads; i++)
        {
            pthread_join(threads[i], NULL);
        }

        free(threads);
        free(workChunks);

        pthread_mutex_t * currentLevellock = currentLevel->levelLock;
        free(currentLevel);
        currentLevel = nextLevel;
        nextLevel = InitLevelQueue(nextLevel->level + 1);
        nextLevel->levelLock = currentLevellock;

    } while (currentLevel->unprocessedCount > 0);

    free(currentLevel);
    free(nextLevel);
}

struct _gather_statistics_s {
    u_int64_t *vertices;
    u_int64_t *reached_vertices;
    u_int64_t *edges;
    int64_t *max_level;
};

void _grather_statistics(struct vertex *v, void *v_arg) {
    struct _gather_statistics_s *arg = (struct _gather_statistics_s *)v_arg;

    ++(*arg->vertices);
    *(arg->edges) += v->edge_count;
    if (v->level != UNVISITED)
        ++(*arg->reached_vertices);
    if (v->level > *(arg->max_level))
        (*arg->max_level) = v->level;
}

void grather_statistics(struct vertex_id_map **graph,
    u_int64_t *vertices,
    u_int64_t *reached_vertices,
    u_int64_t *edges,
    int64_t *max_level) {
    struct _gather_statistics_s arg;

    *vertices = 0;
    *reached_vertices = 0;
    *edges = 0;
    *max_level = UNVISITED;
    arg.vertices = vertices;
    arg.reached_vertices = reached_vertices;
    arg.edges = edges;
    arg.max_level = max_level;

    for_all_vertices(graph, _grather_statistics, &arg);
}

u_int64_t timediff(clock_t t1, clock_t t2) {
    u_int64_t elapsed;
    elapsed = ((double)t2 - t1) / CLOCKS_PER_SEC * 1000;
    return elapsed;
}

int main(int argc, char *argv[]) {
    u_int64_t vertices, reached_vertices, edges;
    int64_t max_level;
    clock_t t1, t2;
    u_int64_t elapsed;
    int i;

    if (argc != 3) {
        printf("usage: %s graph_file root_id", argv[0]);
        exit(-__LINE__);
    }

    struct vertex_id_map **graph = read_graph(argv[1]);

    if (!graph)
        exit(-1);

    u_int64_t root = atoi(argv[2]);


    t1 = clock();
    for (i = 0;i < 100;i++)
    {
        parallelbfs(graph, root);
    }
    t2 = clock();
    elapsed = timediff(t1, t2);

    printf("\nParallel Operation took %lld milliseconds\n", elapsed);

    grather_statistics(graph, &vertices, &reached_vertices, &edges, &max_level);

    printf("Graph vertices: %lld with total edges %lld.  Reached vertices from %lld is %lld and max level is %lld\n",
        vertices, edges, root, reached_vertices, max_level);

    t1 = clock();
    for (i = 0;i < 100;i++)
    {
        bfs(graph, root);
    }
    t2 = clock();
    elapsed = timediff(t1, t2);

    printf("\Serial Operation took %lld milliseconds\n", elapsed);

    grather_statistics(graph, &vertices, &reached_vertices, &edges, &max_level);

    printf("Graph vertices: %lld with total edges %lld.  Reached vertices from %lld is %lld and max level is %lld\n",
        vertices, edges, root, reached_vertices, max_level);

    return 0;
}