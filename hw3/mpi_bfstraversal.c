#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <inttypes.h>
#include <unistd.h>
#include <mpi.h>

//typedef __int32 int32_t;
//typedef unsigned __int32 u_int32_t;
//
//typedef __int64 int64_t;
//typedef unsigned __int64 u_int64_t;

#define HASH_MAP_SIZE   (1000001)
#define UNVISITED   (-1)

typedef struct {
    u_int64_t vertex_count;
    u_int64_t edge_count;
    u_int64_t max_vertex_id;
    u_int64_t *columnIndices;
    u_int64_t *rowOffsets;
    int64_t *distances;
} bfsTraversalWork;


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

    fseek(fp, 0, SEEK_END);
    u_int64_t   filesize = ftell(fp);

    u_int64_t edge_count = filesize / (2 * sizeof(u_int64_t));

    struct raw_edge *raw_edges = (struct raw_edge *)zmalloc(sizeof(*raw_edges) * edge_count);

    fseek(fp, 0, SEEK_SET);

    u_int64_t total_read = 0;

    char *base_ptr = (char *)raw_edges;
    while (true) {
        size_t read_size = fread(base_ptr, 1, filesize, fp);

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
    u_int64_t vertex_count = 0;
    for (edge_index = 0; edge_index < edge_count; edge_index++) {
        struct vertex *from_v, *to_v;

        from_v = locate_vertex_from_id(graph, raw_edges[edge_index].from);
        if (!from_v) {
            from_v = create_vertex(raw_edges[edge_index].from);
            insert_vertex_to_id_hash(graph, from_v);
            vertex_count++;
        }

        to_v = locate_vertex_from_id(graph, raw_edges[edge_index].to);
        if (!to_v) {
            to_v = create_vertex(raw_edges[edge_index].to);
            insert_vertex_to_id_hash(graph, to_v);
            vertex_count++;
        }

        add_edge_to_vertex(from_v, to_v);
    }

    return graph;
}

bfsTraversalWork *initbfsTraversalWork()
{
    bfsTraversalWork *work = (bfsTraversalWork*)zmalloc(sizeof(bfsTraversalWork));

    work->vertex_count = 0;
    work->edge_count = 0;
    work->max_vertex_id = 0;
    work->columnIndices = NULL;
    work->rowOffsets = NULL;
    work->distances = NULL;

    return work;
}

void freebfsTraversalWork(bfsTraversalWork *work)
{
    if (work->columnIndices != NULL)
    {
        free(work->columnIndices);
    }

    if (work->rowOffsets != NULL)
    {
        free(work->rowOffsets);
    }


    if (work->distances != NULL)
    {
        free(work->distances);
    }

    free(work);
}

bfsTraversalWork * convertToAdjacencyMatrix(struct vertex_id_map **map)
{
    u_int64_t i, j;
    struct vertex_id_map *vm;
    bfsTraversalWork* work = initbfsTraversalWork();

    for (i = 0; i < HASH_MAP_SIZE; i++) {
        vm = map[i];
        while (vm) {
            if (vm->vertex->id > work->max_vertex_id)
            {
                work->max_vertex_id = vm->vertex->id;
            }
            work->vertex_count++;
            work->edge_count += vm->vertex->edge_count;
            vm = vm->next;
        }
    }

    work->rowOffsets = (u_int64_t*)zmalloc(sizeof(u_int64_t) * (work->max_vertex_id + 2));
    work->columnIndices = (u_int64_t*)zmalloc(sizeof(u_int64_t) * work->edge_count);
    work->distances = (int64_t*)zmalloc(sizeof(int64_t) * (work->max_vertex_id + 1));

    // set all distances to unvisited
    memset(work->distances, UNVISITED, sizeof(int64_t) * (work->max_vertex_id + 1));

    struct vertex *vertex;
    u_int64_t rowOffset = 0;
    for (i = 0; i <= work->max_vertex_id; i++) {
        vertex = locate_vertex_from_id(map, i);
        work->rowOffsets[i] = rowOffset;
        if (vertex)
        {
            for (j = 0; j < vertex->edge_count; j++) {
                struct vertex *nv = vertex->edges[j];
                work->columnIndices[rowOffset] = nv->id;
                rowOffset++;
            }
        }
    }
    work->rowOffsets[i] = rowOffset;
    return work;
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

void _clear_bfs_state(struct vertex *v, void *arg) {
    v->level = UNVISITED;
    v->next = NULL;
    v->prev = NULL;
}

void clear_bfs_state(struct vertex_id_map **graph) {
    for_all_vertices(graph, _clear_bfs_state, NULL);
}

void bfs(struct vertex_id_map **graph, u_int64_t root) {
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

void mpi_bfs(bfsTraversalWork *work, int root, int myProcess, int numWorkerProcess)
{
    // Calculate max size of queue for each worker process
    int vertexPartitionSize = (work->max_vertex_id + 1) / numWorkerProcess;

    if ((work->max_vertex_id + 1) % numWorkerProcess != 0)
        vertexPartitionSize++;
    //printf("Process %d PartitionSize: %I64d\n", myProcess, vertexPartitionSize);

    // Allocate space for current queue and next level queue for each worker process
    int myCurrentLevelQueueSize = 0;
    int * myCurrentLevelQueue = (int*)zmalloc(sizeof(int) * vertexPartitionSize);
    int * myIncomingQueue = (int*)zmalloc(sizeof(int) * vertexPartitionSize);
    int * nextLevelQueue = (int*)zmalloc(sizeof(int) * numWorkerProcess  * vertexPartitionSize);

    // Add root to appropriate current queue if applicable
    unsigned int level = 0;
    if (root % numWorkerProcess == myProcess)
    {
        myCurrentLevelQueueSize++;
        myCurrentLevelQueue[(root / numWorkerProcess)] = 1;
        work->distances[root] = level;
    }

    do
    {
        u_int64_t i, j;
       // printf("Process %d Level: %I64d with Queue Size %d\n", myProcess, level, myCurrentLevelQueueSize);

        // Iterate through the queue 
        if (myCurrentLevelQueueSize > 0)
        {
            for (i = 0; i <= vertexPartitionSize; i++)
            {
                if (myCurrentLevelQueue[i] == 1)
                {
                    // Get actual vertex id from queue map
                    u_int64_t vertexId = i * numWorkerProcess + myProcess;
                    // printf("Process %d Vertex %I64d\n", myProcess, vertexId);

                    // Iterate through child of vertex if it is on current level and store them in appropriate next level queue
                    if (vertexId <= work->max_vertex_id)
                    {
                        if (work->distances[vertexId] == level)
                        {
                            for (j = work->rowOffsets[vertexId];j <= work->rowOffsets[vertexId + 1] - 1;j++)
                            {
                                u_int64_t targetVertexId = work->columnIndices[j];
                                if (work->distances[targetVertexId] == UNVISITED)
                                {
                                    // printf("Process %d target Vertex %I64d\n", myProcess, targetVertexId);
                                    work->distances[targetVertexId] = level + 1;
                                    nextLevelQueue[((targetVertexId % numWorkerProcess) * vertexPartitionSize) + (targetVertexId / numWorkerProcess)] = 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        myCurrentLevelQueueSize = 0;
        memset(myCurrentLevelQueue, 0, sizeof(int) * vertexPartitionSize);

        // Communicate all the next level queue back to coordinator process
        for (i = 0; i < numWorkerProcess; i++)
        {
            MPI_Send(nextLevelQueue + (i * vertexPartitionSize), vertexPartitionSize, MPI_INT, numWorkerProcess, i, MPI_COMM_WORLD);
        }
        //MPI_Barrier(MPI_COMM_WORLD);

        // Receive the consolidated next level items for the current process and it to the queue if not processed already
        memset(myIncomingQueue, 0, sizeof(int) * vertexPartitionSize);
        MPI_Recv(myIncomingQueue, vertexPartitionSize, MPI_INT, numWorkerProcess, 0, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);
        //MPI_Barrier(MPI_COMM_WORLD);

        for (j = 0;j < vertexPartitionSize;j++)
        {
            if (myIncomingQueue[j] == 1 && myCurrentLevelQueue[j] != 1)
            {
                u_int64_t vertexId = j * numWorkerProcess + myProcess;
                if (vertexId <= work->max_vertex_id)
                {
                    if (work->distances[vertexId] == UNVISITED || work->distances[vertexId] == (level + 1))
                    {
                        work->distances[vertexId] = level + 1;
                        myCurrentLevelQueue[j] = 1;
                        myCurrentLevelQueueSize++;
                    }
                }
            }
        }

        // Send the queue size back to coordinator process for exit condition
        MPI_Send(&myCurrentLevelQueueSize, 1, MPI_INT, numWorkerProcess, 0, MPI_COMM_WORLD);

        // Receive next level to proceed or 0 to exit
        MPI_Bcast(&level, 1, MPI_UNSIGNED, numWorkerProcess, MPI_COMM_WORLD);
        //MPI_Barrier(MPI_COMM_WORLD);
        memset(nextLevelQueue, 0, sizeof(int) * numWorkerProcess  * vertexPartitionSize);
    } while (level > 0);

    free(myIncomingQueue);
    free(myCurrentLevelQueue);
    free(nextLevelQueue);
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

void grather_statistics_bfsTraversalWork(bfsTraversalWork *work,
    u_int64_t *vertices,
    u_int64_t *reached_vertices,
    u_int64_t *edges,
    int64_t *max_level) {
    *vertices = work->vertex_count;
    *edges = work->edge_count;

    u_int64_t i, reachedvertices = 0;
    int64_t maxlevel = UNVISITED;
    for (i = 0; i <= work->max_vertex_id; i++)
    {
        if (work->distances[i] != UNVISITED)
        {
            reachedvertices++;
            if (work->distances[i] > maxlevel)
            {
                maxlevel = work->distances[i];
            }
        }
        /*
        else
        {
        if (work->rowOffsets[i] != work->rowOffsets[i + 1])
        {
        printf("Graph vertices: %I64d\n", i);
        }
        }
        */
    }

    *reached_vertices = reachedvertices;
    *max_level = maxlevel;
}

int main(int argc, char *argv[]) {
    u_int64_t vertices, reached_vertices, edges;
    int64_t max_level;

    if (argc != 3) {
        printf("usage: %s graph_file root_id", argv[0]);
        exit(-__LINE__);
    }

    MPI_Init(&argc, &argv);

    int my_process, processes;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_process);
    MPI_Comm_size(MPI_COMM_WORLD, &processes);

    int coordinatorProcess = processes - 1;

    if (my_process == coordinatorProcess) {
        // Coordinator process
        struct vertex_id_map **graph = read_graph(argv[1]);

        if (!graph)
            exit(-1);

        // Convert graph into rowset and indices matrix for communication
        bfsTraversalWork* work = convertToAdjacencyMatrix(graph);


        // Broadcast all the graph structure
        MPI_Bcast(&work->vertex_count, 1, MPI_UNSIGNED_LONG_LONG, coordinatorProcess, MPI_COMM_WORLD);
        MPI_Bcast(&work->max_vertex_id, 1, MPI_UNSIGNED_LONG_LONG, coordinatorProcess, MPI_COMM_WORLD);
        MPI_Bcast(&work->edge_count, 1, MPI_UNSIGNED_LONG_LONG, coordinatorProcess, MPI_COMM_WORLD);

        MPI_Bcast(work->rowOffsets, work->max_vertex_id + 2, MPI_UNSIGNED_LONG_LONG, coordinatorProcess, MPI_COMM_WORLD);
        MPI_Bcast(work->columnIndices, work->edge_count, MPI_UNSIGNED_LONG_LONG, coordinatorProcess, MPI_COMM_WORLD);

        // Read and broadcast root
        u_int64_t root = atoi(argv[2]);
        MPI_Bcast(&root, 1, MPI_UNSIGNED_LONG_LONG, coordinatorProcess, MPI_COMM_WORLD);


        // Initialize space for each worker process queue
        int vertexPartitionSize = (work->max_vertex_id + 1) / coordinatorProcess;
        if ((work->max_vertex_id + 1) % coordinatorProcess != 0)
            vertexPartitionSize++;
        int * nextLevelQueue = (int*)zmalloc(sizeof(int) * coordinatorProcess  * vertexPartitionSize);
        int * myCurrentLevelQueue = (int*)zmalloc(sizeof(int) * vertexPartitionSize);

        //proceed if node is present in graph
        if ((root <= work->max_vertex_id) && (work->rowOffsets[root] != work->rowOffsets[root + 1]))
        {
            unsigned int level = 0;
            int i, j;

            do
            {
                // reset the space for new level
                //printf("Coordinator Process %d Level: %d\n", my_process, level);
                memset(nextLevelQueue, 0, sizeof(int) * coordinatorProcess  * vertexPartitionSize);
                memset(myCurrentLevelQueue, 0, sizeof(int) * vertexPartitionSize);


                // Receive all next level vertexes from each proces to other process
                for (i = 0; i < coordinatorProcess * coordinatorProcess; i++)
                {
                    MPI_Status status;
                    MPI_Recv(myCurrentLevelQueue, vertexPartitionSize, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

                    // Merge the map
                    for (j = 0;j < vertexPartitionSize;j++)
                    {
                        if (myCurrentLevelQueue[j] == 1)
                        {
                            nextLevelQueue[status.MPI_TAG * vertexPartitionSize + j] = 1;
                        }
                    }
                }
                //MPI_Barrier(MPI_COMM_WORLD);

                // Send the next level queue items to each worker
                for (i = 0; i < coordinatorProcess; i++)
                {
                    MPI_Send(nextLevelQueue + (i * vertexPartitionSize), vertexPartitionSize, MPI_INT, i, 0, MPI_COMM_WORLD);
                }
                //MPI_Barrier(MPI_COMM_WORLD);

                //level++;

                // Check the queue size from each worker to make sure we need next level iteration or not
                int totalQueueSize = 0;
                for (i = 0; i < coordinatorProcess; i++)
                {
                    int processQueueSize;
                    MPI_Recv(&processQueueSize, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    totalQueueSize += processQueueSize;
                }

                if (totalQueueSize > 0)
                {
                    level++;
                }
                else
                {
                    level = 0;
                }

                // Broadcast the level
                MPI_Bcast(&level, 1, MPI_UNSIGNED, coordinatorProcess, MPI_COMM_WORLD);
                //MPI_Barrier(MPI_COMM_WORLD);
            } while (level > 0);


            // Collect the distances of each node
            for (i = 0; i <= work->max_vertex_id; i++)
            {
                int64_t distance;
                MPI_Status status;
                MPI_Recv(&distance, 1, MPI_LONG_LONG, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
                work->distances[status.MPI_TAG] = distance;
            }
        }

        grather_statistics_bfsTraversalWork(work, &vertices, &reached_vertices, &edges, &max_level);
        printf("Graph vertices: %ld with total edges %ld.  Reached vertices from %ld is %ld and max level is %ld\n",
            vertices, edges, root, reached_vertices, max_level);

        free(nextLevelQueue);
        free(myCurrentLevelQueue);
        freebfsTraversalWork(work);
    }
    else {
        bfsTraversalWork* work = initbfsTraversalWork();
        u_int64_t root;

        // Recieve common graph structures
        MPI_Bcast(&work->vertex_count, 1, MPI_UNSIGNED_LONG_LONG, coordinatorProcess, MPI_COMM_WORLD);
        MPI_Bcast(&work->max_vertex_id, 1, MPI_UNSIGNED_LONG_LONG, coordinatorProcess, MPI_COMM_WORLD);
        MPI_Bcast(&work->edge_count, 1, MPI_UNSIGNED_LONG_LONG, coordinatorProcess, MPI_COMM_WORLD);

        //printf("Process %d Graph vertices: %I64d with total edges %I64d. Max vertex is %I64d\n", my_process, work->vertex_count, work->edge_count, work->max_vertex_id);

        work->rowOffsets = (u_int64_t*)zmalloc(sizeof(u_int64_t) * (work->max_vertex_id + 2));
        work->columnIndices = (u_int64_t*)zmalloc(sizeof(u_int64_t) * work->edge_count);

        MPI_Bcast(work->rowOffsets, work->max_vertex_id + 2, MPI_UNSIGNED_LONG_LONG, coordinatorProcess, MPI_COMM_WORLD);
        MPI_Bcast(work->columnIndices, work->edge_count, MPI_UNSIGNED_LONG_LONG, coordinatorProcess, MPI_COMM_WORLD);

        work->distances = (int64_t*)zmalloc(sizeof(int64_t) * (work->max_vertex_id + 1));
        // set all distances to unvisited
        memset(work->distances, UNVISITED, sizeof(int64_t) * (work->max_vertex_id + 1));

        MPI_Bcast(&root, 1, MPI_UNSIGNED_LONG_LONG, coordinatorProcess, MPI_COMM_WORLD);

        //printf("Process %d Root %ld\n", my_process, root);


        //proceed if node is present in graph
        if ((root <= work->max_vertex_id) && (work->rowOffsets[root] != work->rowOffsets[root + 1]))
        {
            // Do Bfs traversal
            mpi_bfs(work, root, my_process, coordinatorProcess);

            //grather_statistics_bfsTraversalWork(work, &vertices, &reached_vertices, &edges, &max_level);
            //printf("Process %d Graph vertices: %I64d with total edges %I64d.  Reached vertices from %I64d is %I64d and max level is %I64d\n",
             //  my_process, vertices, edges, root, reached_vertices, max_level);

            // return the distances of nodes affinitized to this process
            int i;
            for (i = my_process; i <= work->max_vertex_id; i += coordinatorProcess)
            {
                MPI_Send(&work->distances[i], 1, MPI_LONG_LONG, coordinatorProcess, i, MPI_COMM_WORLD);
            }
        }

        freebfsTraversalWork(work);
    }

    MPI_Finalize();
    return 0;
}

