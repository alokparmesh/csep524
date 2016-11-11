#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <windows.h>

typedef __int32 int32_t;
typedef unsigned __int32 u_int32_t;

typedef __int64 int64_t;
typedef unsigned __int64 u_int64_t;

#include "opencl_interface.h"

#define WG_SIZE     16

// Adjust this up or down depending on the graph size of interest.
// It does not effect BFS speed, but it will effect ID -> vertex mapping speed
#define HASH_MAP_SIZE   (1000001)
#define UNVISITED   (-1)

char _bfsTraversal[] =
"#define WG_SIZE    16\n"\
"#define UNVISITED   (-1)\n"\
OPENCL_CODE(
    kernel void _bfsTraversal(
        global const ulong *rowOffsets,
        global const ulong *columnIndices,
        global long *distances,
        global int *notCompleted,
        const ulong max_vertex_id,
        const int level) {
    const int i = get_group_id(0) * WG_SIZE + get_local_id(0);
    long j;
    if (i <= max_vertex_id)
    {
        if (distances[i] == level)
        {
            notCompleted[0] = 1;

            for (j = rowOffsets[i];j <= rowOffsets[i + 1] - 1;j++)
            {
                if (distances[columnIndices[j]] == UNVISITED)
                {
                    distances[columnIndices[j]] = level + 1;
                }
            }
        }
    }
});

// Wrapper
static bool _bfsTraversal_init = false;
static cl_program _bfsTraversal_program;

void bfsTraversal_cleanup() {
    if (_bfsTraversal_init)
        clReleaseProgram(_bfsTraversal_program);
}

typedef struct {
    u_int64_t vertex_count;
    u_int64_t edge_count;
    u_int64_t max_vertex_id;
    u_int64_t *columnIndices;
    u_int64_t *rowOffsets;
    int64_t *distances;
} bfsTraversalWork;

void bfsTraversal(bfsTraversalWork *work, int root) {
    if (root <= work->max_vertex_id)
    {
        work->distances[root] = 0;

        // Compile kernel program
        if (!_bfsTraversal_init) {
            _bfsTraversal_program = opencl_compile_program(_bfsTraversal);
            _bfsTraversal_init = true;
        }

        int notCompleted[] = { 0 };
        cl_int err;
        cl_mem rowOffsets, columnIndices, distances, notCompletedBuffer;
        cl_ulong max_vertex_id = work->max_vertex_id;
        cl_int level = 0;

        rowOffsets = clCreateBuffer(
            opencl_get_context(),
            CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
            sizeof(cl_ulong) *(work->max_vertex_id + 2), (void *)work->rowOffsets,
            &err);
        clCheckErr(err, "Failed to create device buffer");

        columnIndices = clCreateBuffer(
            opencl_get_context(),
            CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
            sizeof(cl_ulong) *  work->edge_count, (void *)work->columnIndices,
            &err);
        clCheckErr(err, "Failed to create device buffer");

        distances = clCreateBuffer(
            opencl_get_context(),
            CL_MEM_READ_WRITE | CL_MEM_COPY_HOST_PTR,
            sizeof(cl_long) *(work->max_vertex_id + 1), (void *)work->distances,
            &err);
        clCheckErr(err, "Failed to create device buffer");

        notCompletedBuffer = clCreateBuffer(
            opencl_get_context(),
            CL_MEM_READ_WRITE | CL_MEM_COPY_HOST_PTR,
            sizeof(cl_int) * 1, (void *)notCompleted,
            &err);
        clCheckErr(err, "Failed to create device buffer");

        // Create Kernel & set arguments
        cl_kernel kernel;
        kernel = clCreateKernel(_bfsTraversal_program, "_bfsTraversal", &err);
        clCheckErr(err, "Failed to create kernel");
        clCheck(clSetKernelArg(kernel, 0, sizeof(rowOffsets), &rowOffsets));
        clCheck(clSetKernelArg(kernel, 1, sizeof(columnIndices), &columnIndices));
        clCheck(clSetKernelArg(kernel, 2, sizeof(distances), &distances));
        clCheck(clSetKernelArg(kernel, 4, sizeof(cl_ulong), &max_vertex_id));
        

        cl_event kernel_completion;
        cl_ulong workSize  = max_vertex_id;
        if (workSize & (WG_SIZE - 1)) {
            workSize = (workSize & (~(WG_SIZE - 1))) + WG_SIZE;
        }
        size_t global_work_size[1] = { workSize };
        size_t local_work_size[1] = { WG_SIZE };

        notCompleted[0] = 0;
        do
        {
            notCompleted[0] = 0;
            clCheck(clEnqueueWriteBuffer(opencl_get_queue(), notCompletedBuffer, CL_TRUE,
                0, sizeof(cl_int) * 1, (void *)notCompleted, 0, NULL, NULL));

            clCheck(clSetKernelArg(kernel, 3, sizeof(notCompletedBuffer), &notCompletedBuffer));
            clCheck(clSetKernelArg(kernel, 5, sizeof(cl_int), &level));
            clCheck(clEnqueueNDRangeKernel(
                opencl_get_queue(), kernel,
                1, NULL,
                global_work_size, local_work_size, 0, NULL, &kernel_completion));

            clCheck(clWaitForEvents(1, &kernel_completion));
            clCheck(clReleaseEvent(kernel_completion));

            clCheck(clEnqueueReadBuffer(opencl_get_queue(), notCompletedBuffer, CL_TRUE,
                0, sizeof(cl_int) * 1, (void *)notCompleted, 0, NULL, NULL));
            level++;
        } while (notCompleted[0] != 0);

        clCheck(clEnqueueReadBuffer(opencl_get_queue(), distances, CL_TRUE,
            0, sizeof(cl_long) * (work->max_vertex_id + 1), (void *)work->distances, 0, NULL, NULL));

        clCheck(clReleaseMemObject(rowOffsets));
        clCheck(clReleaseMemObject(columnIndices));
        clCheck(clReleaseMemObject(distances));
        clCheck(clReleaseKernel(kernel));
    }
}

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
    u_int64_t i,j;
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

    work->rowOffsets = (u_int64_t*)zmalloc(sizeof(u_int64_t) * (work->max_vertex_id +2));
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

void host_bfs(bfsTraversalWork *work, int root)
{
    u_int64_t i,j;
    
    if (root <= work->max_vertex_id)
    {
        work->distances[root] = 0;
        int64_t level = 0;

        bool done = true;
        do
        {
            done = true;
            for (i = 0; i <= work->max_vertex_id; i++)
            {
                if (work->distances[i] == level)
                {
                    done = false;

                    for (j = work->rowOffsets[i];j <= work->rowOffsets[i + 1] - 1;j++)
                    {
                        if (work->distances[work->columnIndices[j]] == UNVISITED)
                        {
                            work->distances[work->columnIndices[j]] = level + 1;
                        }
                    }
                }
            }

            level++;
        } while (!done);
    }
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

    struct vertex_id_map **graph = read_graph(argv[1]);

    if (!graph)
        exit(-1);

    u_int64_t root = atoi(argv[2]);

    opencl_start();
    bfsTraversalWork* work = convertToAdjacencyMatrix(graph);
    //printf("Vertices: %ld  Edges %ld\n", work->vertex_count, work->edge_count);
   
    //host_bfs(work, root);
    bfsTraversal(work, root);
    bfs(graph, root);

    grather_statistics(graph, &vertices, &reached_vertices, &edges, &max_level);
    printf("Graph vertices bfs: %ld with total edges %ld.  Reached vertices from %ld is %ld and max level is %ld\n",
        vertices, edges, root, reached_vertices, max_level);

    grather_statistics_bfsTraversalWork(work, &vertices, &reached_vertices, &edges, &max_level);
    printf("Graph vertices parallel: %ld with total edges %ld.  Reached vertices from %ld is %ld and max level is %ld\n",
        vertices, edges, root, reached_vertices, max_level);

    freebfsTraversalWork(work);

    bfsTraversal_cleanup();
    opencl_end();
    return 0;
}
