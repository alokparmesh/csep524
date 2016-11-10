#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <windows.h>

//#include "timing.h"
#include "opencl_interface.h"

#define WG_SIZE     16
#define BLOCK_SIZE  2
char _muls[] =
"#define WG_SIZE    16\n"\
"#define BLOCK_SIZE 2\n"\
OPENCL_CODE(
    kernel void _muls(
        global const float *in_vector,
        global float *out_vector,
        float scalar) {
    const int pos = get_global_id(0) & (~(WG_SIZE - 1));
    const int local_id = get_local_id(0);
    int i;

    for (i = 0; i < BLOCK_SIZE; i++) {
        out_vector[pos * BLOCK_SIZE + local_id + i * WG_SIZE] =
            in_vector[pos * BLOCK_SIZE + local_id + i * WG_SIZE] * scalar;
    }
});


char _matrixVectorMuls[] =
"#define WG_SIZE    16\n"\
"#define BLOCK_SIZE 2\n"\
OPENCL_CODE(
    kernel void _matrixVectorMuls(
        global const int *in_matrix,
        global const int *in_vector,
        global int *out_vector,
        int row,
        int column) {
    const int rowPos = get_group_id(0) * WG_SIZE + get_local_id(0);
    int colPos;
    int sum = 0;
    for (colPos = 0; colPos < column; colPos++) {
        sum = sum + in_matrix[colPos * row + rowPos] * in_vector[colPos];
    }
    out_vector[rowPos] = sum;
});

// Wrapper
static bool _muls_init = false;
static cl_program _muls_program;

void muls_cleanup() {
    if (_muls_init)
        clReleaseProgram(_muls_program);
}

void muls(const float *in_vector,
    float *out_vector,
    float scalar,
    int len) {

    // Compile kernel program
    if (!_muls_init) {
        _muls_program = opencl_compile_program(_muls);
        _muls_init = true;
    }

    cl_int err;
    cl_mem cl_in_vector, cl_out_vector;
    cl_int cl_len = len;

    // copy input buffer
    cl_in_vector = clCreateBuffer(
        opencl_get_context(),
        CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
        sizeof(float) * len, (void *)in_vector, 
        &err);
    clCheckErr(err, "Failed to create device buffer");

    // create output buffer
    cl_out_vector = clCreateBuffer(
        opencl_get_context(), 
        CL_MEM_WRITE_ONLY,
        sizeof(float) * len, NULL, 
        &err);
    clCheckErr(err, "Failed to create device buffer");

    cl_float cl_scalar = scalar;

    // Create Kernel & set arguments
    cl_kernel kernel;
    kernel = clCreateKernel(_muls_program, "_muls", &err);
    clCheckErr(err, "Failed to create kernel");
    clCheck(clSetKernelArg(kernel, 0, sizeof(cl_in_vector), &cl_in_vector));
    clCheck(clSetKernelArg(kernel, 1, sizeof(cl_out_vector), &cl_out_vector));
    clCheck(clSetKernelArg(kernel, 2, sizeof(cl_float), &cl_scalar));

    cl_event kernel_completion;
    size_t global_work_size[1] = { len / BLOCK_SIZE };
    size_t local_work_size[1] = { WG_SIZE };

    clCheck(clEnqueueNDRangeKernel(
        opencl_get_queue(), kernel,
        1, NULL, 
        global_work_size, local_work_size, 0, NULL, &kernel_completion));

    clCheck(clWaitForEvents(1, &kernel_completion));
    clCheck(clReleaseEvent(kernel_completion));

    clCheck(clEnqueueReadBuffer(opencl_get_queue(), cl_out_vector, CL_TRUE,
        0, len * sizeof(float), out_vector, 0, NULL, NULL));

    clCheck(clReleaseMemObject(cl_in_vector));
    clCheck(clReleaseMemObject(cl_out_vector));
    clCheck(clReleaseKernel(kernel));
}

typedef struct {
    int *matrix;
    int *vector;
    int *output;
    int actualRow;
    int row;
    int column;
} matrixMutliplyWork;

void matrixVectorMuls(matrixMutliplyWork *work) {

    // Compile kernel program
    if (!_muls_init) {
        _muls_program = opencl_compile_program(_matrixVectorMuls);
        _muls_init = true;
    }

    cl_int err;
    cl_mem in_matrix, in_vector, out_vector;
    cl_int row = work->row;
    cl_int column = work->column;

    // copy input buffer
    in_matrix = clCreateBuffer(
        opencl_get_context(),
        CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
        sizeof(int) * work->row * work->column, (void *)work->matrix,
        &err);
    clCheckErr(err, "Failed to create device buffer");

    in_vector = clCreateBuffer(
        opencl_get_context(),
        CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
        sizeof(int) * work->column, (void *)work->vector,
        &err);
    clCheckErr(err, "Failed to create device buffer");

    // create output buffer
    out_vector = clCreateBuffer(
        opencl_get_context(),
        CL_MEM_WRITE_ONLY,
        sizeof(int) * work->row, NULL,
        &err);
    clCheckErr(err, "Failed to create device buffer");

    // Create Kernel & set arguments
    cl_kernel kernel;
    kernel = clCreateKernel(_muls_program, "_matrixVectorMuls", &err);
    clCheckErr(err, "Failed to create kernel");
    clCheck(clSetKernelArg(kernel, 0, sizeof(in_matrix), &in_matrix));
    clCheck(clSetKernelArg(kernel, 1, sizeof(in_vector), &in_vector));
    clCheck(clSetKernelArg(kernel, 2, sizeof(out_vector), &out_vector));
    clCheck(clSetKernelArg(kernel, 3, sizeof(cl_int), &row));
    clCheck(clSetKernelArg(kernel, 4, sizeof(cl_int), &column));

    cl_event kernel_completion;
    size_t global_work_size[1] = { row };
    size_t local_work_size[1] = { WG_SIZE };

    clCheck(clEnqueueNDRangeKernel(
        opencl_get_queue(), kernel,
        1, NULL,
        global_work_size, local_work_size, 0, NULL, &kernel_completion));

    clCheck(clWaitForEvents(1, &kernel_completion));
    clCheck(clReleaseEvent(kernel_completion));

    clCheck(clEnqueueReadBuffer(opencl_get_queue(), out_vector, CL_TRUE,
        0, sizeof(int) * work->row, (void *)work->output, 0, NULL, NULL));

    clCheck(clReleaseMemObject(in_matrix));
    clCheck(clReleaseMemObject(in_vector));
    clCheck(clReleaseMemObject(out_vector));
    clCheck(clReleaseKernel(kernel));
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

matrixMutliplyWork *initmatrixMutliplyWork()
{
    matrixMutliplyWork *work = (matrixMutliplyWork*)zmalloc(sizeof(matrixMutliplyWork));

    work->actualRow = 0;
    work->row = 0;
    work->column = 0;
    work->matrix = NULL;
    work->vector = NULL;
    work->output = NULL;

    return work;
}

void freematrixMutliplyWork(matrixMutliplyWork *work)
{
    if (work->matrix != NULL)
    {
        free(work->matrix);
    }

    if (work->vector != NULL)
    {
        free(work->vector);
    }


    if (work->output != NULL)
    {
        free(work->output);
    }

    free(work);
}

void * multiply(void* arg)
{
    matrixMutliplyWork  *work = (matrixMutliplyWork  *)arg;
    int i, j;

    for (i = 0; i < work->row; i++)
    {
        int sum = 0;
        for (j = 0; j < work->column; j++)
        {
            sum += work->matrix[j * work->row + i] * work->vector[j];
        }
        work->output[i] = sum;
    }

    return NULL;
}

void host_muls(const float *in_vector,
    float *out_vector,
    float scalar,
    int len) {
    int i;

    for (i = 0; i < len; i++)
        out_vector[i] = in_vector[i] * scalar;
}

void printMatrix(matrixMutliplyWork *work)
{
    printf("row : %d, column : %d\n", work->actualRow, work->column);
    int i, j;
    for (i = 0; i < work->actualRow; i++)
    {
        for (j = 0; j < work->column; j++)
        {
            printf("%d\t", work->matrix[j * work->row + i]);
        }
        printf("\n");
    }

    for (i = 0; i < work->column; i++)
    {
        printf("%d\n", work->vector[i]);
    }
}

void printOutput(int *array, int size)
{
    int i;
    for (i = 0; i < size; i++)
    {
        printf("%d\n", array[i]);
    }
}

// write output array to file
void writeOutput(int *array, int size)
{
    FILE *fw = fopen("output.txt", "w");
    if (!fw) {
        printf("Failed to open:");
        return;
    }

    int i;
    for (i = 0; i < size; i++)
    {
        fprintf(fw, "%d\n", array[i]);
    }

    fclose(fw);
}

matrixMutliplyWork *read_inputFile(char *filename) {
    FILE *inputFile = fopen(filename, "r");

    if (!inputFile) {
        printf("Failed to open:");
        return NULL;
    }

    matrixMutliplyWork *work = initmatrixMutliplyWork();

    fscanf(inputFile, "%d,%d\n", &work->actualRow, &work->column);
    work->row = work->actualRow;
    if (work->row & (WG_SIZE - 1)) {
        work->row = (work->row & (~(WG_SIZE - 1))) + WG_SIZE;
    }
    work->matrix = (int*)zmalloc(sizeof(int) * work->row * work->column);

    int maxSizeOfLine = sizeof(char) * work->actualRow * 11;
    char * buffer = (char *)malloc(maxSizeOfLine);
    char *record, *line;
    int i = 0, j = 0;

    for (i = 0; i < work->actualRow; i++) {
        line = fgets(buffer, maxSizeOfLine, inputFile);
        j = 0;
        record = strtok(line, ",");
        while (record != NULL)
        {
            work->matrix[j * work->row + i] = atoi(record);
            j++;
            record = strtok(NULL, ",");
        }
    }

    work->vector = (int*)zmalloc(sizeof(int) * work->column);
    for (i = 0; i < work->column; i++) {
        line = fgets(buffer, maxSizeOfLine, inputFile);
        work->vector[i] = atoi(line);
    }

    work->output = (int*)zmalloc(sizeof(int) * work->row);

    free(buffer);
    fclose(inputFile);
    return work;
}


int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("usage: %s inputFile\n", argv[0]);
        return -__LINE__;
    }

    matrixMutliplyWork *work = read_inputFile(argv[1]);
    int start, end;

    if (work != NULL)
    {
        /*
        printMatrix(work);
        start = GetTickCount();
        multiply(work);
        end = GetTickCount();
        printf("Host V*S: %.2f milliseconds\n", ((double)((end - start))));

        printOutput(work->output, work->actualRow);
        //writeOutput(work->output, work->actualRow);

        
        */

        opencl_start();

        printMatrix(work);

        start = GetTickCount();
        matrixVectorMuls(work);
        end = GetTickCount();

        printf("GPU M*S: %.2f milliseconds\n", ((double)((end - start))));

        printOutput(work->output, work->actualRow);
        freematrixMutliplyWork(work);
        muls_cleanup();
        opencl_end();
    }
    return 0;
}

/*
float   *vector, *result, temp;
int     i;
int     dim = atoi(argv[1]);
int start, end;

if (dim & (BLOCK_SIZE * WG_SIZE - 1)) {
dim = (dim & (~(BLOCK_SIZE * WG_SIZE - 1))) + BLOCK_SIZE * WG_SIZE;
}
vector = malloc(sizeof(*vector) * dim);
result = malloc(sizeof(*result) * dim);

for (i = 0; i < dim; i++) {
vector[i] = i;
}

start = GetTickCount();
host_muls(vector, result, 5.0, dim);
end = GetTickCount();
printf("Host V*S: %.2f milliseconds\n", ((double)((end - start))) );

// muls(vector, result, 6.0, dim);
start = GetTickCount();
muls(vector, result, 7.0, dim);
end = GetTickCount();
printf("GPU M*S: %.2f milliseconds\n", ((double)((end - start))));

// Check the results
for (i = 0; i < dim; i++) {
temp = vector[i] * 7.0;
if ((temp != 0.0 || vector[i] != 0.0) && fabs(temp - result[i]) > 0.1 * temp) {
printf("Result index: %d seems wrong: %f != %f\n",
i, result[i], temp);
exit(-__LINE__);
}
}

*/

