#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>
#include <sys/types.h>

typedef struct {
    int *matrix;
    int *vector;
    int *output;
    int row;
    int column;
} matrixMutliplyWork;

matrixMutliplyWork *initmatrixMutliplyWork()
{
    matrixMutliplyWork *work = (matrixMutliplyWork*)malloc(sizeof(matrixMutliplyWork));

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


void printMatrix(matrixMutliplyWork *work)
{
    printf("row : %d, column : %d", work->row, work->column);
    int i, j;
    for (i = 0; i < work->row; i++)
    {
        for (j = 0; j < work->column; j++)
        {
            printf("%d\t", work->matrix[i * work->column + j]);
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

    if(!fscanf(inputFile, "%d,%d\n", &work->row, &work->column))
	{
		 printf("Failed to read file");
        return NULL;
	}
	
	work->matrix = (int*)malloc(sizeof(int) * work->row * work->column);

    int maxSizeOfLine = sizeof(char) * work->row * 11;
    char * buffer = (char *)malloc(maxSizeOfLine);
    char *record, *line;
    int i = 0, j = 0;

    for (i = 0; i < work->row; i++) {
        line = fgets(buffer, maxSizeOfLine, inputFile);
        j = 0;
        record = strtok(line, ",");
        while (record != NULL)
        {
            work->matrix[i * work->column + j] = atoi(record);
            j++;
            record = strtok(NULL, ",");
        }
    }

    work->vector = (int*)malloc(sizeof(int) * work->column);
    for (i = 0; i < work->column; i++) {
        line = fgets(buffer, maxSizeOfLine, inputFile);
        work->vector[i] = atoi(line);
    }

    work->output = (int*)malloc(sizeof(int) * work->row);


    free(buffer);
    fclose(inputFile);
    return work;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("usage: %s inputFile\n", argv[0]);
        return -__LINE__;
    }

    MPI_Init(&argc, &argv);

    int my_process, processes;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_process);
    MPI_Comm_size(MPI_COMM_WORLD, &processes);

    if (my_process == 0) {
        matrixMutliplyWork *work = read_inputFile(argv[1]);
        MPI_Bcast(&work->row, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(&work->column, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(work->vector, work->column, MPI_INT, 0, MPI_COMM_WORLD);

        int i, workerProcessCount = processes - 1;
        for (i = 0; i < work->row; i++)
        {
            int destination = 1+ (i % workerProcessCount);
            MPI_Send(&work->matrix[i * work->column], work->column, MPI_INT, destination, 0, MPI_COMM_WORLD);
        }

        for (i = 0; i < work->row; i++)
        {
            int output;
            MPI_Status status;
            MPI_Recv(&output, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD , &status);
            work->output[status.MPI_TAG] = output;
        }

        //printOutput(work->output,work->row);
        writeOutput(work->output, work->row);
        freematrixMutliplyWork(work);
    }
    else {
        int row, column;
        int *vector, *matrixRow;

        MPI_Bcast(&row, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(&column, 1, MPI_INT, 0, MPI_COMM_WORLD);

        vector = (int*)malloc(sizeof(int) * column);
        MPI_Bcast(vector, column, MPI_INT, 0, MPI_COMM_WORLD);

        matrixRow = (int*)malloc(sizeof(int) * column);

        int currentRow, workerProcessCount = processes - 1;
        for (currentRow = my_process-1; currentRow <row; currentRow += workerProcessCount)
        {
            MPI_Recv(matrixRow, column, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            int i, output = 0;
            for (i = 0; i < column; i++)
            {
                output += matrixRow[i] * vector[i];
            }

            MPI_Send(&output, 1, MPI_INT, 0, currentRow, MPI_COMM_WORLD);
        }

        free(vector);
        free(matrixRow);
    }

    MPI_Finalize();
    return 0;
}