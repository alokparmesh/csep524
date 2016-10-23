#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

typedef struct{
    int *matrix;
    int *vector;
    int row;
    int column;
} matrixMutliplyInput;

matrixMutliplyInput *initMatrixMutliplyInput()
{
    matrixMutliplyInput *input = (matrixMutliplyInput*)malloc(sizeof(matrixMutliplyInput)); 
    
    input->row = 0;
    input->column = 0;
    input->matrix = NULL;
    input->vector = NULL;

    return input;
}

void freeMatrixMutliplyInput(matrixMutliplyInput *input)
{
    if(input-> matrix != NULL)
    {
        free(input->matrix);
    }

    if(input->vector != NULL)
    {
        free(input->vector);
    }

    free(input);
}

int *multiply(matrixMutliplyInput *input)
{
    int* output = (int*)malloc(sizeof(int) * input->row);

    int i, j;
    for (i = 0; i < input->row; i++)
    {
        int sum = 0;
        for (j = 0; j < input->column; j++)
        {
            sum += input->matrix[i * input->column + j] * input->vector[j];
        }
        output[i] = sum;
    }

    return output;
}

void printMatrixMutliplyInput(matrixMutliplyInput *input)
{
    printf("row : %d, column : %d", input->row, input->column);
    int i, j;
    for (i = 0; i < input->row; i++)
    {
        for (j = 0; j < input->column; j++)
        {
                printf("%d\t", input->matrix[i * input->column + j]);
        }
        printf("\n");
    }

    for (i = 0; i < input->column; i++) 
    {
        printf("%d\n", input->vector[i]);
    }
}

void printArray(int *array, int size)
{
    int i;
     for (i = 0; i < size; i++) 
    {
        printf("%d\n", array[i]);
    }
}

matrixMutliplyInput *read_inputFile(char *filename) {
	FILE *inputFile = fopen(filename, "r");

	if (!inputFile) {
		printf("Failed to open:");
		return NULL;
	}
	matrixMutliplyInput *input = initMatrixMutliplyInput();
	fscanf(inputFile, "%d,%d\n", &input->row, &input->column);
	input->matrix = (int*)malloc(sizeof(int) * input->row * input->column);

	int maxSizeOfLine = sizeof(char) * input->row * 11;
	char * buffer = (char *)malloc(maxSizeOfLine);
	char *record, *line;
	int i = 0, j = 0;


	for (i = 0; i < input->row; i++) {
		line = fgets(buffer, maxSizeOfLine, inputFile);
		j = 0;
		record = strtok(line, ",");
		while (record != NULL)
		{
			input->matrix[i * input->column + j] = atoi(record);
			j++;
			record = strtok(NULL, ",");
		}
	}

    input->vector = (int*)malloc(sizeof(int) * input->column);
    for (i = 0; i < input->column; i++) {
		line = fgets(buffer, maxSizeOfLine, inputFile);
		input->vector[i] = atoi(line);
	}

	free(buffer);
	fclose(inputFile);
	return input;
}

int main(int argc, char *argv[]) {
	if (argc != 2) {
		printf("usage: %s inputFile\n", argv[0]);
		return -__LINE__;
	}

	matrixMutliplyInput *input = read_inputFile(argv[1]);

	if (input != NULL)
	{
		//printMatrixMutliplyInput(input);
        int *output = multiply(input);
        printArray(output,input->row);
		freeMatrixMutliplyInput(input);
	}

	return 0;
}
