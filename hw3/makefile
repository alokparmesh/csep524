all: MatrixMultiply bfs

MatrixMultiply : mpi_matrixmultiply.c
	mpicc mpi_matrixmultiply.c -o MatrixMultiply.out	

bfs : mpi_bfstraversal.c
	mpicc mpi_bfstraversal.c -o bfs.out

clean :
	rm -rf bfs.out MatrixMultiply.out