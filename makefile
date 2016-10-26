all: MatrixMultiply bfs

MatrixMultiply : MatrixMultiply.c
	gcc -pthread MatrixMultiply.c -o MatrixMultiply.out	

bfs : bfs.c
	gcc -pthread bfs.c -o bfs.out

clean :
	rm -rf bfs.out MatrixMultiply.out