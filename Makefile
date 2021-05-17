build:
	mpicc main.c -o main
run:
	mpirun --oversubscribe -np 5 main
clean:
	rm main