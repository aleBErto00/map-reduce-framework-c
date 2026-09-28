CC = gcc
CFLAGS = -Wall -Wextra -std=c11 -O2 -Iinclude
LDFLAGS = -lpthread

LIB_NAME = libmr.a
SRC = src/mr_framework.c src/utilis.c
OBJ = $(SRC:.c=.o)

all: $(LIB_NAME) examples/word_count examples/print_output 

$(LIB_NAME): $(OBJ)
	ar rcs $@ $(OBJ)

examples/word_count: examples/word_count.c  $(LIB_NAME)
	$(CC) $(CFLAGS) $< -L. -lmr -o $@ $(LDFLAGS)

examples/print_output: examples/print_output.c  $(LIB_NAME)
	$(CC) $(CFLAGS) $< -L. -lmr -o $@ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

test: all
	mkdir -p input_dir
	echo "casa albero casa mela mela albero casa cane otto fuoco casa albero" > input_dir/01_file.txt
	echo "mela casa albero albero" > input_dir/02_file.txt
	echo "123 456 123 789 123" > input_dir/03_file.txt
	$(CC) $(CFLAGS) tests/test_mr.c -L. -lmr -o tests/test_mr $(LDFLAGS)
	./tests/test_mr
	./examples/word_count

clean:
	rm -f src/*.o examples/word_count  examples/print_output tests/test_mr $(LIB_NAME) output.mro esecuzione.log  mr_stats.txt
	rm -rf input_dir

.PHONY: all test clean