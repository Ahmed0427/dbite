CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra

SRCFILE := tests.cpp

TEST := test

SRC_OBJS := $(wildcard ../obj/*.o)

all: $(TEST)

$(TEST): $(SRCFILE) $(SRC_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm $(TEST)

