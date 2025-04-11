# CXX := g++
# CXXFLAGS := -std=c++17 -Wall -Wextra -I./include
# LDFLAGS := -lstdc++fs -Wl,-subsystem,console  # Removed '-Wl,-subsystem,console'

# SRC_DIR := src
# BIN_DIR := bin

# SRCS := $(wildcard $(SRC_DIR)/*.cpp)
# OBJS := $(patsubst $(SRC_DIR)/%.cpp, $(SRC_DIR)/%.o, $(SRCS))

# TARGET := $(BIN_DIR)/dbms

# all: $(TARGET)

# $(TARGET): $(OBJS)
# 	if not exist $(BIN_DIR) mkdir $(BIN_DIR)
# 	$(CXX) $(CXXFLAGS) $(OBJS) -o $(TARGET) $(LDFLAGS)

# $(SRC_DIR)/%.o: $(SRC_DIR)/%.cpp
# 	$(CXX) $(CXXFLAGS) -c $< -o $@


# clean:
# 	@if exist obj\*.o del /F /Q obj\*.o
# 	@if exist bin\dbms.exe del /F /Q bin\dbms.exe

# Define source files explicitly
# Source files (same directory as Makefile)
SRC := BPlusTree.cpp Catalog.cpp DB_Manager.cpp main.cpp Transaction.cpp Index.cpp

# Convert .cpp files to .o in obj/
OBJ := $(patsubst %.cpp,obj/%.o,$(SRC))

# Compiler
CXX := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -I./include
LDFLAGS := 

# Executable
BIN := bin/dbms.exe

# Default target
all: ensure_dirs $(BIN)

# Ensure obj and bin directories exist
ensure_dirs:
	if not exist obj mkdir obj
	if not exist bin mkdir bin

# Link object files into the final executable
$(BIN): $(OBJ)
	$(CXX) $(CXXFLAGS) $(OBJ) -o $(BIN) $(LDFLAGS)

# Compile .cpp to .o
obj/%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean build files
clean:
	@if exist obj del /F /Q obj\*.o 2>nul
	@if exist bin\dbms.exe del /F /Q bin\dbms.exe 2>nul

