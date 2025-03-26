# BuzzB using LSM

## Instructions to run


```bash
# Run
g++ -std=c++17 -O3 -Wall -Werror -Wextra buzzdb.cpp
./a.out

# Display the database file
strings buzzdb.dat | grep -o '2 0 4 [0-9]* 0 4 [0-9]*' | sed -E 's/2 0 4 ([0-9]+) 0 4 ([0-9]+)/\1 \2/'