# BuzzB using LSM

## Instructions to run


```bash
# Run
rm -rf buzzdbLSM.dat && g++ -std=c++17 -O3 -Wall -Werror -Wextra buzzdbLSM.cpp -o buzzdbLSM
./buzzdbLSM

# Display the database file
strings buzzdbLSM.dat | grep -o '2 0 4 [0-9]* 0 4 [0-9]*' | sed -E 's/2 0 4 ([0-9]+) 0 4 ([0-9]+)/\1 \2/'