# BuzzB using LSM

## Instructions to run

### MacOS

```bash
# Run
rm -rf buzzdbLSM.dat && g++ -std=c++17 -O3 -Wall -Werror -Wextra buzzdbLSM.cpp -o buzzdbLSM
./buzzdbLSM

# Display the database file
strings buzzdbLSM.dat | grep -o '2 0 4 [0-9]* 0 4 [0-9]*' | sed -E 's/2 0 4 ([0-9]+) 0 4 ([0-9]+)/\1 \2/'
```

### Windows

```bash

rm '.\buzzdbLSM.dat'
rm '.\buzzdb_wal.log'
rm '.\buzzdbLSM.exe'
rm '.\out.txt'
g++ -std=c++17 -O3 -Wall -Werror -Wextra buzzdbLSM.cpp -o buzzdbLSM
.\buzzdbLSM.exe > out.txt
```
