===============================
 Multiplayer Board Game Server
 README.txt
===============================

1. HOW TO COMPILE AND RUN
-------------------------

This project uses Makefile for compilation.

Step 1 – Compile server and client:
-----------------------------------
To rebuild from scratch:
make clean
make

-----------------------------------

This will generate two executables:
- server
- client

Step 2 – Run the server:
------------------------
./server 5555

Step 3 – Run clients (in separate terminals):
---------------------------------------------
./client 127.0.0.1 5555


2. EXAMPLE COMMANDS
-------------------

After connecting, players can use the following commands:

start   → Signal game start  
roll    → Roll dice during your turn  
board   → Display battlefield board  
state   → Show game state  
q       → Quit the game  

Example gameplay flow:

Player 1:
start

Player turn:
roll

View board:
board

Check state:
state

Exit:
q


3. GAME RULES SUMMARY
---------------------

* Minimum players required: 3  
* Maximum players supported: 5  

* Players take turns rolling a dice (1–6).  
* Each roll advances player position.  
* First player to reach position 30 wins.  

Trap Cells:
-----------
Cells 5, 15, and 25 are traps.

If a player lands on a trap:
→ Move back 3 positions.

Winning:
--------
* When a player reaches position 30:
  - Game ends immediately.
  - Winner is announced to all players.
  - Scores are saved.

Multi-Round Support:
--------------------
* Server auto-resets to lobby after a round.
* Players can start a new round without restarting server.


4. MODE SUPPORTED
-----------------

This system supports:

✔ Multi-process architecture (fork per client)  
✔ Multi-threaded server services  
✔ TCP socket communication  
✔ POSIX shared memory for IPC  
✔ Process-shared mutex synchronization  

Deployment Mode:
----------------
* Network mode (TCP client-server)
* Multiple clients from same or different machines supported

Persistence:
------------
* scores.txt → Stores player statistics
* game.log   → Stores game event logs