// server.c - Multi-player Race Game Server
// Build: gcc -O2 -Wall -Wextra -pthread server.c -o server
// Run:   ./server 5555

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

// ============================================================
// GAME CONFIGURATION
// ============================================================
#define MAX_PLAYERS 5       // Maximum number of players allowed
#define MIN_PLAYERS 3       // Minimum players needed to start game
#define TARGET_POS  30      // Winning position

#define DEV_CLEAN_SHM 1     

// ============================================================
// DATA STRUCTURES
// ============================================================

// Player statistics tracked across games
typedef struct {
  int wins;             
  int games_played;      
  int total_rolls;       
  int traps_hit;         
  int total_distance;    
} PlayerStats;

typedef struct {
  int num_players;                    
  PlayerStats playerstats[MAX_PLAYERS]; 
  int player_wins[MAX_PLAYERS];       
  int positions[MAX_PLAYERS];        
  int connected[MAX_PLAYERS];         
  int active[MAX_PLAYERS];            
  int joined_players;               

  // === Game State ===
  int current_turn;       
  int game_active;         // Game started flag (0=lobby, 1=playing)
  int winner;              
  int round_id;            // Current round number
  int start_triggered;  
  int turn_finished;       
  int game_just_started;  
  time_t game_start_time;  

  // === Persistence ===
  int scores_dirty;        

  // === Synchronization (Process-Shared Mutexes) ===
  pthread_mutex_t shm_mutex; 

  // === Logger Data (Circular Buffer) ===
  pthread_mutex_t log_mutex;  
  char log_queue[20][128];    
  int log_head;               
  int log_tail;              
} SharedState;

// Server context (main process only)
typedef struct {
  int listen_fd;                    // Listening socket file descriptor
  pid_t child_pids[MAX_PLAYERS];    // Child process IDs (one per player)
  SharedState *st;                  // Pointer to shared memory
} ServerCtx;

// ============================================================
// SIGNAL HANDLERS
// ============================================================

// Global flag to stop server (set by Ctrl+C)
static volatile sig_atomic_t g_stop = 0;

// Handler for SIGINT (Ctrl+C)
static void on_sigint(int sig) {
  (void)sig;
  g_stop = 1;  // Signal server to stop
}

static void on_sigchld(int sig) {
  (void)sig;
  while (waitpid(-1, NULL, WNOHANG) > 0) {} 
}

// ============================================================
// SHARED MEMORY & SYNCHRONIZATION HELPERS
// ============================================================

// Uses PTHREAD_PROCESS_SHARED attribute
static int init_process_shared_mutex(pthread_mutex_t *m) {
  pthread_mutexattr_t attr;
  if (pthread_mutexattr_init(&attr) != 0) return -1;
  
  // Enable sharing across fork()ed processes
  if (pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED) != 0) {
    pthread_mutexattr_destroy(&attr);
    return -1;
  }
  
  int rc = pthread_mutex_init(m, &attr);
  pthread_mutexattr_destroy(&attr);
  return rc;
}

// Reset all shared state to initial values
static void shared_state_reset(SharedState *st) {
  memset(st, 0, sizeof(*st));
  st->winner = -1;           // No winner
  st->current_turn = 0;
  st->game_active = 0;       // Lobby mode
  st->num_players = 0;
  st->joined_players = 0;
  st->start_triggered = 0;
  st->turn_finished = 0;
  st->game_just_started = 0;
  st->game_start_time = 0;
  st->round_id = 0;
  st->scores_dirty = 0;

  for (int i = 0; i < MAX_PLAYERS; i++) {
    st->connected[i] = 0;
    st->active[i] = 0;
    st->positions[i] = 0;
    st->player_wins[i] = 0;
    memset(&st->playerstats[i], 0, sizeof(PlayerStats));
  }
}

// Create or open POSIX shared memory segment
static SharedState* shm_create_or_open(const char *name, int *is_new) {
  *is_new = 0;

  int fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0600);
  if (fd >= 0) {
    *is_new = 1;
    if (ftruncate(fd, (off_t)sizeof(SharedState)) != 0) {
      perror("ftruncate");
      close(fd);
      return NULL;
    }
  } else {
    // Memory already exists, open it
    if (errno != EEXIST) {
      perror("shm_open");
      return NULL;
    }
    fd = shm_open(name, O_RDWR, 0600);
    if (fd < 0) {
      perror("shm_open existing");
      return NULL;
    }
  }

  // Map shared memory into process address space
  void *p = mmap(NULL, sizeof(SharedState),
                 PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (p == MAP_FAILED) {
    perror("mmap");
    return NULL;
  }
  return (SharedState*)p;
}

// Count how many players are currently online
static int count_online_players(SharedState *st) {
  int online = 0;
  for (int i = 0; i < MAX_PLAYERS; i++) {
    if (st->connected[i] && st->active[i]) online++;
  }
  return online;
}

// Find first available player slot
static int find_free_slot(SharedState *st) {
  for (int i = 0; i < MAX_PLAYERS; i++) {
    if (st->connected[i] == 0 && st->active[i] == 0) return i;
  }
  return -1;  // All slots full
}

// ============================================================
// PERSISTENT SCORING (scores.txt)
// ============================================================

// Load scores from disk into shared memory
static void load_scores(SharedState *st) {
  FILE *fp = fopen("scores.txt", "r");
  if (!fp) {
    printf("[SCORES] No scores.txt found. Starting fresh.\n");
    return;
  }
  
  // Read scores for all players
  for (int i = 0; i < MAX_PLAYERS; i++) {
    fscanf(fp, "Player %*d: Wins=%d Games=%d Rolls=%d Traps=%d",
           &st->player_wins[i],
           &st->playerstats[i].games_played,
           &st->playerstats[i].total_rolls,
           &st->playerstats[i].traps_hit);
  }
  fclose(fp);
  printf("[SCORES] Loaded from scores.txt\n");
}

// Save current scores
static void save_scores(SharedState *st) {
  FILE *fp = fopen("scores.txt", "w");
  if (!fp) {
    perror("fopen scores.txt");
    return;
  }
  
  for (int i = 0; i < MAX_PLAYERS; i++) {
    fprintf(fp, "Player %d: Wins=%d Games=%d Rolls=%d Traps=%d\n",
            i + 1,
            st->player_wins[i],
            st->playerstats[i].games_played,
            st->playerstats[i].total_rolls,
            st->playerstats[i].traps_hit);
  }
  fclose(fp);
  printf("[SCORES] Saved to scores.txt\n");
}

// Save scores from a snapshot
static void save_scores_snapshot(const int wins[MAX_PLAYERS],
                                 const PlayerStats stats[MAX_PLAYERS]) {
  FILE *fp = fopen("scores.txt", "w");
  if (!fp) {
    perror("fopen scores.txt");
    return;
  }

  for (int i = 0; i < MAX_PLAYERS; i++) {
    fprintf(fp, "Player %d: Wins=%d Games=%d Rolls=%d Traps=%d\n",
            i + 1,
            wins[i],
            stats[i].games_played,
            stats[i].total_rolls,
            stats[i].traps_hit);
  }

  fclose(fp);
  printf("[SCORES] Saved to scores.txt\n");
}

// ============================================================
// LOGGER THREAD (Concurrent Logging)
// ============================================================

// Add message to circular log queue
static void write_log(SharedState *st, const char *msg) {
  pthread_mutex_lock(&st->log_mutex);

  int next_head = (st->log_head + 1) % 20;

  // If queue is full, drop oldest message
  if (next_head == st->log_tail) {
    st->log_tail = (st->log_tail + 1) % 20;
  }

  snprintf(st->log_queue[st->log_head], 128, "%s", msg);
  st->log_head = next_head;

  pthread_mutex_unlock(&st->log_mutex);
}

// Continuously writes queued log messages to game.log
static void* logger_thread_main(void *arg) {
  ServerCtx *ctx = (ServerCtx*)arg;
  SharedState *st = ctx->st;

  FILE *fp = fopen("game.log", "a");
  if (!fp) {
    perror("fopen game.log");
    return NULL;
  }

  printf("[LOGGER] Started. Writing to game.log\n");

  // Main logging loop
  while (!g_stop) {
    pthread_mutex_lock(&st->log_mutex);
    
    // Write all pending messages
    while (st->log_tail != st->log_head) {
      fprintf(fp, "%s\n", st->log_queue[st->log_tail]);
      st->log_tail = (st->log_tail + 1) % 20;
    }
    
    pthread_mutex_unlock(&st->log_mutex);

    fflush(fp);  // Ensure logs are written to disk
    usleep(100 * 1000);  // Sleep 100ms
  }

  // Flush remaining logs before exit
  pthread_mutex_lock(&st->log_mutex);
  while (st->log_tail != st->log_head) {
    fprintf(fp, "%s\n", st->log_queue[st->log_tail]);
    st->log_tail = (st->log_tail + 1) % 20;
  }
  pthread_mutex_unlock(&st->log_mutex);
  fflush(fp);

  fclose(fp);
  return NULL;
}

// ============================================================
// SCHEDULER THREAD (Round Robin Turn Management)
// ============================================================

// Scheduler thread main function
static void* scheduler_thread_main(void *arg) {
  ServerCtx *ctx = (ServerCtx*)arg;
  SharedState *st = ctx->st;

  while (!g_stop) {
    int need_save = 0;
    int wins_snap[MAX_PLAYERS];
    PlayerStats stats_snap[MAX_PLAYERS];

    pthread_mutex_lock(&st->shm_mutex);

    int online = count_online_players(st);

    // === RESET LOGIC: All players left ===
    if (st->game_active && online == 0) {
      st->game_active = 0;
      st->start_triggered = 0;
      st->num_players = 0;
      st->winner = -1;
      st->turn_finished = 0;
      st->game_just_started = 0;
      st->game_start_time = 0;
      for (int i = 0; i < MAX_PLAYERS; i++) st->positions[i] = 0;

      printf("[SCHEDULER] All players left. Reset to lobby.\n");
      write_log(st, "EVENT: Reset to lobby (all players left)");
    }

    // === START LOGIC: Begin new round ===
    if (!st->game_active && online >= MIN_PLAYERS && st->start_triggered == 1) {
      st->game_active = 1;
      st->game_just_started = 1;
      st->game_start_time = time(NULL);

      st->start_triggered = 0;
      st->round_id++;  // Increment round counter

      st->winner = -1;
      st->turn_finished = 0;
      for (int i = 0; i < MAX_PLAYERS; i++) st->positions[i] = 0;  // Reset positions

      // Find first active player for initial turn
      int start = 0;
      while (start < MAX_PLAYERS && st->active[start] == 0) start++;
      st->current_turn = (start < MAX_PLAYERS) ? start : 0;

      printf("[SCHEDULER] Round %d started (%d players)\n", st->round_id, online);
      char msg[96];
      snprintf(msg, sizeof(msg), "EVENT: Round %d started (%d players)", st->round_id, online);
      write_log(st, msg);
    }

    // === GAME LOGIC: Turn advancement and winner handling ===
    if (st->game_active && online > 0) {
      // Clear "game just started" flag after 2 seconds
      if (st->game_just_started && (time(NULL) - st->game_start_time) >= 2) {
        st->game_just_started = 0;
      }

      // === Winner detected: auto-reset after 3 seconds ===
      if (st->winner != -1) {
        int win = st->winner;

        printf("[SCHEDULER] Game ended (winner=P%d). Resetting in 3 sec...\n", win + 1);
        write_log(st, "EVENT: Game ended. Auto reset in 3 sec");

        st->scores_dirty = 1;  // Mark scores for saving

        pthread_mutex_unlock(&st->shm_mutex);
        sleep(3);
        pthread_mutex_lock(&st->shm_mutex);

        // Reset game state to lobby
        st->game_active = 0;
        st->turn_finished = 0;
        st->game_just_started = 0;
        st->game_start_time = 0;
        st->winner = -1;
        st->start_triggered = 0;
        for (int i = 0; i < MAX_PLAYERS; i++) st->positions[i] = 0;

        printf("[SCHEDULER] Lobby ready. Type 'start' to begin.\n");
        write_log(st, "EVENT: Lobby ready. Waiting for next start");
      } 
      // === Turn advancement (Round Robin) ===
      else if (st->turn_finished == 1 || st->active[st->current_turn] == 0) {
        int cur = st->current_turn;
        int next = (cur + 1) % MAX_PLAYERS;
        
        // Skip inactive players (disconnected)
        while (st->active[next] == 0 && next != cur) {
          next = (next + 1) % MAX_PLAYERS;
        }

        st->current_turn = next;
        st->turn_finished = 0;

        printf("[SCHEDULER] Turn -> Player %d\n", st->current_turn + 1);
        char msg[64];
        snprintf(msg, sizeof(msg), "EVENT: Turn advanced to Player %d", st->current_turn + 1);
        write_log(st, msg);
      }
    }

    if (st->scores_dirty) {
      for (int i = 0; i < MAX_PLAYERS; i++) {
        wins_snap[i] = st->player_wins[i];
        stats_snap[i] = st->playerstats[i];
      }
      st->scores_dirty = 0;
      need_save = 1;
    }

    pthread_mutex_unlock(&st->shm_mutex);

    if (need_save) {
      save_scores_snapshot(wins_snap, stats_snap);
    }

    usleep(200000);  // Sleep 200ms
  }
  return NULL;
}

// ============================================================
// GAME BOARD GENERATION
// ============================================================

// Generate visual representation of game board
// Layout: 6 rows x 5 columns, snake pattern (30 to 1)
static void generate_battlefield(SharedState *st, char *out, size_t outsz) {
  const int ROWS = 6;
  const int COLS = 5;
  const int MAX_POS = 30;
  int board_layout[6][5];

  // Fill board in snake pattern
  int current_val = MAX_POS;
  for (int r = 0; r < ROWS; r++) {
    if (r % 2 == 0) {
      // Even rows: left to right
      for (int c = 0; c < COLS; c++) board_layout[r][c] = current_val--;
    } else {
      // Odd rows: right to left (snake)
      for (int c = COLS - 1; c >= 0; c--) board_layout[r][c] = current_val--;
    }
  }

  size_t used = 0;
  used += snprintf(out + used, outsz - used, "\n--- CURRENT BATTLEFIELD ---\n");

  // Draw board
  for (int r = 0; r < ROWS; r++) {
    for (int c = 0; c < COLS; c++) {
      int cell_num = board_layout[r][c];
      char players_here[32] = "";
      int found = 0;

      // Check if any players on this cell
      for (int i = 0; i < MAX_PLAYERS; i++) {
        if (st->active[i] && st->positions[i] == cell_num) {
          char p_idx[8];
          snprintf(p_idx, sizeof(p_idx), "P%d", i + 1);
          strcat(players_here, p_idx);
          strcat(players_here, " ");
          found = 1;
        }
      }

      if (found) {
        // Show players on this cell
        used += snprintf(out + used, outsz - used, "[%-5s]", players_here);
      } else {
        // Show cell number (mark traps with !)
        if (cell_num == 5 || cell_num == 15 || cell_num == 25)
          used += snprintf(out + used, outsz - used, "[!%-3d!]", cell_num);
        else
          used += snprintf(out + used, outsz - used, "[%3d  ]", cell_num);
      }
    }
    used += snprintf(out + used, outsz - used, "\n");
  }

  used += snprintf(out + used, outsz - used, "---------------------------\n");
}

// ============================================================
// CLIENT SESSION HANDLER (Child Process)
// ============================================================

// Handle individual client session in forked child process
static void child_session(int player_idx, int sock_fd, SharedState *st) {
  // Seed random number generator (for dice rolls)
  srand((unsigned)time(NULL) ^ (unsigned)getpid() ^ (unsigned)(player_idx * 2654435761u));

  // Send welcome message
  dprintf(sock_fd, "WELCOME Player %d\n", player_idx + 1);
  dprintf(sock_fd, "INFO Target=%d\n", TARGET_POS);
  dprintf(sock_fd, "INFO Traps: 5, 15, 25 (move back 3)\n");
  dprintf(sock_fd, "INFO Commands: start | roll | board | state | q\n");

  char buf[256];

  // Client state tracking
  int last_round_seen = 0;
  int last_winner_seen = -1;
  int printed_game_over = 0;

  // Main client loop
  while (1) {
    // Use select() with timeout to periodically check game state
    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(sock_fd, &readfds);

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 200000;  // 200ms timeout

    int sel = select(sock_fd + 1, &readfds, NULL, NULL, &tv);

    // === Check for game events (broadcasts) ===
    pthread_mutex_lock(&st->shm_mutex);
    int rid = st->round_id;
    int ga = st->game_active;
    int w = st->winner;
    int online = count_online_players(st);
    int just = st->game_just_started;
    pthread_mutex_unlock(&st->shm_mutex);

    // Broadcast: Game start announcement
    if (ga && just && rid != last_round_seen) {
      dprintf(sock_fd, "\n========================================\n");
      dprintf(sock_fd, "ROUND %d STARTED\n", rid);
      dprintf(sock_fd, "%d players racing to %d\n", online, TARGET_POS);
      dprintf(sock_fd, "Use 'roll' when it's your turn.\n");
      dprintf(sock_fd, "========================================\n");
      last_round_seen = rid;
      printed_game_over = 0;
    }

    // Broadcast: Game over announcement
    if (w != -1 && !printed_game_over) {
      dprintf(sock_fd, "\n========================================\n");
      dprintf(sock_fd, "GAME OVER\n");
      dprintf(sock_fd, "Player %d WINS\n", w + 1);
      dprintf(sock_fd, "========================================\n");
      dprintf(sock_fd, "Type 'start' for next round, or 'q' to quit.\n");
      printed_game_over = 1;
      last_winner_seen = w;
    }

    // Broadcast: Lobby ready after auto-reset
    if (ga == 0 && w == -1 && last_winner_seen != -1) {
      dprintf(sock_fd, "\n[LOBBY] Ready. Type 'start' to begin a new round.\n");
      last_winner_seen = -1;
    }

    if (sel < 0) {
      if (errno == EINTR) continue;
      break;
    }
    if (sel == 0) continue;  // Timeout, no input

    // === Receive client command ===
    ssize_t n = recv(sock_fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) {
      // Client disconnected
      int online_now = 0;

      pthread_mutex_lock(&st->shm_mutex);
      st->connected[player_idx] = 0;
      st->active[player_idx] = 0;
      st->num_players = count_online_players(st);
      online_now = st->num_players;
      pthread_mutex_unlock(&st->shm_mutex);

      fprintf(stderr, "[DISCONNECT] player %d (online=%d)\n", player_idx + 1, online_now);
      fflush(stderr);

      char msg[80];
      snprintf(msg, sizeof(msg), "EVENT: Player %d Disconnected", player_idx + 1);
      write_log(st, msg);
      break;
    }

    buf[n] = '\0';
    buf[strcspn(buf, "\r\n")] = 0;

    int need_print_state = 0;
    int need_print_table = 0;

    // === Command: quit ===
    if (strcmp(buf, "q") == 0) {
      int online_now = 0;

      pthread_mutex_lock(&st->shm_mutex);
      st->connected[player_idx] = 0;
      st->active[player_idx] = 0;
      st->num_players = count_online_players(st);
      online_now = st->num_players;
      pthread_mutex_unlock(&st->shm_mutex);

      fprintf(stderr, "[QUIT] player %d (online=%d)\n", player_idx + 1, online_now);
      fflush(stderr);

      char msg[80];
      snprintf(msg, sizeof(msg), "EVENT: Player %d Quit (q)", player_idx + 1);
      write_log(st, msg);
      break;
    }

    // === Command: board ===
    if (strcmp(buf, "board") == 0) need_print_table = 1;
    
    // === Command: state ===
    if (strcmp(buf, "state") == 0) need_print_state = 1;

    // === Command: start ===
    if (strcmp(buf, "start") == 0) {
      pthread_mutex_lock(&st->shm_mutex);
      st->start_triggered = 1;  // Signal scheduler to start game
      pthread_mutex_unlock(&st->shm_mutex);

      dprintf(sock_fd, "LOBBY: Start signaled.\n");
      need_print_state = 1;
      write_log(st, "EVENT: Start signaled by a player");
    }

    // === Command: roll ===
    if (strcmp(buf, "roll") == 0) {
      pthread_mutex_lock(&st->shm_mutex);

      int ga2 = st->game_active;
      int turn = st->current_turn;
      int win = st->winner;

      if (!ga2) {
        // Game not started
        pthread_mutex_unlock(&st->shm_mutex);
        dprintf(sock_fd, "INFO: Game not started. Type 'start'.\n");
        need_print_state = 1;
      } else if (win != -1) {
        // Game already ended
        pthread_mutex_unlock(&st->shm_mutex);
        dprintf(sock_fd, "INFO: Game ended. Type 'start' for next round.\n");
        need_print_state = 1;
      } else if (turn != player_idx) {
        // Not this player's turn
        pthread_mutex_unlock(&st->shm_mutex);
        dprintf(sock_fd, "INFO: Not your turn. Current turn=Player %d\n", turn + 1);
        need_print_state = 1;
      } else {
        
        // Roll dice (1-6)
        int r = (rand() % 6) + 1;
        st->positions[player_idx] += r;
        st->playerstats[player_idx].total_rolls++;

        // === TRAP LOGIC ===
        int hit_trap = 0;
        int current_pos = st->positions[player_idx];
        if (current_pos == 5 || current_pos == 15 || current_pos == 25) {
          st->positions[player_idx] -= 3;  // Move back 3
          if (st->positions[player_idx] < 0) st->positions[player_idx] = 0;
          hit_trap = 1;
          st->playerstats[player_idx].traps_hit++;  // Only increment if trap hit
        }

        // === WIN CONDITION ===
        if (st->positions[player_idx] >= TARGET_POS) {
          st->positions[player_idx] = TARGET_POS;
          st->winner = player_idx;
          st->player_wins[player_idx]++;

          // Update games_played for all active players
          for (int i = 0; i < MAX_PLAYERS; i++) {
            if (st->active[i]) st->playerstats[i].games_played++;
          }

          st->scores_dirty = 1;  // Mark for saving
        }

        int final_pos = st->positions[player_idx];
        int winner_now = st->winner;

        st->turn_finished = 1;  // Signal turn complete
        pthread_mutex_unlock(&st->shm_mutex);

        // Send result to client
        dprintf(sock_fd, "ACTION: You rolled %d. ", r);
        if (hit_trap) dprintf(sock_fd, "TRAP! Back 3. ");
        dprintf(sock_fd, "Position: %d\n", final_pos);

        // Log event
        char msg[128];
        if (winner_now == player_idx) {
          snprintf(msg, sizeof(msg), "EVENT: Player %d rolled %d and WON", player_idx + 1, r);
        } else if (hit_trap) {
          snprintf(msg, sizeof(msg), "EVENT: Player %d rolled %d, hit TRAP, pos=%d", player_idx + 1, r, final_pos);
        } else {
          snprintf(msg, sizeof(msg), "EVENT: Player %d rolled %d (pos=%d)", player_idx + 1, r, final_pos);
        }
        write_log(st, msg);

        need_print_table = 1;
        need_print_state = 1;
      }
    }

    // === Invalid command ===
    if (strcmp(buf, "board") != 0 &&
        strcmp(buf, "state") != 0 &&
        strcmp(buf, "start") != 0 &&
        strcmp(buf, "roll") != 0 &&
        strcmp(buf, "q") != 0) {
      dprintf(sock_fd, "INFO: Commands: start | roll | board | state | q\n");
      need_print_state = 1;
    }

    // === Print board if requested ===
    if (need_print_table) {
      char board_buf[4096];
      pthread_mutex_lock(&st->shm_mutex);
      generate_battlefield(st, board_buf, sizeof(board_buf));
      pthread_mutex_unlock(&st->shm_mutex);
      dprintf(sock_fd, "%s", board_buf);
    }

    // === Print state if requested ===
    if (need_print_state) {
      pthread_mutex_lock(&st->shm_mutex);
      int rid2 = st->round_id;
      int turn2 = st->current_turn;
      int online2 = count_online_players(st);
      int ga3 = st->game_active;
      int win3 = st->winner;
      int wins = st->player_wins[player_idx];
      pthread_mutex_unlock(&st->shm_mutex);

      dprintf(sock_fd,
              "STATE round=%d game_active=%d winner=%d turn=%d online=%d wins=%d\n",
              rid2, ga3, (win3 == -1 ? 0 : win3 + 1), turn2 + 1, online2, wins);
    }
  }

  dprintf(sock_fd, "BYE Player %d\n", player_idx + 1);
  close(sock_fd);
  _exit(0);  // Exit child process
}

// ============================================================
// NETWORKING HELPER
// ============================================================

static int make_listen_socket(int port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) { perror("socket"); return -1; }

  // Allow address reuse
  int yes = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);  // Listen on all interfaces
  addr.sin_port = htons((uint16_t)port);

  if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
    perror("bind");
    close(fd);
    return -1;
  }
  if (listen(fd, 16) != 0) {
    perror("listen");
    close(fd);
    return -1;
  }
  return fd;
}

// ============================================================
// MAIN FUNCTION
// ============================================================

int main(int argc, char **argv) {
  // Check arguments
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <port>\n", argv[0]);
    return 1;
  }

  int port = atoi(argv[1]);
  if (port <= 0) {
    fprintf(stderr, "Invalid port.\n");
    return 1;
  }

  signal(SIGPIPE, SIG_IGN);

  // === Setup signal handlers ===
  struct sigaction sa_int = (struct sigaction){0};
  struct sigaction sa_chld = (struct sigaction){0};

  sa_int.sa_handler = on_sigint;
  sigaction(SIGINT, &sa_int, NULL);

  sa_chld.sa_handler = on_sigchld;
  sa_chld.sa_flags = SA_RESTART | SA_NOCLDSTOP;
  sigaction(SIGCHLD, &sa_chld, NULL);

  // === Create or open shared memory ===
  const char *SHM_NAME = "/csn6214_game_shm";
  int is_new = 0;
  SharedState *st = shm_create_or_open(SHM_NAME, &is_new);
  if (!st) return 1;

  if (is_new) {
    // New shared memory: initialize everything
    shared_state_reset(st);
    if (init_process_shared_mutex(&st->shm_mutex) != 0) {
      perror("pthread_mutex_init(pshared)");
      return 1;
    }
    if (init_process_shared_mutex(&st->log_mutex) != 0) {
      perror("pthread_mutex_init(log)");
      return 1;
    }
    load_scores(st);  // Load scores from disk
  } else {
    // Existing memory: reset runtime state, keep scores
    pthread_mutex_lock(&st->shm_mutex);
    st->game_active = 0;
    st->winner = -1;
    st->current_turn = 0;
    st->num_players = 0;
    st->joined_players = 0;
    st->start_triggered = 0;
    st->turn_finished = 0;
    st->game_just_started = 0;
    st->game_start_time = 0;
    st->round_id = 0;
    st->scores_dirty = 0;
    for (int i = 0; i < MAX_PLAYERS; i++) {
      st->connected[i] = 0;
      st->active[i] = 0;
      st->positions[i] = 0;
    }
    pthread_mutex_unlock(&st->shm_mutex);

    pthread_mutex_lock(&st->log_mutex);
    st->log_head = 0;
    st->log_tail = 0;
    pthread_mutex_unlock(&st->log_mutex);

    load_scores(st);  // Reload scores from disk
  }

  int listen_fd = make_listen_socket(port);
  if (listen_fd < 0) return 1;

  // === Initialize server context ===
  ServerCtx ctx;
  memset(&ctx, 0, sizeof(ctx));
  ctx.listen_fd = listen_fd;
  ctx.st = st;
  for (int i = 0; i < MAX_PLAYERS; i++) ctx.child_pids[i] = -1;

  // === Create threads ===
  pthread_t th_logger, th_sched;
  
  // Logger thread
  if (pthread_create(&th_logger, NULL, logger_thread_main, &ctx) != 0) {
    perror("pthread_create(logger)");
    return 1;
  }
  
  // Scheduler thread
  if (pthread_create(&th_sched, NULL, scheduler_thread_main, &ctx) != 0) {
    perror("pthread_create(scheduler)");
    return 1;
  }

  printf("Server listening on port %d. Ctrl+C to stop.\n", port);

  // === Main accept loop ===
  while (!g_stop) {
    struct sockaddr_in cli;
    socklen_t len = sizeof(cli);

    // Accept new client connection
    int cfd = accept(listen_fd, (struct sockaddr*)&cli, &len);
    if (cfd < 0) {
      if (errno == EINTR) continue;  // Interrupted by signal
      perror("accept");
      break;
    }

    // Find free player slot
    pthread_mutex_lock(&st->shm_mutex);
    int idx = find_free_slot(st);
    pthread_mutex_unlock(&st->shm_mutex);

    if (idx < 0) {
      // Server full
      dprintf(cfd, "SERVER FULL (max %d)\n", MAX_PLAYERS);
      close(cfd);
      continue;
    }

    printf("Client connected: player %d\n", idx + 1);

    // === Fork child process for this client ===
    pid_t pid = fork();
    if (pid < 0) {
      perror("fork");
      close(cfd);
      continue;
    }

    if (pid == 0) {
      // === CHILD PROCESS ===
      close(listen_fd); 
      child_session(idx, cfd, st);  // Handle client
    } else {
      // === PARENT PROCESS ===
      ctx.child_pids[idx] = pid;
      close(cfd); 

      // Update shared state
      pthread_mutex_lock(&st->shm_mutex);
      st->connected[idx] = 1;
      st->active[idx] = 1;
      st->joined_players += 1;
      int online = count_online_players(st);
      st->num_players = online;
      pthread_mutex_unlock(&st->shm_mutex);

      fprintf(stderr, "[CONNECT] player %d (online=%d)\n", idx + 1, online);
      fflush(stderr);

      char msg[64];
      snprintf(msg, sizeof(msg), "EVENT: Player %d Connected", idx + 1);
      write_log(st, msg);
    }
  }

  // === Shutdown sequence ===
  g_stop = 1;

  // Wait for threads to finish
  pthread_join(th_sched, NULL);
  pthread_join(th_logger, NULL);

  // Terminate all child processes
  for (int i = 0; i < MAX_PLAYERS; i++) {
    if (ctx.child_pids[i] > 0) kill(ctx.child_pids[i], SIGTERM);
  }
  for (int i = 0; i < MAX_PLAYERS; i++) {
    if (ctx.child_pids[i] > 0) waitpid(ctx.child_pids[i], NULL, 0);
  }

  close(listen_fd);

  // Save final scores
  save_scores(st);

  munmap(st, sizeof(*st));

  // Remove shared memory (if DEV_CLEAN_SHM enabled)
  if (DEV_CLEAN_SHM) {
    shm_unlink(SHM_NAME);
  }

  printf("Server stopped.\n");
  return 0;
}