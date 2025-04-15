#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include "commo.h"

namespace janus
{
<<<<<<< HEAD
  enum State
=======

#define HEARTBEAT_INTERVAL 100000

  enum Role
>>>>>>> 5495f15dab6c8e1f0ba7adc2365d78085bc194d6
  {
    FOLLOWER,
    CANDIDATE,
    LEADER
  };

<<<<<<< HEAD
=======
  struct LogEntry
  {
    uint64_t term;
    shared_ptr<Marshallable> command;
  };

>>>>>>> 5495f15dab6c8e1f0ba7adc2365d78085bc194d6
  class RaftServer : public TxLogServer
  {
  public:
    /* Your data here */
<<<<<<< HEAD
    uint64_t currentTerm = 0;
    uint64_t votedFor = -1;
    std::vector<shared_ptr<Marshallable>> commands;
    std::vector<uint64_t> terms;

    uint64_t commitLength;
    std::chrono::time_point<std::chrono::steady_clock> t_start = std::chrono::steady_clock::now();
    std::chrono::time_point<std::chrono::steady_clock> election_start_time = std::chrono::steady_clock::now();
    int electionTimeout;
    uint64_t startIndex = -1;

    State currentRole;
    uint64_t currentLeader;
    uint64_t timeout_val;
    std::unordered_set<uint64_t> votesReceived;
    std::unordered_map<uint64_t, uint64_t> nextIndex;
    std::unordered_map<uint64_t, uint64_t> matchLength;
    std::recursive_mutex m;

    /* Your functions here */
    void Init();
    void LeaderElection();
    void SendHeartBeat();
    void HeartBeatTimer();
    void ElectionTimer();
    void Simulation();
    void ReplicateLog(int followerId);

=======

    /* Your functions here */
    // Part 1A:-
    // Process an incoming Request Vote RPC.
    bool ProcessRequestVote(uint64_t candidateTerm, uint64_t candidateid, uint64_t &retTerm, bool_t &voteGranted);

    // Functions to manage election timeouts
    void ResetElectionDeadline();
    void ElectionTimerCoroutine();
    void StartElection();

    // Heartbeat coroutine for leader
    void HeartbeatCoroutine();

    void ReplicateLogEntry(uint64_t index);

    bool AppendLogEntry(shared_ptr<Marshallable> cmd);

  private:
    uint64_t currentTerm_ = 0;
    int votedFor_ = -1; // indicates no voite is given in current Term.
    Role role_ = FOLLOWER;
    uint64_t heartbeatInterval_ = 100000;    // 100ms in microseconds.
    uint64_t electionTimeoutBase_ = 800000;  // 400ms base timeout.
    uint64_t electionTimeoutRange_ = 400000; // up to 300ms extra.
    uint64_t electionDeadline_ = 0;          // Timestamp (in microseconds) when election timeout expires.

    // Vote Count for candidate used in the election
    int voteCount_ = 0;
    // Total number of peers(for now 3)
    int totalPeers_ = 5; // clusters of 5 nodes

    // log Container (part B)
    vector<LogEntry> log_;
    uint64_t commitIndex_ = 0;                          // index of the highest log entry known to be committed
    uint64_t lastApplied_ = 0;                          // index of last entry applied to the state machine
                                                        // Optionally, if you want to track progress for each follower:
    std::unordered_map<siteid_t, uint64_t> nextIndex_;  // for each follower, the next log index to send
    std::unordered_map<siteid_t, uint64_t> matchIndex_; // for each follower, highest replicated log index
>>>>>>> 5495f15dab6c8e1f0ba7adc2365d78085bc194d6
    /* do not modify this class below here */

  public:
    RaftServer(Frame *frame);
    ~RaftServer();

    bool Start(shared_ptr<Marshallable> &cmd, uint64_t *index, uint64_t *term);
    void GetState(bool *is_leader, uint64_t *term);

  private:
    bool disconnected_ = false;
    void Setup();

  public:
    void SyncRpcExample();
    void Disconnect(const bool disconnect = true);
    void Reconnect()
    {
      Disconnect(false);
    }
    bool IsDisconnected();

    virtual bool HandleConflicts(Tx &dtxn,
                                 innid_t inn_id,
                                 vector<string> &conflicts)
    {
      verify(0);
    };
    RaftCommo *commo()
    {
      return (RaftCommo *)commo_;
    }
  };
} // namespace janus
