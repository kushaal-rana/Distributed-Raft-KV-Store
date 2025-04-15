#include "server.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"
#include <chrono>
#include <cstdlib>
#include <unistd.h>
#include "rrr.hpp"
#include "raft_rpc.h"

#undef RAFT_TEST_CORO // <-- explicitly ensure it's not defined
#include "macros.h"   // <-- now macros.h won't cause issues
using namespace std::chrono;

namespace janus
{

  RaftServer::RaftServer(Frame *frame)
  {
    frame_ = frame;
    /* Your code here for server initialization. Note that this function is
       called in a different OS thread. Be careful about thread safety if
       you want to initialize variables here. */
<<<<<<< HEAD
  }

  RaftServer::~RaftServer()
  {
    /* Your code here for server teardown */
  }

  void RaftServer::Setup()
  {
    /* Your code here for server setup. Due to the asynchronous nature of the
       framework, this function could be called after a RPC handler is triggered.
       Your code should be aware of that. This function is always called in the
       same OS thread as the RPC handlers. */
    Simulation();
  }

  void RaftServer::Simulation()
  {
    Coroutine::CreateRun([this]()
                         {
    uint64_t prevFTerm = -1, prevCTerm = -1, prevLterm = -1, logCall = -1;
    while (true) {
      if (currentRole == FOLLOWER && prevFTerm != currentTerm) {
        prevFTerm = currentTerm;
        HeartBeatTimer();
      }
      
      else if (currentRole == CANDIDATE && prevCTerm != currentTerm) {
        prevCTerm = currentTerm;
        ElectionTimer();
        LeaderElection();
      } 
      Coroutine::Sleep(40000);  
    } });
  }

  void RaftServer::SyncRpcExample()
  {
    /* This is an example of synchronous RPC using coroutine; feel free to
       modify this function to dispatch/receive your own messages.
       You can refer to the other function examples in commo.h/cc on how
       to send/recv a Marshallable object over RPC. */
    Coroutine::CreateRun([this]()
                         {
    string res;
    auto event = commo()->SendString(0, /* partition id is always 0 for lab1 */
                                     2, "hello", &res);

    event->Wait(1000000); //timeout after 1000000us=1s
    if (event->status_ == Event::TIMEOUT) {
      Log_info("timeout happens");
    } else {
      Log_info("rpc response is: %s", res.c_str());
    } });
  }

  void RaftServer::LeaderElection()
  {
    m.lock();
    votesReceived.clear();
    votesReceived.insert(site_id_);
    m.unlock();
    for (int serverId = 0; serverId < 5 && currentRole == CANDIDATE; serverId++)
    {
      if (serverId != site_id_)
      {
        auto callback = [&]()
        {
          uint64_t retTerm, cTerm = currentTerm, candidateId = site_id_, svrId = serverId;
          bool_t vote_granted;

          // Log_info("[SendRequestVote] (cId, svrId, term) = (%d, %d, %d)\n", candidateId, serverId, currentTerm);
          uint64_t logLength = terms.size();
          uint64_t logTerm = logLength > 0 ? terms[logLength - 1] : 0;

          // Call to communication Class to send RequestVote RPC
          auto event = commo()->SendRequestVote(0, svrId, candidateId, cTerm, logTerm, logLength, &retTerm, &vote_granted);

          event->Wait(40000); // timeout
          if (event->status_ != Event::TIMEOUT)
          {
            m.lock();
            // Log_info("[ReceiveRequestVote] : (cId, sId, rTerm, vote_granted) -> (%d, %d, %d, %d)]", site_id_, svrId, retTerm, vote_granted);
            if (currentRole == CANDIDATE && vote_granted && retTerm == currentTerm)
            {
              votesReceived.insert(svrId);

              if (votesReceived.size() == 3)
              {
                currentLeader = site_id_;
                currentRole = LEADER;
                timeout_val = 0;

                // Log_info("New Leader: %d, matchLength: %d, commitLength: %d", site_id_, matchLength[site_id_], commitLength);
                for (int fId = 0; fId < 5; fId++)
                {
                  if (fId == site_id_)
                    continue;
                  nextIndex[fId] = commands.size();
                  matchLength[fId] = 0;
                  ReplicateLog(fId);
                }

                matchLength[site_id_] = terms.size();
              }
            }
            else if (retTerm > currentTerm)
            {
              currentTerm = retTerm;
              currentRole = FOLLOWER;
              votedFor = -1;
            }
            m.unlock();
          }
        };
        Coroutine::CreateRun(callback);
      }
    }
  }

  void RaftServer::ElectionTimer()
  {
    auto callback = [&]()
    {
      m.lock();
      electionTimeout = 1000 + site_id_ * 500;
      election_start_time = std::chrono::steady_clock::now();
      m.unlock();

      while (currentRole == CANDIDATE && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - election_start_time).count() < std::chrono::milliseconds(electionTimeout).count())
      {
        Coroutine::Sleep(electionTimeout * 150);
      }

      m.lock();
      if (currentRole == CANDIDATE)
      {
        currentTerm += 1;
        votedFor = site_id_;
      }
      m.unlock();
    };
    Coroutine::CreateRun(callback);
  }

  void RaftServer::HeartBeatTimer()
  {
    auto callback = [&]()
    {
      // auto random_timeout = getRandom(1000, 1500);
      auto random_timeout = 1000 + 125 * site_id_;
      auto timeout = std::chrono::milliseconds(random_timeout);
      t_start = std::chrono::steady_clock::now();

      while (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t_start).count() < timeout.count())
      {
        Coroutine::Sleep(random_timeout * 75);
      }

      // Log_info("HeartBeat Timeout %d for server: %d", random_timeout, site_id_);

      m.lock();
      // Log_info("CurrentRole: %d, votedFor: %d", currentRole == FOLLOWER, votedFor);
      if (currentRole == FOLLOWER)
      {
        currentTerm += 1;
        currentRole = CANDIDATE;
        votedFor = site_id_;
        // Log_info("Server %d is promoted to a candidate, term: %d", site_id_, currentTerm);
      }
      m.unlock();
    };
    Coroutine::CreateRun(callback);
=======
    ResetElectionDeadline();
  }

  // Helper function: Get current time in microseconds.
  uint64_t GetCurrentTimeMicros()
  {
    auto now = steady_clock::now();
    return duration_cast<microseconds>(now.time_since_epoch()).count();
  }

  // Reset the election deadline using a randomized offset.
  void RaftServer::ResetElectionDeadline()
  {
    uint64_t now = GetCurrentTimeMicros();
    uint64_t randomOffset = rand() % electionTimeoutRange_;
    electionDeadline_ = now + electionTimeoutBase_ + randomOffset;
  }

  // Process an incoming RequestVote RPC.
  bool RaftServer::ProcessRequestVote(uint64_t candidateTerm, uint64_t candidateId, uint64_t &retTerm, bool_t &voteGranted)
  {
    lock_guard<recursive_mutex> lock(mtx_);
    if (candidateTerm < currentTerm_)
    {
      // Candidate's term is outdated
      retTerm = currentTerm_;
      voteGranted = false;
      return false;
    }
    if (candidateTerm > currentTerm_)
    {
      // Update to the new Term and Step down to Followers
      currentTerm_ = candidateTerm;
      role_ = FOLLOWER;
      votedFor_ = -1;
    }
    // If we haven't voted yet or already voted for this candidate
    if (votedFor_ == -1 || votedFor_ == candidateId)
    {
      votedFor_ = candidateId;
      voteGranted = true;
    }
    else
    {
      voteGranted = false;
    }
    retTerm = currentTerm_;
    Log_info("S%d's term=%lu: got RequestVote from C%d, candTerm=%lu -> granting=%d",
             loc_id_, currentTerm_, candidateId, candidateTerm, (int)voteGranted);

    // Reset Election TimeOut on Granting(or even denying) a vote
    ResetElectionDeadline();
    return voteGranted;
  }

  // Election timer coroutine: Check periodically whether an election should be triggered.

  void RaftServer::ElectionTimerCoroutine()
  {
    Coroutine::CreateRun([this]()
                         {
      while (true)
      {
      {

        lock_guard<recursive_mutex> lock(mtx_);
        uint64_t now = GetCurrentTimeMicros();
        if (now >= electionDeadline_ && role_ != LEADER)
        {
          // Election timeout reached, start a new election
          StartElection();
        }
      }
      usleep(50000); // Sleep for 50ms to avoid busy waiting
      } });
  }

  // Start election procedure: Become candidate, increment term, vote for self and send RPCs.

  void RaftServer::StartElection()
  {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    role_ = CANDIDATE;
    currentTerm_++;
    votedFor_ = loc_id_;
    voteCount_ = 1; // Vote for self
    // Reset election deadline
    ResetElectionDeadline();
    Log_info("Server %d starts election for term %lu", loc_id_, currentTerm_);
    // Send RequestVote RPCs to all other servers
    // Assume commo()->rpc_par_proxies_ holds {peer_id, proxy} pairs.
    auto proxies = commo()->rpc_par_proxies_[0];
    for (auto &p : proxies)
    {
      auto peer_id = p.first;
      if (peer_id == loc_id_)
      {
        continue; // Skip self
      }
      // Send RequestVote RPC to peer_id
      // For each peer, send the RequestVote.
      // We define a local helper to capture 'this'.
      RaftProxy *proxy = (RaftProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [this](Future *fu)
      {
        // This callback is called when the RPC completes
        uint64_t retTerm;
        int temp;
        fu->get_reply() >> retTerm;
        fu->get_reply() >> temp;

        bool_t voteGranted = (temp != 0);
        // Process the vote reply
        lock_guard<recursive_mutex> lock(mtx_);
        if (retTerm > currentTerm_)
        {
          // Out Term is outdated
          currentTerm_ = retTerm;
          role_ = FOLLOWER;
          votedFor_ = -1;
          return;
        }
        if (role_ != CANDIDATE)
        {
          // We are not a candidate anymore
          return;
        }
        if (voteGranted)
        {
          voteCount_++;
          // If vote count is more than half of total peers:
          if (voteCount_ > totalPeers_ / 2)
          {
            // We have enough votes, become leader
            role_ = LEADER;
            Log_info("Server %d becomes leader for term %lu", loc_id_, currentTerm_);
            // Start sending heartbeats.
            HeartbeatCoroutine();
          }
        }
        Log_info("S%d in term=%lu got vote reply from peer: retTerm=%lu, granted=%d",
                 loc_id_, currentTerm_, retTerm, (int)voteGranted);
      };
      // Use Call_Async from the RPC framework.
      Call_Async(proxy, RequestVote, currentTerm_, (uint64_t)loc_id_, fuattr);
    }
  }

  // Heartbeat coroutine: Leader sends empty AppendEntries RPCs periodically.

  void RaftServer::HeartbeatCoroutine()
  {
    Coroutine::CreateRun([this]()
                         {
    while(true){
      {
        lock_guard<recursive_mutex> lock(mtx_);
        if(role_ != LEADER){
          return; // Stop sending heartbeats if not leader
        }
        // Send heartbeats to all followers
        auto proxies = commo()->rpc_par_proxies_[0];
        for (auto& p : proxies) {
          auto peer_id = p.first;
          if (peer_id == loc_id_) {
            continue; // Skip self
          }
          // For heartbeat, we send an empty command.
          std::shared_ptr<Marshallable> heartbeatCmd = nullptr;
          commo()->SendAppendEntries(0, peer_id, heartbeatCmd);        
          }
      }
      usleep(heartbeatInterval_); // Sleep 100ms.
    } });
  }

  void RaftServer::Setup()
  {
    /* Your code here for server setup. Due to the asynchronous nature of the
       framework, this function could be called after a RPC handler is triggered.
       Your code should be aware of that. This function is always called in the
       same OS thread as the RPC handlers. */
    SyncRpcExample();
    ElectionTimerCoroutine(); // Start the election timer coroutine
>>>>>>> 5495f15dab6c8e1f0ba7adc2365d78085bc194d6
  }

  bool RaftServer::Start(shared_ptr<Marshallable> &cmd,
                         uint64_t *index,
                         uint64_t *term)
  {
<<<<<<< HEAD
    /* Your code here. This function can be called from another OS thread. */
    m.lock();
    if (currentRole == LEADER)
    {
      *term = currentTerm;
      commands.push_back(cmd);
      terms.push_back(currentTerm);
      *index = terms.size();
      startIndex++;
      // Log_info("Start For Server %d is called", site_id_);
      m.unlock();
      return true;
    }

    m.unlock();
    return false;
  }

  void RaftServer::ReplicateLog(int followerID)
  {
    auto callback = [followerID, this]()
    {
      while (currentRole == LEADER)
      {
        m.lock();
        uint64_t retTerm, ackLength, cTerm = currentTerm;
        bool_t success;

        uint64_t prefixLength = nextIndex[followerID];
        uint64_t prevLogTerm = prefixLength > 0 ? terms[prefixLength - 1] : 0;
        std::vector<shared_ptr<Marshallable>> suffix_commands(commands.begin() + prefixLength, commands.end());
        std::vector<uint64_t> suffix_terms(terms.begin() + prefixLength, terms.end());
        m.unlock();

        auto event = commo()->SendAppendEntries(0, followerID, site_id_, cTerm, prefixLength, prevLogTerm, suffix_commands, suffix_terms, commitLength, &retTerm, &ackLength, &success);
        event->Wait(40000);
        if (event->status_ != Event::TIMEOUT)
        {
          m.lock();
          // Log_info("[VoteResponse] fId: %d, cTerm: %d, retTerm: %d", followerID, cTerm, retTerm);
          timeout_val = 0;
          if (success == false && ackLength == -1)
          {
            currentTerm = retTerm;
            currentRole = FOLLOWER;
            votedFor = -1;
            // Log_info("[Leader-->Follower] retTerm > cTerm for %d", followerID);
            m.unlock();
            break;
          }

          else if (currentRole == LEADER)
          {
            if (success)
            {
              // Log_info("%d responded successfully", followerID);
              nextIndex[followerID] = ackLength;

              matchLength[followerID] = ackLength;

              std::vector<uint64_t> acksReceived;
              // std::cout << "MatchLength: ";
              matchLength[site_id_] = terms.size();
              for (int fsz = 0; fsz < 5; fsz++)
              {
                // std::cout << matchLength[fsz] << " ";
                if (matchLength[fsz] > 0)
                  acksReceived.push_back(matchLength[fsz]);
              }
              // std::cout << std::endl;
              // Log_info("Size of acksReceived arr of leader: %d is: %d", site_id_, acksReceived.size());
              if (acksReceived.size() >= 3)
              {
                sort(acksReceived.begin(), acksReceived.end());

                int pos = -1;
                int minSize = acksReceived[0], maxSize = acksReceived[acksReceived.size() - 3];

                // Log_info("Min_size: %d, max_size: %d, currentTerm: %d", minSize, maxSize, currentTerm);
                for (int len = minSize; len <= maxSize; len++)
                {
                  if (len > commitLength && terms[len - 1] == currentTerm)
                  {
                    pos = len;
                  }
                }

                // Log_info("Pos: %d", pos);
                if (pos != -1)
                {
                  for (int i = commitLength; i < pos; i++)
                  {
                    app_next_(*commands[i]);
                    // Log_info("[Leader: %d] app_next_ called for index: %d", site_id_, i);
                  }
                  commitLength = pos;
                  // Log_info("Committing at Server: %d, commitSize: %d", site_id_, commitLength);
                }
              }
            }
            else if (nextIndex[followerID] > 0)
            {
              nextIndex[followerID]--;
              Coroutine::Sleep(30000);
              m.unlock();
              continue;
            }
          }
        }
        else
        {
          // Log_info("Timeout for %d, leader: %d", followerID, site_id_);
          matchLength[followerID] = 0;
          timeout_val = timeout_val + 1 < 4 ? timeout_val + 1 : 4;
        }
        m.unlock();
        Coroutine::Sleep(125000);
      }
    };
    Coroutine::CreateRun(callback);
  }
  void RaftServer::GetState(bool *is_leader, uint64_t *term)
  {
    /* Your code here. This function can be called from another OS thread. */

    timeout_val = 0;
    *term = currentTerm;
    *is_leader = currentRole == LEADER && timeout_val < 4;
=======
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    if (role_ != LEADER)
    {
      return false;
    }
    // Append the new command to the log
    LogEntry entry;
    entry.term = currentTerm_;
    entry.command = cmd;
    log_.push_back(entry);
    // Use 1-indexing for log entries
    *index = log_.size();
    *term = currentTerm_;
    // Optionally initialize nextIndex_ and matchIndex_ for each follower if not yet set:
    // (Assuming totalPeers_ is the number of nodes; you may initialize these in the constructor.)

    // Begin replicating the new log entry to the followers.
    ReplicateLogEntry(*index);
    return true;
  }

  void RaftServer::ReplicateLogEntry(uint64_t index)
  {
    // We'll count successes; include self as a successful replication.
    int successCount = 1;
    // Get the new entry
    LogEntry entry = log_[index - 1]; // assuming 1-indexing
    // Send the AppendEntries RPC with the new log entry to all followers
    auto proxies = commo()->rpc_par_proxies_[0]; // assuming partition 0
    for (auto &p : proxies)
    {
      if (p.first == loc_id_)
        continue;
      RaftProxy *proxy = (RaftProxy *)p.second;
      FutureAttr fuattr;
      // Capture necessary values by value; use a lambda for the callback:
      fuattr.callback = [this, index, &successCount](Future *fu)
      {
        int temp;
        fu->get_reply() >> temp;
        bool_t followerAppendOK = (temp != 0);
        {
          std::lock_guard<std::recursive_mutex> lock(mtx_);
          if (followerAppendOK)
          {
            successCount++;
            // If majority has successfully replicated, commit the entry.
            if (successCount > totalPeers_ / 2 && index > commitIndex_)
            {
              commitIndex_ = index;
              // Apply the command exactly once (via app_next_)
              if (app_next_ && index > lastApplied_)
              {
                app_next_(*log_[index - 1].command);
                lastApplied_ = index;
              }
            }
          }
        }
      };
      // Here, we wrap the log entry in a MarshallDeputy (so it can be serialized)
      MarshallDeputy md(entry.command);
      Call_Async(proxy, AppendEntries, md, fuattr);
    }
  }

  bool RaftServer::AppendLogEntry(shared_ptr<Marshallable> cmd)
  {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    // Here, you would typically check for consistency with the previous log entry.
    // For a simple initial implementation, we simply append.
    LogEntry entry;
    entry.term = currentTerm_;
    entry.command = cmd;
    log_.push_back(entry);
    return true; // Indicate that the entry was appended.
  }

  void RaftServer::GetState(bool *is_leader, uint64_t *term)
  {
    /* Your code here. This function can be called from another OS thread. */
    *is_leader = 0;
    *term = 0;
  }

  void RaftServer::SyncRpcExample()
  {
    /* This is an example of synchronous RPC using coroutine; feel free to
       modify this function to dispatch/receive your own messages.
       You can refer to the other function examples in commo.h/cc on how
       to send/recv a Marshallable object over RPC. */
    Coroutine::CreateRun([this]()
                         {
    string res;
    auto event = commo()->SendString(0, /* partition id is always 0 for lab1 */
                                     0, "hello", &res);
    event->Wait(1000000); //timeout after 1000000us=1s
    if (event->status_ == Event::TIMEOUT) {
      Log_info("timeout happens");
    } else {
      Log_info("rpc response is: %s", res.c_str()); 
    } });
  }

  RaftServer::~RaftServer()
  {
    /* Your code here for server teardown */
>>>>>>> 5495f15dab6c8e1f0ba7adc2365d78085bc194d6
  }

  /* Do not modify any code below here */

  void RaftServer::Disconnect(const bool disconnect)
  {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    verify(disconnected_ != disconnect);
    // global map of rpc_par_proxies_ values accessed by partition then by site
    static map<parid_t, map<siteid_t, map<siteid_t, vector<SiteProxyPair>>>> _proxies{};
    if (_proxies.find(partition_id_) == _proxies.end())
    {
      _proxies[partition_id_] = {};
    }
    RaftCommo *c = (RaftCommo *)commo();
    if (disconnect)
    {
      verify(_proxies[partition_id_][loc_id_].size() == 0);
      verify(c->rpc_par_proxies_.size() > 0);
      auto sz = c->rpc_par_proxies_.size();
      _proxies[partition_id_][loc_id_].insert(c->rpc_par_proxies_.begin(), c->rpc_par_proxies_.end());
      c->rpc_par_proxies_ = {};
      verify(_proxies[partition_id_][loc_id_].size() == sz);
      verify(c->rpc_par_proxies_.size() == 0);
    }
    else
    {
      verify(_proxies[partition_id_][loc_id_].size() > 0);
      auto sz = _proxies[partition_id_][loc_id_].size();
      c->rpc_par_proxies_ = {};
      c->rpc_par_proxies_.insert(_proxies[partition_id_][loc_id_].begin(), _proxies[partition_id_][loc_id_].end());
      _proxies[partition_id_][loc_id_] = {};
      verify(_proxies[partition_id_][loc_id_].size() == 0);
      verify(c->rpc_par_proxies_.size() == sz);
    }
    disconnected_ = disconnect;
  }

  bool RaftServer::IsDisconnected()
  {
    return disconnected_;
  }

} // namespace janus
