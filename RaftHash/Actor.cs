using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Diagnostics;
using Nito.AsyncEx;

namespace RaftHash
{
    internal class Actor
    {
        internal enum Role
        {
            Leader,
            Follower,
            Candidate,
            Witness,
        }

        private readonly ActorLog log = new ActorLog();

        private readonly State state;

        private readonly int me;

        private Role role = Role.Follower;

        private int commitIndex = -1;

        private int lastApplied = -1;

        // Leader state:
        private List<int> nextIndex;
        private List<int> matchIndex;
        private List<Task<Result>> pendingAppends;
        private List<DateTime> lastHeartbeat;

        private readonly IRemoteCall remote;

        private Random rnd = new Random();

        private bool exit = false;

        private const int CancelElectionMsAvg = 10000;
        private const int CancelElectionMsVar = 2000;
        private const int LeaderHeartbeatIntervalMs = 100;
        private const int TriggerElectionTimeoutMs = 5000;  // must be >> LeadserHeartbeatIntervalMs

        // Candidate state
        private List<int> votesReceived;

        enum VoteResult
        {
            Pending,
            Won,
            Lost,
            TimedOut,
        };

        private VoteResult voteResult;

        // Follower
        private System.Timers.Timer triggerElectionTimer;

        private int lastLeader;

        private int CancelElectionMs 
        { 
            get 
            { 
                return CancelElectionMsAvg - (CancelElectionMsVar / 2) + rnd.Next(CancelElectionMsVar); 
            } 
        }

        private Result Success
        {
            get
            {
                return new Result
                {
                    success = true,
                    term = log.CurrentTerm,
                    source = me,
                };
            }
        }

        private Result Failure
        {
            get
            {
                return new Result
                {
                    success = false,
                    term = log.CurrentTerm,
                    source = me,
                };
            }
        }


        #region RPC Endpoints
        public Result RequestVote(int term, int candidateId, int lastLogIndex, int lastLogTerm)
        {
            lock (this)
            {
                Log("Got vote request from {0}", candidateId);
                if (term < log.CurrentTerm)
                {
                    Log("Vote request has old term {0}", term);
                    return Failure;
                }

                if (term > log.CurrentTerm)
                {
                    log.CurrentTerm = term;
                    log.FlushState();
                    ChangeRole(Role.Follower);
                }

                if (log.VotedFor.HasValue && log.VotedFor.Value != candidateId)
                {
                    Log("Already voted for {0}", log.VotedFor.Value);
                    return Failure;
                }

                if (!log.IsUpToDate(lastLogIndex, lastLogTerm))
                {
                    Log("Candidate not up to date", candidateId);
                    return Failure;
                }

                log.VotedFor = candidateId;
                Log("Voted for {0}", candidateId);

                log.FlushState();

                return Success;
            }

        }

        public Result AppendEntries(int term, int leaderId, int prevLogIndex, int prevLogTerm, List<ActorLog.LogEntry> entries, int leaderCommit)
        {
            lock (this)
            {
                Log("Got appendEntries for term {0}, {1}, {2}", term, prevLogIndex, entries.Count);
                if (term < log.CurrentTerm)
                {
                    return Failure;
                }

                if (term > log.CurrentTerm)
                {
                    Log("Updating term");
                    log.CurrentTerm = term;
                    log.FlushState();
                }

                if (role != Role.Follower)
                {
                    Log("Changing to follower");
                    ChangeRole(Role.Follower);
                }

                Debug.Assert(role == Role.Follower);

                if (log.LastIndex < prevLogIndex)
                {
                    Log("Fail: {0} < {1}", log.LastIndex, prevLogIndex);
                    return Failure;
                }

                if (log[prevLogIndex].term != prevLogTerm)
                {
                    Log("Fail2: {0} != {1}", log[prevLogIndex].term, prevLogTerm);
                    log.TruncateLog(prevLogIndex);
                    return Failure;
                }

                log.AppendLog(prevLogIndex, entries);

                if (leaderCommit > commitIndex)
                {
                    commitIndex = Math.Min(leaderCommit, log.LastIndex);
                }

                while (commitIndex > lastApplied)
                {
                    ++lastApplied;
                    state.Apply(log[lastApplied]);
                    Log("Applied " + lastApplied);
                }

                lastLeader = leaderId;

                ResetTriggerElectionTimer();
                Log("Yo");
                return Success;
            }
        }

        #endregion

        private void ChangeRole(Role role)
        {
            if (this.role == role)
            {
                return;
            }

            this.role = role;

            Monitor.PulseAll(this);
        }

        #region Candidate 
        private void RunElection()
        {
			log.CurrentTerm++;
            Log("Starting election with term {0}", log.CurrentTerm);

            log.FlushState();            
            votesReceived = new List<int>();
            voteResult = VoteResult.Pending;

            List<Task<Result>> tasks = new List<Task<Result>>(state.ClusterMembers.Count());

            Task.Delay(CancelElectionMs).ContinueWith((x) => DoCancelElection(log.CurrentTerm));
            
            for (int idx = 0; idx < state.ClusterMembers.Count(); idx++)
            {
                remote.RequestVote(state.ClusterMembers.ElementAt(idx), log.CurrentTerm, me, log.LastIndex, log[log.LastIndex].term).ContinueWith((r) => HandleVote(r, log.CurrentTerm));
            }

            var currentTerm = log.CurrentTerm;

            while (voteResult == VoteResult.Pending && !exit)
            {
                Monitor.Wait(this, 1000);
            }
        }

        private void HandleVote(Task<Result> task, int term)
        {
            lock (this)
            {
                if (term != log.CurrentTerm || role != Role.Candidate || voteResult != VoteResult.Pending)
                {
                    // Old vote, ignore.
                    return;
                }
                if (!task.IsCompleted)
                {
                    Log("Task did not complete");
                    return;
                }
                if (task.Result.term > log.CurrentTerm)
                {
                    Log("Got higher term");
                    log.CurrentTerm = task.Result.term;
                    log.FlushState();
                    ChangeRole(Role.Follower);
                    voteResult = VoteResult.Lost;
                }
                if (task.Result.success)
                {
                    votesReceived.Add(task.Result.source);
                    Log("Got vote from {0}, now got {1}", task.Result.source, votesReceived.Distinct().Count());
                    if (state.CheckMajority(votesReceived))
                    {
                        ChangeRole(Role.Leader);
                        voteResult = VoteResult.Won;
                    }
                }
                Monitor.PulseAll(this);
            }
        }
        private void DoCancelElection(int term)
        {
            lock (this)
            {
                if (term != log.CurrentTerm || role != Role.Candidate || voteResult != VoteResult.Pending)
                {
                    // Old vote, ignore.
                    return;
                }
                voteResult = VoteResult.TimedOut;
                Monitor.PulseAll(this);
            }
        }
                
        #endregion
        
        #region Leader
        private void LeaderHeartbeatAll()
        {
            for (int idx = 0; idx < state.ClusterMembers.Count(); idx++)
            {
                if (idx == me)
                {
                    pendingAppends[idx] = null;
                    continue;
                }
                if (pendingAppends[idx] == null && (matchIndex[idx] < log.LastIndex || (DateTime.UtcNow - lastHeartbeat[idx]).TotalMilliseconds > LeaderHeartbeatIntervalMs))
                {
                    LeaderHeartbeat(idx);                    
                }
            }
        }

        private void LeaderHeartbeat(int idx)
        {
            lastHeartbeat[idx] = DateTime.UtcNow;
            
            var prevIndex = nextIndex[idx] - 1;
            pendingAppends[idx] = remote.AppendEntries(
                state.ClusterMembers.ElementAt(idx),
                log.CurrentTerm,
                me,
                prevIndex,
                log[prevIndex].term,
                log.Slice(nextIndex[idx]),
                commitIndex);
            int lastIndex = log.LastIndex;
            pendingAppends[idx].ContinueWith(x => AppendEntriesDone(idx, lastIndex, x));
        }

        // Called on threadpool thread when AppendEntries return
        private void AppendEntriesDone(int member, int lastIndex, Task<Result> task)
        {
            lock(this)
            {
                pendingAppends[member] = null;

                if (task.IsCompleted)
                {
                    if (task.Result.term > log.CurrentTerm)
                    {
                        ChangeRole(Role.Follower);
                    }
                    else if (!task.Result.success)
                    {
                        nextIndex[member]--;
                        LeaderHeartbeat(member);
                    }
                    else
                    {
                        matchIndex[member] = lastIndex;
                        nextIndex[member] = lastIndex + 1;
                        CheckForCommit();

                        // Check if we should send another immideately.
                        if (lastIndex < log.LastIndex)
                        {
                            LeaderHeartbeat(member);
                        }
                    }
                }
            }
        }

        #endregion

        private void CheckForCommit()
        {
            Debug.Assert(role == Role.Leader);

            for (int newCommit = commitIndex + 1; newCommit <= log.LastIndex; newCommit++)
            {
                if (log[newCommit].term != log.CurrentTerm)
                {
                    break;
                }

                var members = Enumerable.Range(0, state.ClusterMembers.Count()).Where(i => matchIndex[i] >= newCommit);

                if (!state.CheckMajority(members))
                {
                    break;
                }

                commitIndex = newCommit;
            }

            ApplyState();
        }

        private void ApplyState()
        {
            bool stateChanged = false;
            while (commitIndex > lastApplied)
            {
                stateChanged = true;
                state.Apply(log[++lastApplied]);
            }
            if (stateChanged)
            {
                Monitor.PulseAll(this);
            }
        }

        private void LeaderLoop()
        {
            Log("Becoming leader");

            nextIndex = new List<int>(Enumerable.Repeat(log.LastIndex + 1, state.ClusterMembers.Count()));
            matchIndex = new List<int>(Enumerable.Repeat(-1, state.ClusterMembers.Count()));
            pendingAppends = new List<Task<Result>>(Enumerable.Repeat<Task<Result>>(null, state.ClusterMembers.Count()));
            lastHeartbeat = new List<DateTime>(Enumerable.Repeat(DateTime.MinValue, state.ClusterMembers.Count()));

            while (role == Role.Leader && !exit)
            {
                Log("Leader heartbeat loop");
                LeaderHeartbeatAll();
                Monitor.Wait(this, LeaderHeartbeatIntervalMs);
            }
        }

        private void FollowerLoop()
        {
            Log("Becoming follower");

            triggerElectionTimer = new System.Timers.Timer(TriggerElectionTimeoutMs)
            {
                AutoReset = false
            };
            triggerElectionTimer.Elapsed += triggerElectionTimer_Elapsed;
            triggerElectionTimer.Start();

            while (role == Role.Follower && !exit)
            {
                Monitor.Wait(this);
            }

            triggerElectionTimer.Dispose();
            triggerElectionTimer = null;                 
        }

        private void triggerElectionTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            lock (this)
            {
                Log("Timed out, triggering election");
                ChangeRole(Role.Candidate);
            }
        }

        private void ResetTriggerElectionTimer()
        {
            lock (this)
            {
                if (triggerElectionTimer != null)
                {
                    Log("Resetting election timer");
                    
                    triggerElectionTimer.Stop();
                    triggerElectionTimer.Interval = TriggerElectionTimeoutMs;
                    triggerElectionTimer.Start();
                }
            }
        }

        private void CandidateLoop()
        {
            Log("Becoming candidate");

            while (role == Role.Candidate && !exit)
            {
                RunElection();
            }
        }

        public void Loop()
        {            
            lock (this)
            {
                while (!exit)
                {

                    try
                    {
                        switch (role)
                        {
                            case Role.Leader:
                                LeaderLoop();
                                break;
                            case Role.Follower:
                                FollowerLoop();
                                break;
                            case Role.Candidate:
                                CandidateLoop();
                                break;
                        }
                    }
                    catch (AggregateException e)
                    {
                        throw e.InnerException;
                    }
                }
            }
        }

        public void Exit()
        {
            lock(this)
            {
                exit = true;
                Monitor.PulseAll(this);
            }
        }

        public Actor(string me, IList<string> members, IRemoteCall dispatcher)
        {
            this.state = new State(members);
            this.remote = dispatcher;
            this.me = members.IndexOf(me);
        }

        public void SetValue(string key, string value)
        {
            try
            {
                SetValueAsync(key, value).Wait();
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        async public Task SetValueAsync(string key, string value)
        {
            int index;
            lock(this)
            {
                if (role != Role.Leader)
                {
                    if (log.VotedFor.HasValue)
                    {
                        throw new NotLeaderException(state.ClusterMembers.ElementAt(log.VotedFor.Value));
                    }
                    else
                    {
                        throw new NotLeaderException(null);
                    }                    
                }


                index = log.Add(new ActorLog.LogEntry
                {
                    type = ActorLog.LogType.AddMember,
                    key = key,
                    value = value,
                    term = log.CurrentTerm,
                });

                matchIndex[me] = index;

                Monitor.Pulse(this);             
            }

            await WaitForApplied(index);
            Log("Successfully set {0} to {1} at {2}", key, value, index);
        }

        async private Task WaitForApplied(int index)
        {
            await Task.Run(() =>
            {
                lock (this)
                {
                    while (lastApplied < index)
                    {
                        Monitor.Wait(this);
                    }                    
                }
            });            
        }
        
        public string GetValue(string key)
        {
            lock(this)
                {
                if (role != Role.Leader)
                {
                    if (log.VotedFor.HasValue)
                    {
                        throw new NotLeaderException(state.ClusterMembers.ElementAt(log.VotedFor.Value));
                    }
                    else
                    {
                        throw new NotLeaderException(null);
                    }
                }

                return state[key];
            }
        }


        public class NotLeaderException : Exception
        {
            public string CurrentLeader { get; private set; }

            public NotLeaderException(string currentLeader)
            {
                CurrentLeader = currentLeader;
            }

            public override string Message
            {
                get
                {
                    if (CurrentLeader != null)
                    {
                        return string.Format("Not current leader, suggested leader: {0}", CurrentLeader);
                    }
                    else
                    {
                        return string.Format("Not current leader");
                    }
                }
            }
        }

        private void Log(string output, params object[] objects)
        {
            System.Diagnostics.Debug.WriteLine(state.ClusterMembers.ElementAt(me) + ": " + string.Format(output, objects));
        }
                
    }


}
