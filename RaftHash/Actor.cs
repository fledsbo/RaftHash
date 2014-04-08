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

        private readonly AsyncMonitor monitor = new AsyncMonitor();

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

        // Follower
        private System.Timers.Timer triggerElectionTimer;

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
        async public Task<Result> RequestVote(int term, int candidateId, int lastLogIndex, int lastLogTerm)
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
                await log.FlushStateAsync();
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

            await log.FlushStateAsync();

            return Success;

        }

        async public Task<Result> AppendEntries(int term, int leaderId, int prevLogIndex, int prevLogTerm, List<ActorLog.LogEntry> entries, int leaderCommit)
        {
            if (term < log.CurrentTerm)
            {
                return Failure;
            }

            if (term > log.CurrentTerm)
            {
                log.CurrentTerm = term;
                await log.FlushStateAsync();
            }

            if (role != Role.Follower)
            {
                ChangeRole(Role.Follower);
            }

            Debug.Assert(role == Role.Follower);

            if (log.LastIndex < prevLogIndex)
            {
                return Failure;
            }

            if (log[prevLogIndex].term != prevLogTerm)
            {
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
            }

            ResetTriggerElectionTimer();
            return Success;
        }

        #endregion

        private void ChangeRole(Role role)
        {
            if (this.role == role)
            {
                return;
            }

            this.role = role;

            monitor.PulseAll();
        }

        #region Candidate 
        async private Task RunElection()
        {
			log.CurrentTerm++;
            Log("Starting election with term {0}", log.CurrentTerm);

			log.VotedFor = me;
            await log.FlushStateAsync();
            votesReceived = new List<int>();

            List<Task<Result>> tasks = new List<Task<Result>>(state.ClusterMembers.Count());
            var endElectionTask = DelayEndElection();
            tasks.Add(endElectionTask);
            
            for (int idx = 0; idx < state.ClusterMembers.Count(); idx++)
            {
                if (idx == me)
                {
                    votesReceived.Add(me);

                }
                else
                {
                    tasks.Add(remote.RequestVote(state.ClusterMembers.ElementAt(idx), log.CurrentTerm, me, log.LastIndex, log[log.LastIndex].term));                    
                }
            }

            while (tasks.Count() > 0)
            {
                var task = await Task.WhenAny(tasks);
                tasks.Remove(task);    
                if (task == endElectionTask)
                {
                    Log("Election timed out");
                    break;
                }
                if (!task.IsCompleted)
                {
                    Log("Task did not complete");
                    continue;
                }
                if (task.Result.term > log.CurrentTerm)
                {
                    Log("Got higher term");
                    log.CurrentTerm = task.Result.term;
                    log.FlushState();
                    ChangeRole(Role.Follower);
                    break;
                }
                if (task.Result.success)
                {
                    votesReceived.Add(task.Result.source);
                    Log("Got vote from {0}, now got {1}", task.Result.source, votesReceived.Distinct().Count());
                    if (state.CheckMajority(votesReceived))
                    {
                        ChangeRole(Role.Leader);
                        break;
                    }
                }
            }
        }

        async Task<Result> DelayEndElection()
        {
            await Task.Delay(CancelElectionMs);
            return Success;
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
                if (pendingAppends[idx] == null && (matchIndex[idx] < log.LastIndex || (lastHeartbeat[idx] - DateTime.UtcNow).TotalMilliseconds > LeaderHeartbeatIntervalMs))
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

            pendingAppends[idx].ContinueWith(x => AppendEntriesDone(idx, log.LastIndex, x));
        }

        // Called on threadpool thread when AppendEntries return
        private void AppendEntriesDone(int member, int lastIndex, Task<Result> task)
        {
            using (monitor.Enter())
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
                state.Apply(log[lastApplied++]);
            }
            if (stateChanged)
            {
                monitor.PulseAll();
            }
        }

        async private Task LeaderLoop()
        {
            Log("Becoming leader");

            nextIndex = new List<int>(state.ClusterMembers.Count());
            matchIndex = new List<int>(state.ClusterMembers.Count());
            pendingAppends = new List<Task<Result>>(state.ClusterMembers.Count());
            lastHeartbeat = new List<DateTime>(state.ClusterMembers.Count());

            for (int i = 0; i < nextIndex.Count(); i++)
            {
                nextIndex[i] = log.LastIndex + 1;
                matchIndex[i] = -1;
                pendingAppends[i] = null;
                lastHeartbeat[i] = DateTime.MinValue;
            }

            while (role == Role.Leader && !exit)
            {
                LeaderHeartbeatAll();
                try
                {
                    await monitor.WaitAsync(new CancellationTokenSource(LeaderHeartbeatIntervalMs).Token);
                }
                catch (OperationCanceledException) { }
            }
        }

        async private Task FollowerLoop()
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
                await monitor.WaitAsync();
            }

            triggerElectionTimer.Dispose();
        }

        private void triggerElectionTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            ChangeRole(Role.Candidate);
        }

        private void ResetTriggerElectionTimer()
        {
            triggerElectionTimer.Stop();
            triggerElectionTimer.Start();
        }

        async private Task CandidateLoop()
        {
            using (await monitor.EnterAsync())
            {
                Log("Becoming candidate");

                while (role == Role.Candidate && !exit)
                {
                    await RunElection();
                }
            }

        }

        async private Task Loop()
        {
            while (!exit)
            {
                try
                {
                    switch (role)
                    {
                        case Role.Leader:
                            await LeaderLoop();
                            break;
                        case Role.Follower:
                            await FollowerLoop();
                            break;
                        case Role.Candidate:
                            await CandidateLoop();
                            break;
                    }
                } 
                catch (AggregateException e)
                {
                    throw e.InnerException;
                }
            }
        }

        public void Run()
        {
            AsyncPump.Run(async delegate { await Loop(); });
        }



        public void Exit()
        {
            using (monitor.Enter())
            {
                exit = true;
                monitor.PulseAll();
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
            using (await monitor.EnterAsync())
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


                int index = log.Add(new ActorLog.LogEntry
                {
                    type = ActorLog.LogType.AddMember,
                    key = key,
                    value = value,
                });

                monitor.PulseAll();

                while (lastApplied < index)
                {
                    await monitor.WaitAsync();
                }

            }
        }
        
        public string GetValue(string key)
        {
            using (monitor.Enter())
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
