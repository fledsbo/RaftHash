using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaftHash
{
     struct Result
     {
        public int term;
        public bool success;
        public int source;
     }

    interface IRemoteCall
    {
        Task<Result> RequestVote(string target, int term, int candidateId, int lastLogIndex, int lastLogTerm);

        Task<Result> AppendEntries(string target, int term, int leaderId, int prevLogIndex, int prevLogTerm, List<ActorLog.LogEntry> entries, int leaderCommit);
    }
}
