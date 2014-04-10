using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace RaftHash
{
    class ActorLog
    {
        internal enum LogType
        {
            SetValue,
            ClearValue,
            AddMember,
            RemoveMember,
            Special,
        }
        internal struct LogEntry
        {
            public LogType type;
            public int term;
            public string key;
            public string value;                        
        }
        
        static readonly LogEntry Genesis = new LogEntry
        {
            type = LogType.Special,
            term = -1,
        };

        private int currentTerm = -1;
        internal int CurrentTerm 
        {
            get
            {
                return currentTerm;
            }
            set
            {
                if (currentTerm != value)
                {
                    VotedFor = null;
                    currentTerm = value;
                }
            }
        }

        internal int? VotedFor { get; set; }

        private readonly List<LogEntry> log = new List<LogEntry>();

        async internal Task FlushStateAsync()
        {
            await Task.Delay(10);
        }
        
        internal void FlushState()
        {
            Thread.Sleep(10);
        }

        internal void TruncateLog(int prevLogIndex)
        {
            if (prevLogIndex <= LastIndex)
            {
                log.RemoveRange(prevLogIndex, log.Count() - prevLogIndex);
            }
        }

        internal void AppendLog(int prevLogIndex, List<LogEntry> entries)
        {
            int startIndex = log.Count() - (prevLogIndex + 1);
            int numNew = entries.Count() - startIndex;
            if (numNew <= 0)
            {
                // noop
                return;
            }

            log.AddRange(entries.GetRange(startIndex, numNew));
        }

        internal bool SetTerm(int term)
        {
            if (term > CurrentTerm)
            {
                CurrentTerm = term;
                return true;
            }

            return false;
        }

        internal LogEntry this[int index] { get { return index < 0 ? Genesis : log[index]; } }

        internal int LastIndex { get { return log.Count() - 1; } }

        internal bool IsUpToDate(int lastLogIndex, int lastLogTerm)
        {
            if (LastIndex < 0)
            {
                return true;
            }

            if (lastLogTerm > log[LastIndex].term)
            {
                return true;
            }

            if (lastLogTerm < log[LastIndex].term)
            {
                return false;
            }

            return lastLogIndex >= LastIndex;
        }

        internal List<LogEntry> Slice(int startIndex)
        {
            return log.GetRange(startIndex, log.Count() - startIndex);
        }

        internal int Add(LogEntry logEntry)
        {
            log.Add(logEntry);
            return log.Count() - 1;
        }

        internal ActorLog()
        {
            VotedFor = null;
            CurrentTerm = 0;
        }
    }

}

