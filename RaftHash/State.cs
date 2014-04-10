using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaftHash
{
    internal class State
    {
        public IReadOnlyCollection<string> ClusterMembers
        {
            get
            {
                return clusterMembers.AsReadOnly();
            }
        }

        private List<string> clusterMembers;

        public string this[string key]
        {
            get
            {
                return data[key];
            }
        }

        private readonly ConcurrentDictionary<string, string> data = new ConcurrentDictionary<string,string>();

        internal void Apply(ActorLog.LogEntry logEntry)
        {
            switch (logEntry.type)
            {
                case ActorLog.LogType.AddMember:
                    data.AddOrUpdate(logEntry.key, logEntry.value, (key, value) => logEntry.value);
                    break;

                case ActorLog.LogType.RemoveMember:
                    string dummy;
                    data.TryRemove(logEntry.key, out dummy);
                    break;

            }
        }
        
        internal bool CheckMajority(IEnumerable<int> votes)
        {
            return votes.Distinct().Where(x => x < ClusterMembers.Count() && x >= 0).Count() 
                > Math.Floor((decimal)ClusterMembers.Count() / 2);
        }

        internal State(IEnumerable<string> members)
        {
            clusterMembers = new List<string>(members);
        }
    }
}
