using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using RaftHash;

namespace RaftHashTest
{
    internal class TestDispatcher : IRemoteCall
    {
        private Dictionary<string, Actor> actors;
        public TestDispatcher(Dictionary<string, Actor> actors)
        {
            this.actors = actors;
        }

        public async Task<Result> RequestVote(string target, int term, int candidateId, int lastLogIndex, int lastLogTerm)
        {
            Console.WriteLine("In RequestVote!");
            var actor = actors[target];
            return await Task.Factory.StartNew<Result>(() => actor.RequestVote(term, candidateId, lastLogIndex, lastLogTerm));
        }

        public async Task<Result> AppendEntries(string target, int term, int leaderId, int prevLogIndex, int prevLogTerm, List<ActorLog.LogEntry> entries, int leaderCommit)
        {
            var actor = actors[target];
            return await Task.Factory.StartNew<Result>(() => actor.AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit));
        }
    }
    
    [TestClass]
    public class ActorTest
    {
        [TestMethod]
        public void TestMultiThreaded()
        {
            List<string> actorNames = new List<string>() { "zero", "one", "two" };

            Dictionary<string, Actor> actors = new Dictionary<string, Actor>();
            List<Thread> threads = new List<Thread>();

            var dispatcher = new TestDispatcher(actors);

            foreach (var member in actorNames)
            {
                var actor = new Actor(member, actorNames, dispatcher);
                var thread = new Thread(x => actor.Loop());
                actors.Add(member, actor);
                threads.Add(thread);
            }

            threads.ForEach(x => x.Start());

            for (int i = 0; i < 60; i++)
            {
                CallOnLeader(actors, x => x.SetValue(i.ToString(), i.ToString()));
            }

            for (int i = 0; i < 60; i++)
            {
                CallOnLeader(actors, x => Assert.AreEqual(i.ToString(), x.GetValue(i.ToString())));
            }

            foreach (var actor in actors.Values)
            {
                actor.Exit();
            }
            
            foreach (var thread in threads)
            {
                thread.Join();
            }
        }

        private string leaderGuess;

        void CallOnLeader(Dictionary<string, Actor> actors, Action<Actor> action)
        {
            while (true)
            {
                if (leaderGuess == null || !actors.ContainsKey(leaderGuess))
                {
                    leaderGuess = actors.Keys.First();
                }

                try
                {
                    action.Invoke(actors[leaderGuess]);
                    return;
                }
                catch (Actor.NotLeaderException e)
                {
                    leaderGuess = e.CurrentLeader;
                    if (leaderGuess == null)
                    { 
                        Thread.Sleep(1000);
                    }
                }
            }

        }
    }
}
