using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RaftHash;


namespace RaftHashTest
{
    [TestClass]
    public class ActorLogTest
    {
        [TestMethod]
        public void AppendLog()
        {
            ActorLog log = new ActorLog();

            log.AppendLog(-1, new System.Collections.Generic.List<ActorLog.LogEntry>());
            log.AppendLog(-1, new System.Collections.Generic.List<ActorLog.LogEntry>
                {
                    new ActorLog.LogEntry { term = 0},
                    new ActorLog.LogEntry { term = 1},
                    new ActorLog.LogEntry { term = 2},
                });

            log.AppendLog(2, new System.Collections.Generic.List<ActorLog.LogEntry>
                {
                    new ActorLog.LogEntry { term = 3},
                    new ActorLog.LogEntry { term = 4},
                    new ActorLog.LogEntry { term = 5},
                });

            log.AppendLog(4, new System.Collections.Generic.List<ActorLog.LogEntry>
                {
                    new ActorLog.LogEntry { term = 5},
                    new ActorLog.LogEntry { term = 6},
                });

            log.AppendLog(4, new System.Collections.Generic.List<ActorLog.LogEntry>
                {
                    new ActorLog.LogEntry { term = 5},
                });

            Assert.AreEqual(6, log.LastIndex);

            for (int i = 0; i < 7; i++)
            {
                Assert.AreEqual(i, log[i].term);
            }
        }

        [TestMethod]
        public void TruncateLog()
        {
            ActorLog log = new ActorLog();

            log.AppendLog(-1, new System.Collections.Generic.List<ActorLog.LogEntry>());
            log.AppendLog(-1, new System.Collections.Generic.List<ActorLog.LogEntry>
                {
                    new ActorLog.LogEntry { term = 0},
                    new ActorLog.LogEntry { term = 1},
                    new ActorLog.LogEntry { term = 2},
                });
            log.TruncateLog(1);
            Assert.AreEqual(0, log.LastIndex);
            log.TruncateLog(2);
            log.TruncateLog(1);
            Assert.AreEqual(0, log.LastIndex);
        }

        [TestMethod]
        public void LastIndex()
        {
            ActorLog log = new ActorLog();

            Assert.AreEqual(-1, log.LastIndex);

            log.AppendLog(-1, new System.Collections.Generic.List<ActorLog.LogEntry>
                {
                    new ActorLog.LogEntry { term = 0},
                    new ActorLog.LogEntry { term = 1},
                    new ActorLog.LogEntry { term = 2},
                });
            Assert.AreEqual(2, log.LastIndex);
        }

        [TestMethod]
        public void SetTerm()
        {
            ActorLog log = new ActorLog();
            Assert.IsTrue(log.SetTerm(1));
            Assert.IsFalse(log.SetTerm(1));
            Assert.IsFalse(log.SetTerm(0));
            Assert.IsTrue(log.SetTerm(2));
            Assert.IsFalse(log.SetTerm(2));
        }

        [TestMethod]
        public void IsUpToDate()
        {
            ActorLog log = new ActorLog();
            log.AppendLog(-1, new System.Collections.Generic.List<ActorLog.LogEntry>
                {
                    new ActorLog.LogEntry { term = 0},
                    new ActorLog.LogEntry { term = 1},
                    new ActorLog.LogEntry { term = 2},
                });
            Assert.IsTrue(log.IsUpToDate(2, 2));
            Assert.IsFalse(log.IsUpToDate(5, 1));
            Assert.IsTrue(log.IsUpToDate(1, 3));
            Assert.IsFalse(log.IsUpToDate(1, 2));
            Assert.IsTrue(log.IsUpToDate(3, 2));

        }
    }
}

