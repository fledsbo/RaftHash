using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RaftHash;


namespace RaftHashTest
{
    [TestClass]
    public class StateTest
    {

        [TestMethod]
        public void TestMajority()
        {
            var state = new State(new[] { "one", "two", "three" });
            Assert.IsTrue(state.CheckMajority(new[]{0, 1}));
            Assert.IsTrue(state.CheckMajority(new[]{0, 1, 2}));
            Assert.IsFalse(state.CheckMajority(new[]{0}));
            Assert.IsFalse(state.CheckMajority(new[]{0, 4}));
        }
    }
}

