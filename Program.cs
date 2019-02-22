using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Aggregate
{
    class TestAggregator : IAggregator {
        public bool WantsEventType(string name) {
            switch (name) {
            case "clicked_get_started":
            case "signup_complete":
            case "login_complete":
            case "finished_onboarding":
                return true;
            default:
                return false;
            }
        }

        public String GetStateKey(Event evt) {
            return "onboarding_funnel/user:" + evt.userId;
        }
        
        public bool OnEvent(Event evt, State state) {
            int pos;
            if (!state.TryGetInt("pos", out pos)) {
                pos = 0;
            }
            int nextPos = pos;
            switch (evt.name) {
            case "clicked_get_started":
                nextPos = 1;
                break;
            case "signup_complete":
                if (pos == 1) {
                    nextPos = 2;
                }
                break;
            case "login_complete":
                if (pos == 1) {
                    nextPos = 3;
                }
                break;
            case "finished_onboarding":
                if (pos == 2 || pos == 3) {
                    nextPos = 4;
                }
                break;
            }
            if (pos == nextPos) {
                return false;
            }
            state.data["pos"] = nextPos;
            return true;
        }
    }

    class Program
    {
        private static readonly Guid userId = Guid.NewGuid();
        private static readonly Guid sessionId = Guid.NewGuid();

        private static Event makeEvent(string name, double timestamp) {
            return new Event(name, sessionId, userId, timestamp, new ReadOnlyDictionary<string, object>(new Dictionary<string, object>()));
        }

        class TestDatabase : DummyStateDatabase {
            public override void SaveState(State state) {
                Console.WriteLine("Saving state: " + state.key);
                foreach (string name in state.data.Keys) {
                    Console.WriteLine("  " + name + " = " + state.data[name]);
                }
            }
        }

        static void Main(string[] args)
        {
            ReadOnlyCollection<IAggregator> aggregators = new List<IAggregator> {
                new TestAggregator()
            }.AsReadOnly();
            
            BulkAggregator bulkAggregator = new BulkAggregator(() => new TestDatabase(), aggregators, 2);

            Event[] events = new Event[] {
                makeEvent("clicked_get_started", 1),
                makeEvent("signup_complete", 2),
                makeEvent("finished_onboarding", 3),
            };

            bulkAggregator.StartWorkers();
            bulkAggregator.HandleEvents(events);
            bulkAggregator.SaveModifiedStates();
            bulkAggregator.WaitForWorkers();
            bulkAggregator.StopWorkers();
        }
    }
}
