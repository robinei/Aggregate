using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;

public class Event {
    public readonly String name;
    public readonly Guid sessionId;
    public readonly Guid userId;
    public readonly double timestamp;
    public readonly ReadOnlyDictionary<string, object> props;

    public Event(
        String name,
        Guid sessionId,
        Guid userId,
        double timestamp,
        ReadOnlyDictionary<string, object> props
    ) {
        this.name = name;
        this.sessionId = sessionId;
        this.userId = userId;
        this.timestamp = timestamp;
        this.props = props;
    }
}

public class State {
    public readonly string key;
    public readonly Dictionary<string, object> data;
    public double lastEventTimestamp;
    public bool modified;

    public State(string key, Dictionary<string, object> data) {
        this.key = key;
        this.data = data;
    }

    public bool TryGetInt(string name, out int value) {
        object obj;
        if (data.TryGetValue(name, out obj) && obj is int) {
            value = (int) obj;
            return true;
        }
        value = 0;
        return false;
    }
}

public interface IAggregator {
    bool WantsEventType(string name);
    String GetStateKey(Event evt);
    bool OnEvent(Event evt, State state);
}

public interface IStateDatabase {
    State LoadState(string stateKey);
    void SaveState(State state);
}

public class DummyStateDatabase : IStateDatabase {
    public State LoadState(string stateKey) {
        return new State(stateKey, new Dictionary<string, object>());
    }
    public virtual void SaveState(State state) {

    }
}

public class AggregationContext {
    private readonly IStateDatabase database;
    private readonly ReadOnlyCollection<IAggregator> aggregators;
    private readonly Dictionary<string, ReadOnlyCollection<IAggregator>> aggregatorsForEventName = new Dictionary<string, ReadOnlyCollection<IAggregator>>();
    private readonly Dictionary<string, State> stateCache = new Dictionary<string, State>();
    private readonly List<State> modifiedStates = new List<State>();

    public AggregationContext(IStateDatabase database, ReadOnlyCollection<IAggregator> aggregators) {
        this.database = database;
        this.aggregators = aggregators;
    }

    public void HandleEvent(Event evt) {
        foreach (IAggregator agg in GetAggregatorsForEventType(evt.name)) {
            string stateKey = agg.GetStateKey(evt);
            State state;
            if (!stateCache.TryGetValue(stateKey, out state)) {
                state = database.LoadState(stateKey);
                stateCache[stateKey] = state;
            }
            if (state.lastEventTimestamp >= evt.timestamp) {
                continue;
            }
            state.lastEventTimestamp = evt.timestamp;
            if (agg.OnEvent(evt, state) && !state.modified) {
                state.modified = true;
                modifiedStates.Add(state);
            }
        }
    }

    public void SaveModifiedStates() {
        foreach (State state in modifiedStates) {
            if (state.modified) {
                database.SaveState(state);
                state.modified = false;
            }
        }
        modifiedStates.Clear();
    }

    public ReadOnlyCollection<IAggregator> GetAggregatorsForEventType(string name) {
        ReadOnlyCollection<IAggregator> result;
        if (!aggregatorsForEventName.TryGetValue(name, out result)) {
            List<IAggregator> list = new List<IAggregator>();
            foreach (IAggregator agg in aggregators) {
                if (agg.WantsEventType(name)) {
                    list.Add(agg);
                }
            }
            result = list.AsReadOnly();
            aggregatorsForEventName[name] = result;
        }
        return result;
    }
}


public class BulkAggregator {
    private readonly int workerCount;
    private int nextWorker = 0;
    private readonly AggregationContext distributorContext;
    private readonly AggregationContext[] workerContexts;
    private readonly BlockingCollection<Action>[] workerQueues;
    private readonly Thread[] workerThreads;
    private readonly Dictionary<string, int> stateKeyToWorkerMapping = new Dictionary<string, int>();

    public BulkAggregator(Func<IStateDatabase> databaseFactory, ReadOnlyCollection<IAggregator> aggregators, int workerCount) {
        this.workerCount = workerCount;
        distributorContext = new AggregationContext(new DummyStateDatabase(), aggregators);
        workerContexts = new AggregationContext[workerCount];
        workerQueues = new BlockingCollection<Action>[workerCount];
        workerThreads = new Thread[workerCount];
        for (int i = 0; i < workerCount; ++i) {
            BlockingCollection<Action> queue = new BlockingCollection<Action>();
            workerContexts[i] = new AggregationContext(databaseFactory(), aggregators);
            workerQueues[i] = queue;
            workerThreads[i] = new Thread(() => {
                while (true) {
                    Action action = queue.Take();
                    try {
                        action();
                    } catch (OperationCanceledException) {
                        return;
                    }
                }
            });
        }
    }

    public void StartWorkers() {
        for (int i = 0; i < workerCount; ++i) {
            workerThreads[i].Start();
        }
    }

    public void StopWorkers() {
        for (int i = 0; i < workerCount; ++i) {
            workerQueues[i].Add(() => {
                throw new OperationCanceledException();
            });
        }
        for (int i = 0; i < workerCount; ++i) {
            workerThreads[i].Join();
        }
    }

    public void WaitForWorkers() {
        Semaphore semaphore = new Semaphore(0, workerCount);
        for (int i = 0; i < workerCount; ++i) {
            workerQueues[i].Add(() => {
                semaphore.Release();
            });
        }
        for (int i = 0; i < workerCount; ++i) {
            semaphore.WaitOne();
        }
    }

    public void HandleEvents(Event[] events) {
        foreach (Event evt in events) {
            foreach (IAggregator agg in distributorContext.GetAggregatorsForEventType(evt.name)) {
                string stateKey = agg.GetStateKey(evt);
                int worker;
                if (!stateKeyToWorkerMapping.TryGetValue(stateKey, out worker)) {
                    worker = nextWorker;
                    nextWorker = (nextWorker + 1) % workerCount; // distribute aggregates to workers round-robin
                    stateKeyToWorkerMapping[stateKey] = worker;
                }
                Event e = evt;
                workerQueues[worker].Add(() => {
                    workerContexts[worker].HandleEvent(e);
                });
            }
        }
    }

    public void SaveModifiedStates() {
        for (int i = 0; i < workerCount; ++i) {
            int worker = i;
            workerQueues[worker].Add(() => {
                workerContexts[worker].SaveModifiedStates();
            });
        }
    }
}

