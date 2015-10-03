# gen_paxos 2

Revival. New from scratch. New license.

# Example use cases

## Leader election

Span a `gen_paxos_leader` as a process using `start_link/3`. After you
all spawned all leader candidates, kick the node with `boot/2`
function, passing all process ids of other leaders, including
itself. The it starts leader election, by using Paxos.

You can know who is leader, by calling `leader/1` function, passing
`Pid` of the leader process. Or by specifying `controlling_process` as
an option of `start_link`. When an election finished, the leader
module sends out the epoch and name of leader `{elected, {Leader,
Epoch}}`. You also may know the time when master lease has finished,
by receiving `{lease_end, Epoch}`.


## Partition tolerant strongly consistent replication

`gen_paxos_queue` and `gen_paxos_map` are its implementations.


# Internals

Process tree of a node

```
    +--------------+
    |Ballot manager|
    +------+-------+
           |
           |    +------------+
           +----+Acceptor (0)|
           |    +------------+
           |    +------------+
           +----+Learner (0) |
           |    +------------+
           |    +--------------+
           +----+(Proposer (0))|
           |    +--------------+
           |
           ...
           |    +------------+
           +----+Acceptor (E)|
           |    +------------+
           |    +------------+
           +----+Learner (E) |
           |    +------------+
           |    +--------------+
           +----+(Proposer (E))|
                +--------------+
```

Proposer (E) proposes all Acceptors (E), and Learner (E) listens to
all Acceptors (E). Ballot manager exchanges Acceptor Pids each other,
when they don't have enough ballots prepared. All ballots are stored
in each manager's 'buffer'.

## Node replace

Current node replace is done in a naive way where ballot managers just
replaces thier consensus set member when asked for a node replace with
new ballot manager member.
