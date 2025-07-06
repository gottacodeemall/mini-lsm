## Week 1
### Day 1
Understanding about fairness in locks
-  If there are readers which have taken a read lock, and new set of readers keep coming in, the readers will keep the lock to themselves starving any writers waiting for the read lock to be released.
    - Parking Lot locks ensures fairness [ref](https://amanieu.github.io/parking_lot/parking_lot/struct.RwLock.html)
- How are fairness based locks implemented:
    1. Ticket based System - Queueing and tokening
    2. Alternating - Alternate between readers and writers
    3. Priority Ordering where writers have higher priority over readers

Interesting parts about locking
- How do you prevent a long running operation to block a write lock
    - Perform the long running operation outside the lock
    - Only take the lock for the atomic swap of memtable in the reference
- How do you prevent 2 competing writers from updating the memtable twice
    - Take a lock on the full `state` object during writes
- How do you ensure that the writers don't update the frozen memtable
    - Taking the full lock the `state` object will ensure only 1 write can fully access the state at once.
    - This is exactly what I faced in the unit test, if I am freezing my memtable in a put operation, I should get a new reference to the latest immutable memtable rather than using the old reference.
- `state` object - `Arc<RwLock<Arc<LsmStorageState>>>`
    - Arc around LsmStorageState is to ensure that the clones of LsmStorageState are cheap and can be accessed from multiple threads
    - RwLock is to ensure that the readers keep reading an independent copy of the LsmStorageState
    - Writers wait for the readers to complete taking a reference to the current state and only then apply any mutable operations to update the reference
    - Arc over RwLock is needed to let access to state from multiple threads