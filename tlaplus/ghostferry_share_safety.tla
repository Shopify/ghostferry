----------------------------- MODULE ghostferry_share_safety -----------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Records,          \* Set of possible records that can be in the database
    TableCapacity,    \* Maximum size of the table
    LockMode,        \* Either "FOR_UPDATE" or "FOR_SHARE_NOWAIT"
    NoRecordHere     \* Special value to indicate no record exists

ASSUME /\ LockMode \in {"FOR_UPDATE", "FOR_SHARE_NOWAIT"}
       /\ NoRecordHere \notin Records

\* Process identifiers
CONSTANTS
    TableIterator,    \* Process that copies data
    Application       \* Process that writes data

PrimaryKeys == 1..TableCapacity
PossibleRecords == Records \cup {NoRecordHere}

\* Helper operator to check if a process can acquire a lock on a row
CanAcquireLock(row, proc, owners) ==
    CASE LockMode = "FOR_UPDATE" ->
        \* For update: only if no other process holds the lock
        owners[row] = {}
    [] LockMode = "FOR_SHARE_NOWAIT" ->
        \* For share: ok if no writer holds the lock
        \/ owners[row] = {}
        \/ ~(\E owner \in owners[row]: owner = Application)

VARIABLES
    SourceTable,      \* Source database table
    TargetTable,      \* Target database table
    lockOwners,       \* Map from row -> set of processes that own the lock
    copyComplete,     \* Whether TableIterator has finished copying
    currentRow,       \* Current row being processed by TableIterator
    rowToModify,      \* Row that Application is trying to modify
    newValue          \* New value for the row

vars == << SourceTable, TargetTable, lockOwners, copyComplete,
          currentRow, rowToModify, newValue >>

Init ==
    /\ SourceTable \in [PrimaryKeys -> PossibleRecords]
    /\ TargetTable = [k \in PrimaryKeys |-> NoRecordHere]
    /\ lockOwners = [r \in PrimaryKeys |-> {}]
    /\ copyComplete = FALSE
    /\ currentRow = 1
    /\ rowToModify \in PrimaryKeys
    /\ newValue \in Records

\* TableIterator tries to copy a row
LockRowToCopy ==
    /\ ~copyComplete
    /\ currentRow <= TableCapacity
    /\ CanAcquireLock(currentRow, TableIterator, lockOwners)
    /\ lockOwners' = [lockOwners EXCEPT ![currentRow] = @ \cup {TableIterator}]
    /\ UNCHANGED << SourceTable, TargetTable, copyComplete, currentRow, rowToModify, newValue >>

\* TableIterator errors on a cursor batch when it can't get a lock on any row (FOR_SHARE_NOWAIT)
FailOnLockedRow ==
    /\ ~copyComplete
    /\ currentRow <= TableCapacity
    /\ LockMode = "FOR_SHARE_NOWAIT"
    /\ ~CanAcquireLock(currentRow, TableIterator, lockOwners)
    /\ UNCHANGED << SourceTable, TargetTable, lockOwners, currentRow, copyComplete, rowToModify, newValue >>

\* TableIterator tries copies
CopyRow ==
    /\ ~copyComplete
    /\ currentRow <= TableCapacity
    /\ lockOwners' = [lockOwners EXCEPT ![currentRow] = @ \ {TableIterator}]
    /\ IF SourceTable[currentRow] # NoRecordHere
        THEN TargetTable' = [TargetTable EXCEPT ![currentRow] = SourceTable[currentRow]]
        ELSE UNCHANGED TargetTable
    /\ currentRow' = currentRow + 1
    /\ UNCHANGED << SourceTable, copyComplete, rowToModify, newValue >>

\* TableIterator waits for a row (FOR_UPDATE)
WaitForRow ==
    /\ ~copyComplete
    /\ currentRow <= TableCapacity
    /\ LockMode = "FOR_UPDATE"
    /\ ~CanAcquireLock(currentRow, TableIterator, lockOwners)
    /\ UNCHANGED vars

\* TableIterator completes copying
CompleteCopy ==
    /\ ~copyComplete
    /\ currentRow > TableCapacity
    /\ copyComplete' = TRUE
    /\ UNCHANGED << SourceTable, TargetTable, lockOwners, currentRow, rowToModify, newValue >>

\* Application modifies a row
LockRowToModify ==
    /\ ~copyComplete
    /\ lockOwners[rowToModify] = {} \* No locks on the row (in any mode, it's a FOR UPDATE by Application)
    /\ lockOwners' = [lockOwners EXCEPT ![rowToModify] = @ \cup {Application}]
    /\ UNCHANGED << SourceTable, TargetTable, copyComplete, rowToModify, currentRow, newValue >>

ModifyRow ==
    /\ ~copyComplete
    /\ SourceTable' = [SourceTable EXCEPT ![rowToModify] = newValue]
    /\ IF TargetTable[rowToModify] # NoRecordHere \* Simulate a streaming-like update to TargetTable too, if row exists
        THEN TargetTable' = [TargetTable EXCEPT ![rowToModify] = newValue]
        ELSE UNCHANGED TargetTable
    /\ lockOwners' = [lockOwners EXCEPT ![rowToModify] = @ \ {Application}]
    /\ \E r \in PrimaryKeys, v \in Records:
        /\ rowToModify' = r
        /\ newValue' = v
    /\ UNCHANGED << TargetTable, copyComplete, currentRow >>

\* Application picks a new row to modify
PickNewRow ==
    /\ ~copyComplete
    /\ ~CanAcquireLock(rowToModify, Application, lockOwners)
    /\ \E r \in PrimaryKeys, v \in Records:
        /\ rowToModify' = r
        /\ newValue' = v
    /\ UNCHANGED << SourceTable, TargetTable, lockOwners, copyComplete, currentRow >>

\* System is done when copy is complete
Done ==
    /\ copyComplete
    /\ UNCHANGED vars

\* Always-enabled action to prevent deadlocks
Stutter ==
    UNCHANGED vars

Next ==
    \/ LockRowToCopy
    \/ CopyRow
    \/ FailOnLockedRow
    \/ WaitForRow
    \/ CompleteCopy
    \/ LockRowToModify
    \/ ModifyRow
    \/ PickNewRow
    \/ Done
    \/ Stutter

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(LockRowToCopy)
        /\ WF_vars(CopyRow)
        /\ WF_vars(FailOnLockedRow)
        /\ WF_vars(CompleteCopy)
        /\ WF_vars(LockRowToModify)
        /\ WF_vars(ModifyRow)
        /\ WF_vars(PickNewRow)
        \* No fairness for WaitForRow or Stutter

\* Safety Properties
TypeOK ==
    /\ SourceTable \in [PrimaryKeys -> PossibleRecords]
    /\ TargetTable \in [PrimaryKeys -> PossibleRecords]
    /\ lockOwners \in [PrimaryKeys -> SUBSET {TableIterator, Application}]
    /\ currentRow \in 1..(TableCapacity+1)
    /\ copyComplete \in BOOLEAN

LockSafety ==
    \A row \in PrimaryKeys:
        \/ lockOwners[row] = {}  \* No locks
        \/ /\ LockMode = "FOR_UPDATE"
           /\ Cardinality(lockOwners[row]) = 1  \* Exclusive lock
        \/ /\ LockMode = "FOR_SHARE_NOWAIT"
           /\ ~(\E owner1, owner2 \in lockOwners[row]:  \* No writer conflicts
                /\ owner1 # owner2
                /\ (owner1 = Application \/ owner2 = Application))

DataConsistency ==
    \A k \in PrimaryKeys:
        TargetTable[k] # NoRecordHere => TargetTable[k] = SourceTable[k]

FinalConsistency ==
    copyComplete => (\A k \in PrimaryKeys:
                      SourceTable[k] # NoRecordHere => TargetTable[k] = SourceTable[k])

\* Liveness Properties
CopyEventuallyCompletes ==
    LockMode = "FOR_SHARE_NOWAIT" => <>(copyComplete \/ ENABLED Stutter)

ModificationProgress ==
    []<>(\A k \in PrimaryKeys:
        \/ copyComplete
        \/ CanAcquireLock(k, Application, lockOwners)
        \/ ENABLED Stutter)

\* State Constraints
StateConstraint ==
    /\ Cardinality(DOMAIN SourceTable) <= TableCapacity
    /\ Cardinality(DOMAIN TargetTable) <= TableCapacity

=============================================================================