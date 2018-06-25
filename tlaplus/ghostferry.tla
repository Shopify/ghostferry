----------------------------- MODULE ghostferry -----------------------------

(***************************************************************************
 This is the model of the primary Ghostferry algorithm of copying data. Note
 that this is a model that has many simplifying assumptions and thus it will
 have a lot of differences with the actual implementation.

 The comment here describes this model and justify how it is applicable to
 to the real world.

 ## Process Model ##

 A standard, parallel Ghostferry run is something along the following lines:

 1. Ghostferry begins to tail and apply the binlogs via the BinlogStreamer.
 2. Ghostferry begins to iterate through the tables, applying the rows in
    batches via the DataIterator. Ghostferry will find the current minimum
    and maximum primary key for each source table and copies all rows within
    the ranges.
 3. The API consumer of Ghostferry (e.g. copydb) waits until the data copying
    is completed.
 4. The application/source database is locked/set as read only and copydb wait
    until all writes are completed (cutover stage).
 5. The binlog streaming is instructed to stop. A target binlog position is set
    to be the current binlog position of the master. The binlog streamer will
    catch up to that position and then quit.
 6. At this point, the source and target database are expected to be identical.

 Note that until the source database is set to read only, something
 (the application/dba/whatever) will perform INSERT/UPDATE/DELETEs on the source
 database concurrently with respect to Ghostferry.

 This whole process is modeled as four separate processes:
 1. TableIterator: Performs the job of the DataIterator, but only on one table
                   (see Data Model for justification).
 2. BinlogStreamer: Tails and applies the binlog of the source database.
 3. Application: Perform INSERT/UPDATE/DELETE on the source database and record
                 the action into the source database binlog. Essentially
                 combines both the database itself and the client of the
                 database (app/dba/whatever).
 4. Ferry: Coordinates the entire run by waiting for the data copy to complete,
           performing the cut over stage, and stopping the binlog streamer.

 ## Data Model ##

 In the real world, each database contains many schemas, each with many tables.
 Each table can have millions/billions of rows, each of which has many values
 associated with many columns. This is a very difficult to replicate in a just a
 simple mental model, never mind a formal, checkable model. The present data
 model makes simplifying assumptions and reduces the source and target databases
 down to its very essense:

 1. Each database only contains a single table as opposed to many tables.
 2. A table is modeled as a finite sequence of `PossibleRecords`. Each element
    in the sequence is a row and the index of that element is the primary key of
    the row
    - Hence, primary keys are in the set of natural numbers.
 3. `PossibleRecords` is a set of all possible records. There's a special
    element called `NoRecordHere` that indicate that row does not exist. In
    other words, if the i-th entry of the table sequence is `NoRecordHere`, it
    is the equivalent of saying that row does not exist.
    - INSERT into the database is just an UPDATE of a row with `NoRecordHere`
      to another record.
    - DELETE is just an UPDATE to a row with a record with `NoRecordHere`.
    - Thus, the table sequence will always be filled to its maximum capacity.
    - The initial table layout should be something like
      <<record, record, ..., record, NoRecordHere, ... NoRecordHere>>
    - The minimum primary key will always be 1.
    - The maximum primary key will initially be the last entry that is not
      NoRecordHere. This number is manually specified because it will complicate
      the model.
    - The table length is defined by the last entry of record before
      NoRecordHere. The table length is increased everytime when an INSERT
      occurs at a primary key above the current length.
    - The table capacity is defined by the size of the sequence, including the
      NoRecordHere elements.
 4. The Binlog is modeled as a sequence of log entries.
    - The sequence starts empty and are appended to every time the source table
      is changed.
    - To ensure the binlog does not grow to infinity and checking with TLC stays
      feasible, a maximum size is constrained on it.

 Currently, the simplifications made here are not inductively proven to be
 applicable to the real dataset. However, some prose arguments are made that
 is convincing that the simplifications will be applicable to the real, larger
 (possibly infinite) dataset.

 ### Hand waving for: Finite table = Infinite tables ###

 Note that this is not a proof by any sense of the word, merely a hand-wavy
 justification of correctness.

 Since TLC can only check a finite model (and only a small one is feasible),
 the size of the source and target database must be kept as low as possible.
 If we modeled a database with 100 rows, the possible states for TLC to verify
 will explode to an astronomical amount and will not complete in a reasonable
 time. Thus, we have to choose the size of the initial databases and this is
 done via the concept of "super rows".

 Super rows are simply the idea that we can combine multiple rows into a single
 row and not change the semantics of the operation. For example, we can define
 a super row to be a combination of 100 regular rows. If we have a table
 with n regular rows, it would also thus have n/100 super rows.

 Suppose that the super row j maps to regular rows [i, i+100) and suppose that
 the DataIterator atomically copies 100 regular rows [i, i+100) in a batch.
 This would be the equivalent of copying a single super row j:

 ROW_COPY([i, i+100)) = SUPER_ROW_COPY(j)

 As long as ROW_COPY is atomic, the ROW_COPY is the equivalent to
 SUPER_ROW_COPY.

 A more complex case is if we copy 150 rows, which would imply it is copying
 two super rows. Thus:

 ROW_COPY([i, i+150)) = SUPER_ROW_COPY(j) /\ SUPER_ROW_COPY(j+1)

 We can make similar arguments for the binlog streamer, where instead of row
 copy, we are performing operations that mirrows INSERT/UPDATE/DELETE.

 Philosophically, we can also think of the entire table as one giant super row.
 This would imply the table only needs a size of 1. However, without an actual
 proof of correness, the finite table size is chosen to be 2. To be more
 cautious: The capacity of the table is 3 so we can INSERT a row.

 An alternative way to think about this:

 - We can either: app update a row OR copy the row OR binlog to apply the update.
 - For any particular row, the copy phase can only occur once. update and binlog
   can occur any number of times in any order.
 - Binlog respects the ordering of the update
 - There are only three cases that we really care about:
   - copy -> update -> binlog: copy before update and binlog
   - update -> copy -> binlog: copy in between the update and binlog
   - update -> binlog -> copy: copy after update and binlog.
 - Logically, the copy operate can only happen within those time.
   - This means copy \/ update \/ binlog.
   - No where did we need to involve the size of the table, thus copy and update
     can be their super equivalent, which means we only need 1 record to prove
     that this works.

 ### Hand waving for: finite set of possible records to be inserted = infinite set of possible records ###

 There are an infinite amount of possible Records we can insert in real life.
 The TLA+ spec reflects that by not assuming a size in the Record set. However,
 since TLC can only check a small, finite model, the number of records in this
 set must be restricted to a finite amount and we need to inductively proof
 that the behaviours with this finite Record set is equivalent to the infinite
 set. However, this proof is not yet available and a brief "justification" is
 given below:

 Since we ever only care about the transition of a row from containing one
 Record to another, we only need to have two records in this set.

 ### Hand waving for: restricting maximum binlog size is ok ###

 If we do not restrict the maximum binlog size, it will grow unbounded. We
 need to limit it in TLC otherwise the state checking will not complete.

 The intuitive amount to bound this at is the maximum of the capacity of the
 tables and the number of different types of operations we model. The reason is
 this allows at least one branch of the state tree to update each of the entries
 in the table with a different operation.

 ***************************************************************************)

EXTENDS Integers, Sequences, TLC, FiniteSets

\* Helper Methods
\* ==============

SetMin(S) == CHOOSE i \in S: \A j \in S : i <= j
Range(S) == { S[x] : x \in DOMAIN S }

\* Constant Declarations
\* =====================

(***************************************************************************
This defines the initial layout of the source table. The source table should
be defined as a sequence with a bunch of Records. It is possible to be
NoRecordHere as that is the placeholder for an non-existent row.

Example: InitialTable = <<r0, r1, NoRecordHere>>
         MaxPrimaryKey = 2

The example table will also have a TableCapacity of 3 and a CurrentMaxPrimaryKey
of 2. The CurrentMaxPrimaryKey can be increased when the third element is
updated to an element \in Records.
 ***************************************************************************)
CONSTANT TableCapacity

(***************************************************************************
In the case where Ghostferry is restarted, we need to start at a binlog
position that is not the current position. Since the model does not store past
binlog events, we cannot model it that way. Instead, we simply generate
some binlog entries (and the same number of changes on the SourceTable).

Another couple of ways to think about this parameter:

- Number of changes that occured while Ghostferry is down before being resumed.
- The current master position of the source database at the moment Ghostferry
  starts, while the last successful binlog position would be 0.

Example: 0 if not running the reconciliation step.
         2, 3, some small-ish number if running the reconciliation step.
 ***************************************************************************)
CONSTANT InitialTargetBinlogPos

(***************************************************************************
In the case where Ghostferry is restarted, we can restart at a particular
location saved from previously. This parameter does exactly that. This number
should be 0 if you don't care about this step.

Example: 1
 ***************************************************************************)
CONSTANT LastSuccessfulPrimaryKey

(***************************************************************************
The number of rows/binlogs that have already been copied to the target 
database. This is to emulate a case where Ghostferry is shutdown but the saved
cursor and binlog positions are out of date.

Note that RowsOvercopied must be lower than TableCapacity - 
LastSuccessfulPrimaryKey. BinlogOvercopied must be smaller than MaxBinlogSize.

Example: 1
 ***************************************************************************)
CONSTANT RowsOvercopied
CONSTANT BinlogOvercopied

(***************************************************************************
Records defines the set of possible records that can be written to the
database. The set of model values can be symmetrical because the starting order
does not matter.

Example: {r0, r1}

NoRecordHere serves as a placeholder for saying that the row with that id
does not exist in the database. This can be defined as a regular model value
in TLC.
 ***************************************************************************)
CONSTANT Records
CONSTANT NoRecordHere
ASSUME NoRecordHere \notin Records

(***************************************************************************
These are defined as ModelValues that will serve as the identifier to the
different processes running.
 ***************************************************************************)
CONSTANTS TableIterator, BinlogStreamer, Application, Ferry

\* A set of possible records for TypeOK
PossibleRecords == Records \cup {NoRecordHere}

\* A set of tables up to TableCapacity, but have different entries in them.
InitialTables == [1..TableCapacity -> PossibleRecords]

\* The set of all possible primary key
PrimaryKeys == 1..TableCapacity

\* This generates all the permutations of the rows that are changed. The number
\* of changes must equal InitialTargetBinlogSize.
RowsChanged == {key \in [1..InitialTargetBinlogPos -> 1..TableCapacity] : Cardinality(Range(key)) = InitialTargetBinlogPos}

ComputeMaxPrimaryKey(table, capacity) == IF table[capacity] # NoRecordHere
                                         THEN capacity
                                         ELSE
                                           IF \A i \in DOMAIN table : table[i] = NoRecordHere
                                           THEN 0
                                           ELSE (CHOOSE i \in DOMAIN table : \E j \in DOMAIN table : table[i] # NoRecordHere /\ table[j] = NoRecordHere /\ i = j - 1)

(***************************************************************************
--algorithm ghostferry {
  variables
    \* We can pick an InitialTable from the permutations of possible tables.
    \* The target table is initialized with the same capacity but has no records,
    \* unless LastSuccessfulPrimaryKey is set to >0, in which case the record
    \* is set to equal the ones found on the SourceTable as we have copied it
    \* previously.
    InitialTable \in InitialTables,
    SourceTable = InitialTable,
    TargetTable = [k \in PrimaryKeys |-> IF k <= LastSuccessfulPrimaryKey + RowsOvercopied THEN SourceTable[k] ELSE NoRecordHere],

    \* The binlogs are modeled as a list of binlog events.
    \* The size of the binlog is constrainted to MaxBinlogSize via
    \* ActionConstraint. This serves to ensure that the binlog do not increase
    \* infinitely. In essense, this assumes that Ghostferry is fast enough to
    \* tail and apply binlogs.
    SourceBinlog = <<>>,

    \* Set to TRUE when the cutover operation is started to prevent the
    \* application from writing more data into the database. This is equivalent
    \* to either setting the database to read only or use some method to set
    \* the application to read only.
    ApplicationReadonly = FALSE,

    \* This is the index of the binlog position we want to stream to when the
    \* application is set to read only during the cutover.
    TargetBinlogPos = 0,

    \* This is set to TRUE to stop all components of Ghostferry and Ghostferry
    \* should terminate after finishing streaming all the binlog events.
    BinlogStreamingStopRequested = FALSE,

    ChangesApplied = FALSE,
    FerryThreadsStarted = FALSE,

    \* In the actual application, we have to set this as an argument. In the
    \* model, it is always 0.
    LastSuccessfulBinlogPos = 0,

    \* MaxPrimaryKey, initialized so the application model can use it to not
    \* create holes in the source database.
    MaxPrimaryKey = ComputeMaxPrimaryKey(SourceTable, TableCapacity),
    CurrentMaxPrimaryKey = MaxPrimaryKey;

    macro DetermineMaxPrimaryKey() {
      MaxPrimaryKey := ComputeMaxPrimaryKey(SourceTable, TableCapacity);
      CurrentMaxPrimaryKey := MaxPrimaryKey;
    };

    fair process (ProcTableIterator = TableIterator)
    variables
      lastSuccessfulPK = LastSuccessfulPrimaryKey, \* Last PK successfully applied to the target db.
      currentRow;                                  \* The current row's data
    {
      \* Note that tblit_rw is an atomic step. If the read and write steps are
      \* two distinct steps, this could cause a race condition that will cause
      \* data corruption.
      \* TODO: offer a way to see this data corruption with TLC.
      \*
      \* In order to do this in the real work, this could be done via something
      \* like SELECT FOR UPDATE, which would block the Application from being
      \* to change currentRow while currentRow is being written to the target
      \* database.
      \*
      \* It may be possible to perform some sort of locking between the
      \* BinlogStreamer and the TableIterator.
      \* TODO: model this with TLA+ and validate its correctness.
      \*
      \* It may be possible to perform some sort of locking via the Application,
      \* but this seems cumbersome and prone to implementation level error.
      \* TODO: model this with TLA+ and validate its correctness.
      tblit_wait:  await FerryThreadsStarted = TRUE;
                   DetermineMaxPrimaryKey();
      tblit_loop:  while (lastSuccessfulPK < MaxPrimaryKey) {
      tblit_rw:      currentRow := SourceTable[lastSuccessfulPK + 1];
                     if (currentRow # NoRecordHere) {
                       TargetTable[lastSuccessfulPK + 1] := currentRow;
                     };
      tblit_upkey:   lastSuccessfulPK := lastSuccessfulPK + 1;
                   };
    }

    fair process (ProcBinlogStreamer = BinlogStreamer)
    variables
      currentBinlogEntry;          \* The binlog event that is currently being read
    {
      binlog_wait: await FerryThreadsStarted = TRUE;
      binlog_loop: while (BinlogStreamingStopRequested = FALSE \/ (BinlogStreamingStopRequested = TRUE /\ LastSuccessfulBinlogPos < TargetBinlogPos)) {
                     \* We cannot use an await as there could be a deadlock for
                     \* when the application is set to read only and thus nothing
                     \* else writes to the database.
                     \*
                     \* This also means in the real implementation we need a
                     \* non-blocking read for the binlog.
      binlog_read:   if (LastSuccessfulBinlogPos < Len(SourceBinlog)) {
                       currentBinlogEntry := SourceBinlog[LastSuccessfulBinlogPos + 1];
      binlog_write:    if (TargetTable[currentBinlogEntry.pk] = currentBinlogEntry.oldr) {
                         TargetTable[currentBinlogEntry.pk] := currentBinlogEntry.newr;
                       };
      binlog_upkey:    LastSuccessfulBinlogPos := LastSuccessfulBinlogPos + 1;
                     };
                   }
    }

   (***********************************************************************
    The application modeled here encompasses both the real application as
    well as the database itself. It is modeled as a process that is always
    issuing INSERT/UPDATE/DELETEs. The model also appends the record into
    the binlogs.

    The model here also models setting the database/application as read only.
    It will stop writing to the database when ApplicationReadonly = TRUE.

    The model combines the act of picking the row to update, writing to it,
    and recording the action into the binlog as one big atomic step. This
    ASSUMPTION relies upon the fact that MySQL's updates atomically writes
    to the binlog and the actual data with respect to Ghostferry.
    ***********************************************************************)
    fair process (ProcApplication = Application)
    variables
      oldRecord,
      newRecord,
      chosenPK;
    {
      app_wait: if (InitialTargetBinlogPos > 0) {
                  await ChangesApplied = TRUE;
                };
      app_loop: while (ApplicationReadonly = FALSE) {
                  \* Choose a "random" PK to update.
      app_write:  with (pk \in 1..SetMin({TableCapacity, CurrentMaxPrimaryKey + 1})) {
                    chosenPK := pk;
                  };
                  oldRecord := SourceTable[chosenPK];

                  \* Choose a "random" record to update the chosen row, except
                  \* the current value of the chosen row (oldRecord), as that
                  \* would be a pointless update and we don't need to make TLC
                  \* check that.
                  with (r \in PossibleRecords \ {oldRecord}) {
                    newRecord := r;
                  };

                  SourceBinlog := Append(
                    SourceBinlog,
                    [
                      pk |-> chosenPK,
                      oldr |-> oldRecord,
                      newr |-> newRecord
                    ]
                  );
                  SourceTable[chosenPK] := newRecord;

                  \* The following essentially implements auto_increment. We
                  \* might not necessarily need this, but there's no proof
                  \* saying that we can, thus it is included.
                  if (oldRecord = NoRecordHere /\ chosenPK > CurrentMaxPrimaryKey) {
                    assert (chosenPK - 1 = CurrentMaxPrimaryKey);
                    CurrentMaxPrimaryKey := chosenPK;
                  };
                }
    }

   (***********************************************************************
    In the model, the ferry code actually performs the changes while 
    Ghostferry is down, before running the reconciliation. This is obviously
    not needed in the real code.

    Each apllication only needs to implement these steps:

    1. Perform reconciliation if applicable.
    2. Start all threads/goroutines.
    3. Waiting until the DataIterator is finished copying data.
    4. Perform the cutover operation (setting the source to be read only).
    5. Instruct the BinlogStreamer to quit after streaming.

    Note that setting the target binlog position and requesting binlog
    streaming to stop are two distinct steps. Making them one atomic step
    is not realistic unless we implement a lock. With two distinct steps,
    if the steps are reversed, a race condition will be present.
    ***********************************************************************)
    fair process (ProcFerry = Ferry)
    variables
      currentChangeToPerform = 1,
      changedRowIds;
    {
      ferry_reconcile:  if (InitialTargetBinlogPos > 0) {
      ferry_pickchanges:  with (rowIds \in RowsChanged) {
                            changedRowIds := rowIds;
                          };

      ferry_runchanges:   while (currentChangeToPerform <= InitialTargetBinlogPos) {
                            with (pk = changedRowIds[currentChangeToPerform]) {
                              with (r \in PossibleRecords \ {SourceTable[pk]}) {
                                SourceBinlog := Append(
                                  SourceBinlog,
                                  [
                                    pk |-> pk,
                                    oldr |-> SourceTable[pk],
                                    newr |-> r
                                  ]
                                );

                                SourceTable[pk] := r;
                                if (currentChangeToPerform <= BinlogOvercopied) {
                                  TargetTable[pk] := r;
                                };
                                currentChangeToPerform := currentChangeToPerform + 1;
                              };
                            };
                          };
                          ChangesApplied := TRUE; \* Start the application inserting now

      ferry_reconc_loop:  while (LastSuccessfulBinlogPos < InitialTargetBinlogPos) {
                            LastSuccessfulBinlogPos := LastSuccessfulBinlogPos + 1;
      ferry_reconc_del:     TargetTable[SourceBinlog[LastSuccessfulBinlogPos].pk] := NoRecordHere;
      ferry_reconc_ins:     if (SourceBinlog[LastSuccessfulBinlogPos].pk <= LastSuccessfulPrimaryKey) {
                              TargetTable[SourceBinlog[LastSuccessfulBinlogPos].pk] := SourceTable[SourceBinlog[LastSuccessfulBinlogPos].pk];
                            };
                          };
                        };
      ferry_start:      FerryThreadsStarted := TRUE;
      ferry_setro:      await pc[TableIterator] = "Done";
                        ApplicationReadonly := TRUE;
      ferry_waitro:     await pc[Application] = "Done";
      ferry_binlogpos:  TargetBinlogPos := Len(SourceBinlog);
      ferry_binlogstop: BinlogStreamingStopRequested := TRUE;
    }
}

 ***************************************************************************)
\* BEGIN TRANSLATION
CONSTANT defaultInitValue
VARIABLES InitialTable, SourceTable, TargetTable, SourceBinlog, 
          ApplicationReadonly, TargetBinlogPos, BinlogStreamingStopRequested, 
          ChangesApplied, FerryThreadsStarted, LastSuccessfulBinlogPos, 
          MaxPrimaryKey, CurrentMaxPrimaryKey, pc, lastSuccessfulPK, 
          currentRow, currentBinlogEntry, oldRecord, newRecord, chosenPK, 
          currentChangeToPerform, changedRowIds

vars == << InitialTable, SourceTable, TargetTable, SourceBinlog, 
           ApplicationReadonly, TargetBinlogPos, BinlogStreamingStopRequested, 
           ChangesApplied, FerryThreadsStarted, LastSuccessfulBinlogPos, 
           MaxPrimaryKey, CurrentMaxPrimaryKey, pc, lastSuccessfulPK, 
           currentRow, currentBinlogEntry, oldRecord, newRecord, chosenPK, 
           currentChangeToPerform, changedRowIds >>

ProcSet == {TableIterator} \cup {BinlogStreamer} \cup {Application} \cup {Ferry}

Init == (* Global variables *)
        /\ InitialTable \in InitialTables
        /\ SourceTable = InitialTable
        /\ TargetTable = [k \in PrimaryKeys |-> IF k <= LastSuccessfulPrimaryKey + RowsOvercopied THEN SourceTable[k] ELSE NoRecordHere]
        /\ SourceBinlog = <<>>
        /\ ApplicationReadonly = FALSE
        /\ TargetBinlogPos = 0
        /\ BinlogStreamingStopRequested = FALSE
        /\ ChangesApplied = FALSE
        /\ FerryThreadsStarted = FALSE
        /\ LastSuccessfulBinlogPos = 0
        /\ MaxPrimaryKey = ComputeMaxPrimaryKey(SourceTable, TableCapacity)
        /\ CurrentMaxPrimaryKey = MaxPrimaryKey
        (* Process ProcTableIterator *)
        /\ lastSuccessfulPK = LastSuccessfulPrimaryKey
        /\ currentRow = defaultInitValue
        (* Process ProcBinlogStreamer *)
        /\ currentBinlogEntry = defaultInitValue
        (* Process ProcApplication *)
        /\ oldRecord = defaultInitValue
        /\ newRecord = defaultInitValue
        /\ chosenPK = defaultInitValue
        (* Process ProcFerry *)
        /\ currentChangeToPerform = 1
        /\ changedRowIds = defaultInitValue
        /\ pc = [self \in ProcSet |-> CASE self = TableIterator -> "tblit_wait"
                                        [] self = BinlogStreamer -> "binlog_wait"
                                        [] self = Application -> "app_wait"
                                        [] self = Ferry -> "ferry_reconcile"]

tblit_wait == /\ pc[TableIterator] = "tblit_wait"
              /\ FerryThreadsStarted = TRUE
              /\ MaxPrimaryKey' = ComputeMaxPrimaryKey(SourceTable, TableCapacity)
              /\ CurrentMaxPrimaryKey' = MaxPrimaryKey'
              /\ pc' = [pc EXCEPT ![TableIterator] = "tblit_loop"]
              /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                              SourceBinlog, ApplicationReadonly, 
                              TargetBinlogPos, BinlogStreamingStopRequested, 
                              ChangesApplied, FerryThreadsStarted, 
                              LastSuccessfulBinlogPos, lastSuccessfulPK, 
                              currentRow, currentBinlogEntry, oldRecord, 
                              newRecord, chosenPK, currentChangeToPerform, 
                              changedRowIds >>

tblit_loop == /\ pc[TableIterator] = "tblit_loop"
              /\ IF lastSuccessfulPK < MaxPrimaryKey
                    THEN /\ pc' = [pc EXCEPT ![TableIterator] = "tblit_rw"]
                    ELSE /\ pc' = [pc EXCEPT ![TableIterator] = "Done"]
              /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                              SourceBinlog, ApplicationReadonly, 
                              TargetBinlogPos, BinlogStreamingStopRequested, 
                              ChangesApplied, FerryThreadsStarted, 
                              LastSuccessfulBinlogPos, MaxPrimaryKey, 
                              CurrentMaxPrimaryKey, lastSuccessfulPK, 
                              currentRow, currentBinlogEntry, oldRecord, 
                              newRecord, chosenPK, currentChangeToPerform, 
                              changedRowIds >>

tblit_rw == /\ pc[TableIterator] = "tblit_rw"
            /\ currentRow' = SourceTable[lastSuccessfulPK + 1]
            /\ IF currentRow' # NoRecordHere
                  THEN /\ TargetTable' = [TargetTable EXCEPT ![lastSuccessfulPK + 1] = currentRow']
                  ELSE /\ TRUE
                       /\ UNCHANGED TargetTable
            /\ pc' = [pc EXCEPT ![TableIterator] = "tblit_upkey"]
            /\ UNCHANGED << InitialTable, SourceTable, SourceBinlog, 
                            ApplicationReadonly, TargetBinlogPos, 
                            BinlogStreamingStopRequested, ChangesApplied, 
                            FerryThreadsStarted, LastSuccessfulBinlogPos, 
                            MaxPrimaryKey, CurrentMaxPrimaryKey, 
                            lastSuccessfulPK, currentBinlogEntry, oldRecord, 
                            newRecord, chosenPK, currentChangeToPerform, 
                            changedRowIds >>

tblit_upkey == /\ pc[TableIterator] = "tblit_upkey"
               /\ lastSuccessfulPK' = lastSuccessfulPK + 1
               /\ pc' = [pc EXCEPT ![TableIterator] = "tblit_loop"]
               /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                               SourceBinlog, ApplicationReadonly, 
                               TargetBinlogPos, BinlogStreamingStopRequested, 
                               ChangesApplied, FerryThreadsStarted, 
                               LastSuccessfulBinlogPos, MaxPrimaryKey, 
                               CurrentMaxPrimaryKey, currentRow, 
                               currentBinlogEntry, oldRecord, newRecord, 
                               chosenPK, currentChangeToPerform, changedRowIds >>

ProcTableIterator == tblit_wait \/ tblit_loop \/ tblit_rw \/ tblit_upkey

binlog_wait == /\ pc[BinlogStreamer] = "binlog_wait"
               /\ FerryThreadsStarted = TRUE
               /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_loop"]
               /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                               SourceBinlog, ApplicationReadonly, 
                               TargetBinlogPos, BinlogStreamingStopRequested, 
                               ChangesApplied, FerryThreadsStarted, 
                               LastSuccessfulBinlogPos, MaxPrimaryKey, 
                               CurrentMaxPrimaryKey, lastSuccessfulPK, 
                               currentRow, currentBinlogEntry, oldRecord, 
                               newRecord, chosenPK, currentChangeToPerform, 
                               changedRowIds >>

binlog_loop == /\ pc[BinlogStreamer] = "binlog_loop"
               /\ IF BinlogStreamingStopRequested = FALSE \/ (BinlogStreamingStopRequested = TRUE /\ LastSuccessfulBinlogPos < TargetBinlogPos)
                     THEN /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_read"]
                     ELSE /\ pc' = [pc EXCEPT ![BinlogStreamer] = "Done"]
               /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                               SourceBinlog, ApplicationReadonly, 
                               TargetBinlogPos, BinlogStreamingStopRequested, 
                               ChangesApplied, FerryThreadsStarted, 
                               LastSuccessfulBinlogPos, MaxPrimaryKey, 
                               CurrentMaxPrimaryKey, lastSuccessfulPK, 
                               currentRow, currentBinlogEntry, oldRecord, 
                               newRecord, chosenPK, currentChangeToPerform, 
                               changedRowIds >>

binlog_read == /\ pc[BinlogStreamer] = "binlog_read"
               /\ IF LastSuccessfulBinlogPos < Len(SourceBinlog)
                     THEN /\ currentBinlogEntry' = SourceBinlog[LastSuccessfulBinlogPos + 1]
                          /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_write"]
                     ELSE /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_loop"]
                          /\ UNCHANGED currentBinlogEntry
               /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                               SourceBinlog, ApplicationReadonly, 
                               TargetBinlogPos, BinlogStreamingStopRequested, 
                               ChangesApplied, FerryThreadsStarted, 
                               LastSuccessfulBinlogPos, MaxPrimaryKey, 
                               CurrentMaxPrimaryKey, lastSuccessfulPK, 
                               currentRow, oldRecord, newRecord, chosenPK, 
                               currentChangeToPerform, changedRowIds >>

binlog_write == /\ pc[BinlogStreamer] = "binlog_write"
                /\ IF TargetTable[currentBinlogEntry.pk] = currentBinlogEntry.oldr
                      THEN /\ TargetTable' = [TargetTable EXCEPT ![currentBinlogEntry.pk] = currentBinlogEntry.newr]
                      ELSE /\ TRUE
                           /\ UNCHANGED TargetTable
                /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_upkey"]
                /\ UNCHANGED << InitialTable, SourceTable, SourceBinlog, 
                                ApplicationReadonly, TargetBinlogPos, 
                                BinlogStreamingStopRequested, ChangesApplied, 
                                FerryThreadsStarted, LastSuccessfulBinlogPos, 
                                MaxPrimaryKey, CurrentMaxPrimaryKey, 
                                lastSuccessfulPK, currentRow, 
                                currentBinlogEntry, oldRecord, newRecord, 
                                chosenPK, currentChangeToPerform, 
                                changedRowIds >>

binlog_upkey == /\ pc[BinlogStreamer] = "binlog_upkey"
                /\ LastSuccessfulBinlogPos' = LastSuccessfulBinlogPos + 1
                /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_loop"]
                /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                                SourceBinlog, ApplicationReadonly, 
                                TargetBinlogPos, BinlogStreamingStopRequested, 
                                ChangesApplied, FerryThreadsStarted, 
                                MaxPrimaryKey, CurrentMaxPrimaryKey, 
                                lastSuccessfulPK, currentRow, 
                                currentBinlogEntry, oldRecord, newRecord, 
                                chosenPK, currentChangeToPerform, 
                                changedRowIds >>

ProcBinlogStreamer == binlog_wait \/ binlog_loop \/ binlog_read
                         \/ binlog_write \/ binlog_upkey

app_wait == /\ pc[Application] = "app_wait"
            /\ IF InitialTargetBinlogPos > 0
                  THEN /\ ChangesApplied = TRUE
                  ELSE /\ TRUE
            /\ pc' = [pc EXCEPT ![Application] = "app_loop"]
            /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                            SourceBinlog, ApplicationReadonly, TargetBinlogPos, 
                            BinlogStreamingStopRequested, ChangesApplied, 
                            FerryThreadsStarted, LastSuccessfulBinlogPos, 
                            MaxPrimaryKey, CurrentMaxPrimaryKey, 
                            lastSuccessfulPK, currentRow, currentBinlogEntry, 
                            oldRecord, newRecord, chosenPK, 
                            currentChangeToPerform, changedRowIds >>

app_loop == /\ pc[Application] = "app_loop"
            /\ IF ApplicationReadonly = FALSE
                  THEN /\ pc' = [pc EXCEPT ![Application] = "app_write"]
                  ELSE /\ pc' = [pc EXCEPT ![Application] = "Done"]
            /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                            SourceBinlog, ApplicationReadonly, TargetBinlogPos, 
                            BinlogStreamingStopRequested, ChangesApplied, 
                            FerryThreadsStarted, LastSuccessfulBinlogPos, 
                            MaxPrimaryKey, CurrentMaxPrimaryKey, 
                            lastSuccessfulPK, currentRow, currentBinlogEntry, 
                            oldRecord, newRecord, chosenPK, 
                            currentChangeToPerform, changedRowIds >>

app_write == /\ pc[Application] = "app_write"
             /\ \E pk \in 1..SetMin({TableCapacity, CurrentMaxPrimaryKey + 1}):
                  chosenPK' = pk
             /\ oldRecord' = SourceTable[chosenPK']
             /\ \E r \in PossibleRecords \ {oldRecord'}:
                  newRecord' = r
             /\ SourceBinlog' =                 Append(
                                  SourceBinlog,
                                  [
                                    pk |-> chosenPK',
                                    oldr |-> oldRecord',
                                    newr |-> newRecord'
                                  ]
                                )
             /\ SourceTable' = [SourceTable EXCEPT ![chosenPK'] = newRecord']
             /\ IF oldRecord' = NoRecordHere /\ chosenPK' > CurrentMaxPrimaryKey
                   THEN /\ Assert((chosenPK' - 1 = CurrentMaxPrimaryKey), 
                                  "Failure of assertion at line 430, column 21.")
                        /\ CurrentMaxPrimaryKey' = chosenPK'
                   ELSE /\ TRUE
                        /\ UNCHANGED CurrentMaxPrimaryKey
             /\ pc' = [pc EXCEPT ![Application] = "app_loop"]
             /\ UNCHANGED << InitialTable, TargetTable, ApplicationReadonly, 
                             TargetBinlogPos, BinlogStreamingStopRequested, 
                             ChangesApplied, FerryThreadsStarted, 
                             LastSuccessfulBinlogPos, MaxPrimaryKey, 
                             lastSuccessfulPK, currentRow, currentBinlogEntry, 
                             currentChangeToPerform, changedRowIds >>

ProcApplication == app_wait \/ app_loop \/ app_write

ferry_reconcile == /\ pc[Ferry] = "ferry_reconcile"
                   /\ IF InitialTargetBinlogPos > 0
                         THEN /\ pc' = [pc EXCEPT ![Ferry] = "ferry_pickchanges"]
                         ELSE /\ pc' = [pc EXCEPT ![Ferry] = "ferry_start"]
                   /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                                   SourceBinlog, ApplicationReadonly, 
                                   TargetBinlogPos, 
                                   BinlogStreamingStopRequested, 
                                   ChangesApplied, FerryThreadsStarted, 
                                   LastSuccessfulBinlogPos, MaxPrimaryKey, 
                                   CurrentMaxPrimaryKey, lastSuccessfulPK, 
                                   currentRow, currentBinlogEntry, oldRecord, 
                                   newRecord, chosenPK, currentChangeToPerform, 
                                   changedRowIds >>

ferry_pickchanges == /\ pc[Ferry] = "ferry_pickchanges"
                     /\ \E rowIds \in RowsChanged:
                          changedRowIds' = rowIds
                     /\ pc' = [pc EXCEPT ![Ferry] = "ferry_runchanges"]
                     /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                                     SourceBinlog, ApplicationReadonly, 
                                     TargetBinlogPos, 
                                     BinlogStreamingStopRequested, 
                                     ChangesApplied, FerryThreadsStarted, 
                                     LastSuccessfulBinlogPos, MaxPrimaryKey, 
                                     CurrentMaxPrimaryKey, lastSuccessfulPK, 
                                     currentRow, currentBinlogEntry, oldRecord, 
                                     newRecord, chosenPK, 
                                     currentChangeToPerform >>

ferry_runchanges == /\ pc[Ferry] = "ferry_runchanges"
                    /\ IF currentChangeToPerform <= InitialTargetBinlogPos
                          THEN /\ LET pk == changedRowIds[currentChangeToPerform] IN
                                    \E r \in PossibleRecords \ {SourceTable[pk]}:
                                      /\ SourceBinlog' =                 Append(
                                                           SourceBinlog,
                                                           [
                                                             pk |-> pk,
                                                             oldr |-> SourceTable[pk],
                                                             newr |-> r
                                                           ]
                                                         )
                                      /\ SourceTable' = [SourceTable EXCEPT ![pk] = r]
                                      /\ IF currentChangeToPerform <= BinlogOvercopied
                                            THEN /\ TargetTable' = [TargetTable EXCEPT ![pk] = r]
                                            ELSE /\ TRUE
                                                 /\ UNCHANGED TargetTable
                                      /\ currentChangeToPerform' = currentChangeToPerform + 1
                               /\ pc' = [pc EXCEPT ![Ferry] = "ferry_runchanges"]
                               /\ UNCHANGED ChangesApplied
                          ELSE /\ ChangesApplied' = TRUE
                               /\ pc' = [pc EXCEPT ![Ferry] = "ferry_reconc_loop"]
                               /\ UNCHANGED << SourceTable, TargetTable, 
                                               SourceBinlog, 
                                               currentChangeToPerform >>
                    /\ UNCHANGED << InitialTable, ApplicationReadonly, 
                                    TargetBinlogPos, 
                                    BinlogStreamingStopRequested, 
                                    FerryThreadsStarted, 
                                    LastSuccessfulBinlogPos, MaxPrimaryKey, 
                                    CurrentMaxPrimaryKey, lastSuccessfulPK, 
                                    currentRow, currentBinlogEntry, oldRecord, 
                                    newRecord, chosenPK, changedRowIds >>

ferry_reconc_loop == /\ pc[Ferry] = "ferry_reconc_loop"
                     /\ IF LastSuccessfulBinlogPos < InitialTargetBinlogPos
                           THEN /\ LastSuccessfulBinlogPos' = LastSuccessfulBinlogPos + 1
                                /\ pc' = [pc EXCEPT ![Ferry] = "ferry_reconc_del"]
                           ELSE /\ pc' = [pc EXCEPT ![Ferry] = "ferry_start"]
                                /\ UNCHANGED LastSuccessfulBinlogPos
                     /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                                     SourceBinlog, ApplicationReadonly, 
                                     TargetBinlogPos, 
                                     BinlogStreamingStopRequested, 
                                     ChangesApplied, FerryThreadsStarted, 
                                     MaxPrimaryKey, CurrentMaxPrimaryKey, 
                                     lastSuccessfulPK, currentRow, 
                                     currentBinlogEntry, oldRecord, newRecord, 
                                     chosenPK, currentChangeToPerform, 
                                     changedRowIds >>

ferry_reconc_del == /\ pc[Ferry] = "ferry_reconc_del"
                    /\ TargetTable' = [TargetTable EXCEPT ![SourceBinlog[LastSuccessfulBinlogPos].pk] = NoRecordHere]
                    /\ pc' = [pc EXCEPT ![Ferry] = "ferry_reconc_ins"]
                    /\ UNCHANGED << InitialTable, SourceTable, SourceBinlog, 
                                    ApplicationReadonly, TargetBinlogPos, 
                                    BinlogStreamingStopRequested, 
                                    ChangesApplied, FerryThreadsStarted, 
                                    LastSuccessfulBinlogPos, MaxPrimaryKey, 
                                    CurrentMaxPrimaryKey, lastSuccessfulPK, 
                                    currentRow, currentBinlogEntry, oldRecord, 
                                    newRecord, chosenPK, 
                                    currentChangeToPerform, changedRowIds >>

ferry_reconc_ins == /\ pc[Ferry] = "ferry_reconc_ins"
                    /\ IF SourceBinlog[LastSuccessfulBinlogPos].pk <= LastSuccessfulPrimaryKey
                          THEN /\ TargetTable' = [TargetTable EXCEPT ![SourceBinlog[LastSuccessfulBinlogPos].pk] = SourceTable[SourceBinlog[LastSuccessfulBinlogPos].pk]]
                          ELSE /\ TRUE
                               /\ UNCHANGED TargetTable
                    /\ pc' = [pc EXCEPT ![Ferry] = "ferry_reconc_loop"]
                    /\ UNCHANGED << InitialTable, SourceTable, SourceBinlog, 
                                    ApplicationReadonly, TargetBinlogPos, 
                                    BinlogStreamingStopRequested, 
                                    ChangesApplied, FerryThreadsStarted, 
                                    LastSuccessfulBinlogPos, MaxPrimaryKey, 
                                    CurrentMaxPrimaryKey, lastSuccessfulPK, 
                                    currentRow, currentBinlogEntry, oldRecord, 
                                    newRecord, chosenPK, 
                                    currentChangeToPerform, changedRowIds >>

ferry_start == /\ pc[Ferry] = "ferry_start"
               /\ FerryThreadsStarted' = TRUE
               /\ pc' = [pc EXCEPT ![Ferry] = "ferry_setro"]
               /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                               SourceBinlog, ApplicationReadonly, 
                               TargetBinlogPos, BinlogStreamingStopRequested, 
                               ChangesApplied, LastSuccessfulBinlogPos, 
                               MaxPrimaryKey, CurrentMaxPrimaryKey, 
                               lastSuccessfulPK, currentRow, 
                               currentBinlogEntry, oldRecord, newRecord, 
                               chosenPK, currentChangeToPerform, changedRowIds >>

ferry_setro == /\ pc[Ferry] = "ferry_setro"
               /\ pc[TableIterator] = "Done"
               /\ ApplicationReadonly' = TRUE
               /\ pc' = [pc EXCEPT ![Ferry] = "ferry_waitro"]
               /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                               SourceBinlog, TargetBinlogPos, 
                               BinlogStreamingStopRequested, ChangesApplied, 
                               FerryThreadsStarted, LastSuccessfulBinlogPos, 
                               MaxPrimaryKey, CurrentMaxPrimaryKey, 
                               lastSuccessfulPK, currentRow, 
                               currentBinlogEntry, oldRecord, newRecord, 
                               chosenPK, currentChangeToPerform, changedRowIds >>

ferry_waitro == /\ pc[Ferry] = "ferry_waitro"
                /\ pc[Application] = "Done"
                /\ pc' = [pc EXCEPT ![Ferry] = "ferry_binlogpos"]
                /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                                SourceBinlog, ApplicationReadonly, 
                                TargetBinlogPos, BinlogStreamingStopRequested, 
                                ChangesApplied, FerryThreadsStarted, 
                                LastSuccessfulBinlogPos, MaxPrimaryKey, 
                                CurrentMaxPrimaryKey, lastSuccessfulPK, 
                                currentRow, currentBinlogEntry, oldRecord, 
                                newRecord, chosenPK, currentChangeToPerform, 
                                changedRowIds >>

ferry_binlogpos == /\ pc[Ferry] = "ferry_binlogpos"
                   /\ TargetBinlogPos' = Len(SourceBinlog)
                   /\ pc' = [pc EXCEPT ![Ferry] = "ferry_binlogstop"]
                   /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                                   SourceBinlog, ApplicationReadonly, 
                                   BinlogStreamingStopRequested, 
                                   ChangesApplied, FerryThreadsStarted, 
                                   LastSuccessfulBinlogPos, MaxPrimaryKey, 
                                   CurrentMaxPrimaryKey, lastSuccessfulPK, 
                                   currentRow, currentBinlogEntry, oldRecord, 
                                   newRecord, chosenPK, currentChangeToPerform, 
                                   changedRowIds >>

ferry_binlogstop == /\ pc[Ferry] = "ferry_binlogstop"
                    /\ BinlogStreamingStopRequested' = TRUE
                    /\ pc' = [pc EXCEPT ![Ferry] = "Done"]
                    /\ UNCHANGED << InitialTable, SourceTable, TargetTable, 
                                    SourceBinlog, ApplicationReadonly, 
                                    TargetBinlogPos, ChangesApplied, 
                                    FerryThreadsStarted, 
                                    LastSuccessfulBinlogPos, MaxPrimaryKey, 
                                    CurrentMaxPrimaryKey, lastSuccessfulPK, 
                                    currentRow, currentBinlogEntry, oldRecord, 
                                    newRecord, chosenPK, 
                                    currentChangeToPerform, changedRowIds >>

ProcFerry == ferry_reconcile \/ ferry_pickchanges \/ ferry_runchanges
                \/ ferry_reconc_loop \/ ferry_reconc_del
                \/ ferry_reconc_ins \/ ferry_start \/ ferry_setro
                \/ ferry_waitro \/ ferry_binlogpos \/ ferry_binlogstop

Next == ProcTableIterator \/ ProcBinlogStreamer \/ ProcApplication
           \/ ProcFerry
           \/ (* Disjunct to prevent deadlock on termination *)
              ((\A self \in ProcSet: pc[self] = "Done") /\ UNCHANGED vars)

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(ProcTableIterator)
        /\ WF_vars(ProcBinlogStreamer)
        /\ WF_vars(ProcApplication)
        /\ WF_vars(ProcFerry)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

\* Safety Constraints
\* ==================

SourceTargetEquality == (\A self \in ProcSet: pc[self] = "Done") => (SourceTable = TargetTable)

\* Action Constraints
\* ==================

\* It is possible that the binlog infinitely grows. If it becomes too big it
\* becomes infeasible to check the behaviour of the ghostferry algorithm. Thus
\* we limit it.
\*
\* Usually we can pick a MaxBinlogSize == 3.
CONSTANT MaxBinlogSize

BinlogSizeActionConstraint == Len(SourceBinlog) <= MaxBinlogSize

=============================================================================
\* Modification History
\* Last modified Mon Jul 09 10:43:17 EDT 2018 by shuhao
\* Created Thu Jan 18 11:35:40 EST 2018 by shuhao
