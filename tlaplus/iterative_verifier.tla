------------------------- MODULE iterative_verifier -------------------------

EXTENDS Integers, Sequences, TLC

SetMin(S) == CHOOSE i \in S: \A j \in S : i <= j

CONSTANT InitialTable
CONSTANT MaxPrimaryKey

CONSTANT Records

CONSTANTS TableIterator, BinlogStreamer, Application, Ferry

TableCapacity == Len(InitialTable)

PrimaryKeys == 1..TableCapacity

NoRecordHere == CHOOSE r : r \notin Records

PossibleRecords == Records \cup {NoRecordHere}

(***************************************************************************
--algorithm ghostferry {
  variables
    CurrentMaxPrimaryKey = MaxPrimaryKey,
    SourceTable = InitialTable,
    TargetTable = [k \in PrimaryKeys |-> NoRecordHere],
    SourceBinlog = <<>>,
    ApplicationReadonly = FALSE,
    TargetBinlogPos = 0,
    BinlogStreamingStopRequested = FALSE,
    
    \* added variables to verify
    BinlogStreamerEmitReverifyEvent = FALSE,
    ReverifyStore = <<>>,
    VerificationFailed = FALSE;

  macro EmitReverifyEvent(pk) {
    ReverifyStore := Append(ReverifyStore, pk);
  }

  procedure VerifyBeforeCutover()
  variables
    lastVerifiedPK = 0,
    sourceRow,
    targetRow;
  {
    verify_before_init:     BinlogStreamerEmitReverifyEvent := TRUE;
    verify_before_loop:     while (lastVerifiedPK < TableCapacity) {
    verify_before_read_src:   sourceRow := SourceTable[lastVerifiedPK + 1];
    verify_before_read_tgt:   targetRow := TargetTable[lastVerifiedPK + 1];
    verify_before_compare:    if (sourceRow # targetRow) {
    verify_before_emit:         EmitReverifyEvent(lastVerifiedPK + 1);
                              };
    verify_before_upkey:      lastVerifiedPK := lastVerifiedPK + 1;
                            };
    verify_before_exit:     return;
  }
  
  procedure VerifyDuringCutover()
  variables
    reverifyStoreIndex = 0,
    sourceRow,
    targetRow;
  {
    verify_during_stop_emit:   BinlogStreamerEmitReverifyEvent := FALSE;
    verify_during_cutover:     while(reverifyStoreIndex < Len(ReverifyStore)) {
                                 sourceRow := SourceTable[ReverifyStore[reverifyStoreIndex + 1]];
                                 targetRow := TargetTable[ReverifyStore[reverifyStoreIndex + 1]];
                                 if (sourceRow # targetRow) {
                                   VerificationFailed := TRUE;
                                 };
                                 reverifyStoreIndex := reverifyStoreIndex + 1;
                               };
   verify_during_cutover_exit: return;
  }

  \* This table iterator will fail to copy data
  fair process (ProcTableIterator = TableIterator)
  variables
    lastSuccessfulPK = 0;
  {
    tblit_loop:  while (lastSuccessfulPK < MaxPrimaryKey) {
    tblit_rw:      \* Instead of reading and writing the correct entry, we just
                   \* Update the entry on the table to _some_ entry, correct or
                   \* not.
                   with (r \in Records) {
                     TargetTable[lastSuccessfulPK + 1] := r;
                   };
    tblit_upkey:   lastSuccessfulPK := lastSuccessfulPK + 1;
                 };
  }

  fair process (ProcBinlogStreamer = BinlogStreamer)
  variables
    lastSuccessfulBinlogPos = 0,
    currentBinlogEntry;
  {
    binlog_loop: while (BinlogStreamingStopRequested = FALSE \/ (BinlogStreamingStopRequested = TRUE /\ lastSuccessfulBinlogPos < TargetBinlogPos)) {
    binlog_read:   if (lastSuccessfulBinlogPos < Len(SourceBinlog)) {
                     currentBinlogEntry := SourceBinlog[lastSuccessfulBinlogPos + 1];
    binlog_write:    if (TargetTable[currentBinlogEntry.pk] = currentBinlogEntry.oldr) {
                       TargetTable[currentBinlogEntry.pk] := currentBinlogEntry.newr;
    binlog_reveri_ev:  if (BinlogStreamerEmitReverifyEvent = TRUE) {
                         EmitReverifyEvent(currentBinlogEntry.pk);
                       };
                     };
    binlog_upkey:    lastSuccessfulBinlogPos := lastSuccessfulBinlogPos + 1;
                   };
                 }
  }
  
  fair process (ProcApplication = Application)
  variables
    oldRecord,
    newRecord,
    chosenPK,
  {
    app_loop: while (ApplicationReadonly = FALSE) {
    app_write:  with (pk \in 1..SetMin({TableCapacity, CurrentMaxPrimaryKey + 1})) {
                  chosenPK := pk;
                };
                oldRecord := SourceTable[chosenPK];

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

                if (oldRecord = NoRecordHere /\ chosenPK > CurrentMaxPrimaryKey) {
                  assert (chosenPK - 1 = CurrentMaxPrimaryKey);
                  CurrentMaxPrimaryKey := chosenPK;
                }
              };
  }

  fair process (ProcFerry = Ferry) {
    ferry_precutover: await pc[TableIterator] = "Done";
                      call VerifyBeforeCutover();
    ferry_setro:      ApplicationReadonly := TRUE;
    ferry_waitro:     await pc[Application] = "Done";
    ferry_binlogpos:  TargetBinlogPos := Len(SourceBinlog);
    ferry_binlogstop: BinlogStreamingStopRequested := TRUE;
    ferry_verify:     await pc[BinlogStreamer] = "Done";
                      call VerifyDuringCutover();
  }
}
 ***************************************************************************)
\* BEGIN TRANSLATION
\* Procedure variable sourceRow of procedure VerifyBeforeCutover at line 45 col 5 changed to sourceRow_
\* Procedure variable targetRow of procedure VerifyBeforeCutover at line 46 col 5 changed to targetRow_
CONSTANT defaultInitValue
VARIABLES CurrentMaxPrimaryKey, SourceTable, TargetTable, SourceBinlog, 
          ApplicationReadonly, TargetBinlogPos, BinlogStreamingStopRequested, 
          BinlogStreamerEmitReverifyEvent, ReverifyStore, VerificationFailed, 
          pc, stack, lastVerifiedPK, sourceRow_, targetRow_, 
          reverifyStoreIndex, sourceRow, targetRow, lastSuccessfulPK, 
          lastSuccessfulBinlogPos, currentBinlogEntry, oldRecord, newRecord, 
          chosenPK

vars == << CurrentMaxPrimaryKey, SourceTable, TargetTable, SourceBinlog, 
           ApplicationReadonly, TargetBinlogPos, BinlogStreamingStopRequested, 
           BinlogStreamerEmitReverifyEvent, ReverifyStore, VerificationFailed, 
           pc, stack, lastVerifiedPK, sourceRow_, targetRow_, 
           reverifyStoreIndex, sourceRow, targetRow, lastSuccessfulPK, 
           lastSuccessfulBinlogPos, currentBinlogEntry, oldRecord, newRecord, 
           chosenPK >>

ProcSet == {TableIterator} \cup {BinlogStreamer} \cup {Application} \cup {Ferry}

Init == (* Global variables *)
        /\ CurrentMaxPrimaryKey = MaxPrimaryKey
        /\ SourceTable = InitialTable
        /\ TargetTable = [k \in PrimaryKeys |-> NoRecordHere]
        /\ SourceBinlog = <<>>
        /\ ApplicationReadonly = FALSE
        /\ TargetBinlogPos = 0
        /\ BinlogStreamingStopRequested = FALSE
        /\ BinlogStreamerEmitReverifyEvent = FALSE
        /\ ReverifyStore = <<>>
        /\ VerificationFailed = FALSE
        (* Procedure VerifyBeforeCutover *)
        /\ lastVerifiedPK = [ self \in ProcSet |-> 0]
        /\ sourceRow_ = [ self \in ProcSet |-> defaultInitValue]
        /\ targetRow_ = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure VerifyDuringCutover *)
        /\ reverifyStoreIndex = [ self \in ProcSet |-> 0]
        /\ sourceRow = [ self \in ProcSet |-> defaultInitValue]
        /\ targetRow = [ self \in ProcSet |-> defaultInitValue]
        (* Process ProcTableIterator *)
        /\ lastSuccessfulPK = 0
        (* Process ProcBinlogStreamer *)
        /\ lastSuccessfulBinlogPos = 0
        /\ currentBinlogEntry = defaultInitValue
        (* Process ProcApplication *)
        /\ oldRecord = defaultInitValue
        /\ newRecord = defaultInitValue
        /\ chosenPK = defaultInitValue
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self = TableIterator -> "tblit_loop"
                                        [] self = BinlogStreamer -> "binlog_loop"
                                        [] self = Application -> "app_loop"
                                        [] self = Ferry -> "ferry_precutover"]

verify_before_init(self) == /\ pc[self] = "verify_before_init"
                            /\ BinlogStreamerEmitReverifyEvent' = TRUE
                            /\ pc' = [pc EXCEPT ![self] = "verify_before_loop"]
                            /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, 
                                            TargetTable, SourceBinlog, 
                                            ApplicationReadonly, 
                                            TargetBinlogPos, 
                                            BinlogStreamingStopRequested, 
                                            ReverifyStore, VerificationFailed, 
                                            stack, lastVerifiedPK, sourceRow_, 
                                            targetRow_, reverifyStoreIndex, 
                                            sourceRow, targetRow, 
                                            lastSuccessfulPK, 
                                            lastSuccessfulBinlogPos, 
                                            currentBinlogEntry, oldRecord, 
                                            newRecord, chosenPK >>

verify_before_loop(self) == /\ pc[self] = "verify_before_loop"
                            /\ IF lastVerifiedPK[self] < TableCapacity
                                  THEN /\ pc' = [pc EXCEPT ![self] = "verify_before_read_src"]
                                  ELSE /\ pc' = [pc EXCEPT ![self] = "verify_before_exit"]
                            /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, 
                                            TargetTable, SourceBinlog, 
                                            ApplicationReadonly, 
                                            TargetBinlogPos, 
                                            BinlogStreamingStopRequested, 
                                            BinlogStreamerEmitReverifyEvent, 
                                            ReverifyStore, VerificationFailed, 
                                            stack, lastVerifiedPK, sourceRow_, 
                                            targetRow_, reverifyStoreIndex, 
                                            sourceRow, targetRow, 
                                            lastSuccessfulPK, 
                                            lastSuccessfulBinlogPos, 
                                            currentBinlogEntry, oldRecord, 
                                            newRecord, chosenPK >>

verify_before_read_src(self) == /\ pc[self] = "verify_before_read_src"
                                /\ sourceRow_' = [sourceRow_ EXCEPT ![self] = SourceTable[lastVerifiedPK[self] + 1]]
                                /\ pc' = [pc EXCEPT ![self] = "verify_before_read_tgt"]
                                /\ UNCHANGED << CurrentMaxPrimaryKey, 
                                                SourceTable, TargetTable, 
                                                SourceBinlog, 
                                                ApplicationReadonly, 
                                                TargetBinlogPos, 
                                                BinlogStreamingStopRequested, 
                                                BinlogStreamerEmitReverifyEvent, 
                                                ReverifyStore, 
                                                VerificationFailed, stack, 
                                                lastVerifiedPK, targetRow_, 
                                                reverifyStoreIndex, sourceRow, 
                                                targetRow, lastSuccessfulPK, 
                                                lastSuccessfulBinlogPos, 
                                                currentBinlogEntry, oldRecord, 
                                                newRecord, chosenPK >>

verify_before_read_tgt(self) == /\ pc[self] = "verify_before_read_tgt"
                                /\ targetRow_' = [targetRow_ EXCEPT ![self] = TargetTable[lastVerifiedPK[self] + 1]]
                                /\ pc' = [pc EXCEPT ![self] = "verify_before_compare"]
                                /\ UNCHANGED << CurrentMaxPrimaryKey, 
                                                SourceTable, TargetTable, 
                                                SourceBinlog, 
                                                ApplicationReadonly, 
                                                TargetBinlogPos, 
                                                BinlogStreamingStopRequested, 
                                                BinlogStreamerEmitReverifyEvent, 
                                                ReverifyStore, 
                                                VerificationFailed, stack, 
                                                lastVerifiedPK, sourceRow_, 
                                                reverifyStoreIndex, sourceRow, 
                                                targetRow, lastSuccessfulPK, 
                                                lastSuccessfulBinlogPos, 
                                                currentBinlogEntry, oldRecord, 
                                                newRecord, chosenPK >>

verify_before_compare(self) == /\ pc[self] = "verify_before_compare"
                               /\ IF sourceRow_[self] # targetRow_[self]
                                     THEN /\ pc' = [pc EXCEPT ![self] = "verify_before_emit"]
                                     ELSE /\ pc' = [pc EXCEPT ![self] = "verify_before_upkey"]
                               /\ UNCHANGED << CurrentMaxPrimaryKey, 
                                               SourceTable, TargetTable, 
                                               SourceBinlog, 
                                               ApplicationReadonly, 
                                               TargetBinlogPos, 
                                               BinlogStreamingStopRequested, 
                                               BinlogStreamerEmitReverifyEvent, 
                                               ReverifyStore, 
                                               VerificationFailed, stack, 
                                               lastVerifiedPK, sourceRow_, 
                                               targetRow_, reverifyStoreIndex, 
                                               sourceRow, targetRow, 
                                               lastSuccessfulPK, 
                                               lastSuccessfulBinlogPos, 
                                               currentBinlogEntry, oldRecord, 
                                               newRecord, chosenPK >>

verify_before_emit(self) == /\ pc[self] = "verify_before_emit"
                            /\ ReverifyStore' = Append(ReverifyStore, (lastVerifiedPK[self] + 1))
                            /\ pc' = [pc EXCEPT ![self] = "verify_before_upkey"]
                            /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, 
                                            TargetTable, SourceBinlog, 
                                            ApplicationReadonly, 
                                            TargetBinlogPos, 
                                            BinlogStreamingStopRequested, 
                                            BinlogStreamerEmitReverifyEvent, 
                                            VerificationFailed, stack, 
                                            lastVerifiedPK, sourceRow_, 
                                            targetRow_, reverifyStoreIndex, 
                                            sourceRow, targetRow, 
                                            lastSuccessfulPK, 
                                            lastSuccessfulBinlogPos, 
                                            currentBinlogEntry, oldRecord, 
                                            newRecord, chosenPK >>

verify_before_upkey(self) == /\ pc[self] = "verify_before_upkey"
                             /\ lastVerifiedPK' = [lastVerifiedPK EXCEPT ![self] = lastVerifiedPK[self] + 1]
                             /\ pc' = [pc EXCEPT ![self] = "verify_before_loop"]
                             /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, 
                                             TargetTable, SourceBinlog, 
                                             ApplicationReadonly, 
                                             TargetBinlogPos, 
                                             BinlogStreamingStopRequested, 
                                             BinlogStreamerEmitReverifyEvent, 
                                             ReverifyStore, VerificationFailed, 
                                             stack, sourceRow_, targetRow_, 
                                             reverifyStoreIndex, sourceRow, 
                                             targetRow, lastSuccessfulPK, 
                                             lastSuccessfulBinlogPos, 
                                             currentBinlogEntry, oldRecord, 
                                             newRecord, chosenPK >>

verify_before_exit(self) == /\ pc[self] = "verify_before_exit"
                            /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                            /\ lastVerifiedPK' = [lastVerifiedPK EXCEPT ![self] = Head(stack[self]).lastVerifiedPK]
                            /\ sourceRow_' = [sourceRow_ EXCEPT ![self] = Head(stack[self]).sourceRow_]
                            /\ targetRow_' = [targetRow_ EXCEPT ![self] = Head(stack[self]).targetRow_]
                            /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                            /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, 
                                            TargetTable, SourceBinlog, 
                                            ApplicationReadonly, 
                                            TargetBinlogPos, 
                                            BinlogStreamingStopRequested, 
                                            BinlogStreamerEmitReverifyEvent, 
                                            ReverifyStore, VerificationFailed, 
                                            reverifyStoreIndex, sourceRow, 
                                            targetRow, lastSuccessfulPK, 
                                            lastSuccessfulBinlogPos, 
                                            currentBinlogEntry, oldRecord, 
                                            newRecord, chosenPK >>

VerifyBeforeCutover(self) == verify_before_init(self)
                                \/ verify_before_loop(self)
                                \/ verify_before_read_src(self)
                                \/ verify_before_read_tgt(self)
                                \/ verify_before_compare(self)
                                \/ verify_before_emit(self)
                                \/ verify_before_upkey(self)
                                \/ verify_before_exit(self)

verify_during_stop_emit(self) == /\ pc[self] = "verify_during_stop_emit"
                                 /\ BinlogStreamerEmitReverifyEvent' = FALSE
                                 /\ pc' = [pc EXCEPT ![self] = "verify_during_cutover"]
                                 /\ UNCHANGED << CurrentMaxPrimaryKey, 
                                                 SourceTable, TargetTable, 
                                                 SourceBinlog, 
                                                 ApplicationReadonly, 
                                                 TargetBinlogPos, 
                                                 BinlogStreamingStopRequested, 
                                                 ReverifyStore, 
                                                 VerificationFailed, stack, 
                                                 lastVerifiedPK, sourceRow_, 
                                                 targetRow_, 
                                                 reverifyStoreIndex, sourceRow, 
                                                 targetRow, lastSuccessfulPK, 
                                                 lastSuccessfulBinlogPos, 
                                                 currentBinlogEntry, oldRecord, 
                                                 newRecord, chosenPK >>

verify_during_cutover(self) == /\ pc[self] = "verify_during_cutover"
                               /\ IF reverifyStoreIndex[self] < Len(ReverifyStore)
                                     THEN /\ sourceRow' = [sourceRow EXCEPT ![self] = SourceTable[ReverifyStore[reverifyStoreIndex[self] + 1]]]
                                          /\ targetRow' = [targetRow EXCEPT ![self] = TargetTable[ReverifyStore[reverifyStoreIndex[self] + 1]]]
                                          /\ IF sourceRow'[self] # targetRow'[self]
                                                THEN /\ VerificationFailed' = TRUE
                                                ELSE /\ TRUE
                                                     /\ UNCHANGED VerificationFailed
                                          /\ reverifyStoreIndex' = [reverifyStoreIndex EXCEPT ![self] = reverifyStoreIndex[self] + 1]
                                          /\ pc' = [pc EXCEPT ![self] = "verify_during_cutover"]
                                     ELSE /\ pc' = [pc EXCEPT ![self] = "verify_during_cutover_exit"]
                                          /\ UNCHANGED << VerificationFailed, 
                                                          reverifyStoreIndex, 
                                                          sourceRow, targetRow >>
                               /\ UNCHANGED << CurrentMaxPrimaryKey, 
                                               SourceTable, TargetTable, 
                                               SourceBinlog, 
                                               ApplicationReadonly, 
                                               TargetBinlogPos, 
                                               BinlogStreamingStopRequested, 
                                               BinlogStreamerEmitReverifyEvent, 
                                               ReverifyStore, stack, 
                                               lastVerifiedPK, sourceRow_, 
                                               targetRow_, lastSuccessfulPK, 
                                               lastSuccessfulBinlogPos, 
                                               currentBinlogEntry, oldRecord, 
                                               newRecord, chosenPK >>

verify_during_cutover_exit(self) == /\ pc[self] = "verify_during_cutover_exit"
                                    /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                    /\ reverifyStoreIndex' = [reverifyStoreIndex EXCEPT ![self] = Head(stack[self]).reverifyStoreIndex]
                                    /\ sourceRow' = [sourceRow EXCEPT ![self] = Head(stack[self]).sourceRow]
                                    /\ targetRow' = [targetRow EXCEPT ![self] = Head(stack[self]).targetRow]
                                    /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                    /\ UNCHANGED << CurrentMaxPrimaryKey, 
                                                    SourceTable, TargetTable, 
                                                    SourceBinlog, 
                                                    ApplicationReadonly, 
                                                    TargetBinlogPos, 
                                                    BinlogStreamingStopRequested, 
                                                    BinlogStreamerEmitReverifyEvent, 
                                                    ReverifyStore, 
                                                    VerificationFailed, 
                                                    lastVerifiedPK, sourceRow_, 
                                                    targetRow_, 
                                                    lastSuccessfulPK, 
                                                    lastSuccessfulBinlogPos, 
                                                    currentBinlogEntry, 
                                                    oldRecord, newRecord, 
                                                    chosenPK >>

VerifyDuringCutover(self) == verify_during_stop_emit(self)
                                \/ verify_during_cutover(self)
                                \/ verify_during_cutover_exit(self)

tblit_loop == /\ pc[TableIterator] = "tblit_loop"
              /\ IF lastSuccessfulPK < MaxPrimaryKey
                    THEN /\ pc' = [pc EXCEPT ![TableIterator] = "tblit_rw"]
                    ELSE /\ pc' = [pc EXCEPT ![TableIterator] = "Done"]
              /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, TargetTable, 
                              SourceBinlog, ApplicationReadonly, 
                              TargetBinlogPos, BinlogStreamingStopRequested, 
                              BinlogStreamerEmitReverifyEvent, ReverifyStore, 
                              VerificationFailed, stack, lastVerifiedPK, 
                              sourceRow_, targetRow_, reverifyStoreIndex, 
                              sourceRow, targetRow, lastSuccessfulPK, 
                              lastSuccessfulBinlogPos, currentBinlogEntry, 
                              oldRecord, newRecord, chosenPK >>

tblit_rw == /\ pc[TableIterator] = "tblit_rw"
            /\ \E r \in Records:
                 TargetTable' = [TargetTable EXCEPT ![lastSuccessfulPK + 1] = r]
            /\ pc' = [pc EXCEPT ![TableIterator] = "tblit_upkey"]
            /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, SourceBinlog, 
                            ApplicationReadonly, TargetBinlogPos, 
                            BinlogStreamingStopRequested, 
                            BinlogStreamerEmitReverifyEvent, ReverifyStore, 
                            VerificationFailed, stack, lastVerifiedPK, 
                            sourceRow_, targetRow_, reverifyStoreIndex, 
                            sourceRow, targetRow, lastSuccessfulPK, 
                            lastSuccessfulBinlogPos, currentBinlogEntry, 
                            oldRecord, newRecord, chosenPK >>

tblit_upkey == /\ pc[TableIterator] = "tblit_upkey"
               /\ lastSuccessfulPK' = lastSuccessfulPK + 1
               /\ pc' = [pc EXCEPT ![TableIterator] = "tblit_loop"]
               /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, TargetTable, 
                               SourceBinlog, ApplicationReadonly, 
                               TargetBinlogPos, BinlogStreamingStopRequested, 
                               BinlogStreamerEmitReverifyEvent, ReverifyStore, 
                               VerificationFailed, stack, lastVerifiedPK, 
                               sourceRow_, targetRow_, reverifyStoreIndex, 
                               sourceRow, targetRow, lastSuccessfulBinlogPos, 
                               currentBinlogEntry, oldRecord, newRecord, 
                               chosenPK >>

ProcTableIterator == tblit_loop \/ tblit_rw \/ tblit_upkey

binlog_loop == /\ pc[BinlogStreamer] = "binlog_loop"
               /\ IF BinlogStreamingStopRequested = FALSE \/ (BinlogStreamingStopRequested = TRUE /\ lastSuccessfulBinlogPos < TargetBinlogPos)
                     THEN /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_read"]
                     ELSE /\ pc' = [pc EXCEPT ![BinlogStreamer] = "Done"]
               /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, TargetTable, 
                               SourceBinlog, ApplicationReadonly, 
                               TargetBinlogPos, BinlogStreamingStopRequested, 
                               BinlogStreamerEmitReverifyEvent, ReverifyStore, 
                               VerificationFailed, stack, lastVerifiedPK, 
                               sourceRow_, targetRow_, reverifyStoreIndex, 
                               sourceRow, targetRow, lastSuccessfulPK, 
                               lastSuccessfulBinlogPos, currentBinlogEntry, 
                               oldRecord, newRecord, chosenPK >>

binlog_read == /\ pc[BinlogStreamer] = "binlog_read"
               /\ IF lastSuccessfulBinlogPos < Len(SourceBinlog)
                     THEN /\ currentBinlogEntry' = SourceBinlog[lastSuccessfulBinlogPos + 1]
                          /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_write"]
                     ELSE /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_loop"]
                          /\ UNCHANGED currentBinlogEntry
               /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, TargetTable, 
                               SourceBinlog, ApplicationReadonly, 
                               TargetBinlogPos, BinlogStreamingStopRequested, 
                               BinlogStreamerEmitReverifyEvent, ReverifyStore, 
                               VerificationFailed, stack, lastVerifiedPK, 
                               sourceRow_, targetRow_, reverifyStoreIndex, 
                               sourceRow, targetRow, lastSuccessfulPK, 
                               lastSuccessfulBinlogPos, oldRecord, newRecord, 
                               chosenPK >>

binlog_write == /\ pc[BinlogStreamer] = "binlog_write"
                /\ IF TargetTable[currentBinlogEntry.pk] = currentBinlogEntry.oldr
                      THEN /\ TargetTable' = [TargetTable EXCEPT ![currentBinlogEntry.pk] = currentBinlogEntry.newr]
                           /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_reveri_ev"]
                      ELSE /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_upkey"]
                           /\ UNCHANGED TargetTable
                /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, 
                                SourceBinlog, ApplicationReadonly, 
                                TargetBinlogPos, BinlogStreamingStopRequested, 
                                BinlogStreamerEmitReverifyEvent, ReverifyStore, 
                                VerificationFailed, stack, lastVerifiedPK, 
                                sourceRow_, targetRow_, reverifyStoreIndex, 
                                sourceRow, targetRow, lastSuccessfulPK, 
                                lastSuccessfulBinlogPos, currentBinlogEntry, 
                                oldRecord, newRecord, chosenPK >>

binlog_reveri_ev == /\ pc[BinlogStreamer] = "binlog_reveri_ev"
                    /\ IF BinlogStreamerEmitReverifyEvent = TRUE
                          THEN /\ ReverifyStore' = Append(ReverifyStore, (currentBinlogEntry.pk))
                          ELSE /\ TRUE
                               /\ UNCHANGED ReverifyStore
                    /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_upkey"]
                    /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, 
                                    TargetTable, SourceBinlog, 
                                    ApplicationReadonly, TargetBinlogPos, 
                                    BinlogStreamingStopRequested, 
                                    BinlogStreamerEmitReverifyEvent, 
                                    VerificationFailed, stack, lastVerifiedPK, 
                                    sourceRow_, targetRow_, reverifyStoreIndex, 
                                    sourceRow, targetRow, lastSuccessfulPK, 
                                    lastSuccessfulBinlogPos, 
                                    currentBinlogEntry, oldRecord, newRecord, 
                                    chosenPK >>

binlog_upkey == /\ pc[BinlogStreamer] = "binlog_upkey"
                /\ lastSuccessfulBinlogPos' = lastSuccessfulBinlogPos + 1
                /\ pc' = [pc EXCEPT ![BinlogStreamer] = "binlog_loop"]
                /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, TargetTable, 
                                SourceBinlog, ApplicationReadonly, 
                                TargetBinlogPos, BinlogStreamingStopRequested, 
                                BinlogStreamerEmitReverifyEvent, ReverifyStore, 
                                VerificationFailed, stack, lastVerifiedPK, 
                                sourceRow_, targetRow_, reverifyStoreIndex, 
                                sourceRow, targetRow, lastSuccessfulPK, 
                                currentBinlogEntry, oldRecord, newRecord, 
                                chosenPK >>

ProcBinlogStreamer == binlog_loop \/ binlog_read \/ binlog_write
                         \/ binlog_reveri_ev \/ binlog_upkey

app_loop == /\ pc[Application] = "app_loop"
            /\ IF ApplicationReadonly = FALSE
                  THEN /\ pc' = [pc EXCEPT ![Application] = "app_write"]
                  ELSE /\ pc' = [pc EXCEPT ![Application] = "Done"]
            /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, TargetTable, 
                            SourceBinlog, ApplicationReadonly, TargetBinlogPos, 
                            BinlogStreamingStopRequested, 
                            BinlogStreamerEmitReverifyEvent, ReverifyStore, 
                            VerificationFailed, stack, lastVerifiedPK, 
                            sourceRow_, targetRow_, reverifyStoreIndex, 
                            sourceRow, targetRow, lastSuccessfulPK, 
                            lastSuccessfulBinlogPos, currentBinlogEntry, 
                            oldRecord, newRecord, chosenPK >>

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
                                  "Failure of assertion at line 140, column 19.")
                        /\ CurrentMaxPrimaryKey' = chosenPK'
                   ELSE /\ TRUE
                        /\ UNCHANGED CurrentMaxPrimaryKey
             /\ pc' = [pc EXCEPT ![Application] = "app_loop"]
             /\ UNCHANGED << TargetTable, ApplicationReadonly, TargetBinlogPos, 
                             BinlogStreamingStopRequested, 
                             BinlogStreamerEmitReverifyEvent, ReverifyStore, 
                             VerificationFailed, stack, lastVerifiedPK, 
                             sourceRow_, targetRow_, reverifyStoreIndex, 
                             sourceRow, targetRow, lastSuccessfulPK, 
                             lastSuccessfulBinlogPos, currentBinlogEntry >>

ProcApplication == app_loop \/ app_write

ferry_precutover == /\ pc[Ferry] = "ferry_precutover"
                    /\ pc[TableIterator] = "Done"
                    /\ stack' = [stack EXCEPT ![Ferry] = << [ procedure |->  "VerifyBeforeCutover",
                                                              pc        |->  "ferry_setro",
                                                              lastVerifiedPK |->  lastVerifiedPK[Ferry],
                                                              sourceRow_ |->  sourceRow_[Ferry],
                                                              targetRow_ |->  targetRow_[Ferry] ] >>
                                                          \o stack[Ferry]]
                    /\ lastVerifiedPK' = [lastVerifiedPK EXCEPT ![Ferry] = 0]
                    /\ sourceRow_' = [sourceRow_ EXCEPT ![Ferry] = defaultInitValue]
                    /\ targetRow_' = [targetRow_ EXCEPT ![Ferry] = defaultInitValue]
                    /\ pc' = [pc EXCEPT ![Ferry] = "verify_before_init"]
                    /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, 
                                    TargetTable, SourceBinlog, 
                                    ApplicationReadonly, TargetBinlogPos, 
                                    BinlogStreamingStopRequested, 
                                    BinlogStreamerEmitReverifyEvent, 
                                    ReverifyStore, VerificationFailed, 
                                    reverifyStoreIndex, sourceRow, targetRow, 
                                    lastSuccessfulPK, lastSuccessfulBinlogPos, 
                                    currentBinlogEntry, oldRecord, newRecord, 
                                    chosenPK >>

ferry_setro == /\ pc[Ferry] = "ferry_setro"
               /\ ApplicationReadonly' = TRUE
               /\ pc' = [pc EXCEPT ![Ferry] = "ferry_waitro"]
               /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, TargetTable, 
                               SourceBinlog, TargetBinlogPos, 
                               BinlogStreamingStopRequested, 
                               BinlogStreamerEmitReverifyEvent, ReverifyStore, 
                               VerificationFailed, stack, lastVerifiedPK, 
                               sourceRow_, targetRow_, reverifyStoreIndex, 
                               sourceRow, targetRow, lastSuccessfulPK, 
                               lastSuccessfulBinlogPos, currentBinlogEntry, 
                               oldRecord, newRecord, chosenPK >>

ferry_waitro == /\ pc[Ferry] = "ferry_waitro"
                /\ pc[Application] = "Done"
                /\ pc' = [pc EXCEPT ![Ferry] = "ferry_binlogpos"]
                /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, TargetTable, 
                                SourceBinlog, ApplicationReadonly, 
                                TargetBinlogPos, BinlogStreamingStopRequested, 
                                BinlogStreamerEmitReverifyEvent, ReverifyStore, 
                                VerificationFailed, stack, lastVerifiedPK, 
                                sourceRow_, targetRow_, reverifyStoreIndex, 
                                sourceRow, targetRow, lastSuccessfulPK, 
                                lastSuccessfulBinlogPos, currentBinlogEntry, 
                                oldRecord, newRecord, chosenPK >>

ferry_binlogpos == /\ pc[Ferry] = "ferry_binlogpos"
                   /\ TargetBinlogPos' = Len(SourceBinlog)
                   /\ pc' = [pc EXCEPT ![Ferry] = "ferry_binlogstop"]
                   /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, 
                                   TargetTable, SourceBinlog, 
                                   ApplicationReadonly, 
                                   BinlogStreamingStopRequested, 
                                   BinlogStreamerEmitReverifyEvent, 
                                   ReverifyStore, VerificationFailed, stack, 
                                   lastVerifiedPK, sourceRow_, targetRow_, 
                                   reverifyStoreIndex, sourceRow, targetRow, 
                                   lastSuccessfulPK, lastSuccessfulBinlogPos, 
                                   currentBinlogEntry, oldRecord, newRecord, 
                                   chosenPK >>

ferry_binlogstop == /\ pc[Ferry] = "ferry_binlogstop"
                    /\ BinlogStreamingStopRequested' = TRUE
                    /\ pc' = [pc EXCEPT ![Ferry] = "ferry_verify"]
                    /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, 
                                    TargetTable, SourceBinlog, 
                                    ApplicationReadonly, TargetBinlogPos, 
                                    BinlogStreamerEmitReverifyEvent, 
                                    ReverifyStore, VerificationFailed, stack, 
                                    lastVerifiedPK, sourceRow_, targetRow_, 
                                    reverifyStoreIndex, sourceRow, targetRow, 
                                    lastSuccessfulPK, lastSuccessfulBinlogPos, 
                                    currentBinlogEntry, oldRecord, newRecord, 
                                    chosenPK >>

ferry_verify == /\ pc[Ferry] = "ferry_verify"
                /\ pc[BinlogStreamer] = "Done"
                /\ stack' = [stack EXCEPT ![Ferry] = << [ procedure |->  "VerifyDuringCutover",
                                                          pc        |->  "Done",
                                                          reverifyStoreIndex |->  reverifyStoreIndex[Ferry],
                                                          sourceRow |->  sourceRow[Ferry],
                                                          targetRow |->  targetRow[Ferry] ] >>
                                                      \o stack[Ferry]]
                /\ reverifyStoreIndex' = [reverifyStoreIndex EXCEPT ![Ferry] = 0]
                /\ sourceRow' = [sourceRow EXCEPT ![Ferry] = defaultInitValue]
                /\ targetRow' = [targetRow EXCEPT ![Ferry] = defaultInitValue]
                /\ pc' = [pc EXCEPT ![Ferry] = "verify_during_stop_emit"]
                /\ UNCHANGED << CurrentMaxPrimaryKey, SourceTable, TargetTable, 
                                SourceBinlog, ApplicationReadonly, 
                                TargetBinlogPos, BinlogStreamingStopRequested, 
                                BinlogStreamerEmitReverifyEvent, ReverifyStore, 
                                VerificationFailed, lastVerifiedPK, sourceRow_, 
                                targetRow_, lastSuccessfulPK, 
                                lastSuccessfulBinlogPos, currentBinlogEntry, 
                                oldRecord, newRecord, chosenPK >>

ProcFerry == ferry_precutover \/ ferry_setro \/ ferry_waitro
                \/ ferry_binlogpos \/ ferry_binlogstop \/ ferry_verify

Next == ProcTableIterator \/ ProcBinlogStreamer \/ ProcApplication
           \/ ProcFerry
           \/ (\E self \in ProcSet:  \/ VerifyBeforeCutover(self)
                                     \/ VerifyDuringCutover(self))
           \/ (* Disjunct to prevent deadlock on termination *)
              ((\A self \in ProcSet: pc[self] = "Done") /\ UNCHANGED vars)

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(ProcTableIterator)
        /\ WF_vars(ProcBinlogStreamer)
        /\ WF_vars(ProcApplication)
        /\ /\ WF_vars(ProcFerry)
           /\ WF_vars(VerifyBeforeCutover(Ferry))
           /\ WF_vars(VerifyDuringCutover(Ferry))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

VerificationPassIfEqual == (\A self \in ProcSet: pc[self] = "Done" /\ (SourceTable = TargetTable)) => (VerificationFailed = FALSE) /\ PrintT(<<"Equal", SourceTable, TargetTable, VerificationFailed>>)
VerificationFailIfDifferent == (\A self \in ProcSet: pc[self] = "Done" /\ (SourceTable # TargetTable)) => (VerificationFailed = TRUE) /\ PrintT(<<"Different", SourceTable, TargetTable, VerificationFailed>>)

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
\* Last modified Wed Feb 07 15:54:06 EST 2018 by shuhao
\* Created Wed Feb 07 13:50:39 EST 2018 by shuhao
