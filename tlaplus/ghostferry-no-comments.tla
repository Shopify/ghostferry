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
    BinlogStreamingStopRequested = FALSE;

  fair process (ProcTableIterator = TableIterator)
  variables
    lastSuccessfulPK = 0,
    currentRow;
  {
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
    lastSuccessfulBinlogPos = 0,
    currentBinlogEntry;
  {
    binlog_loop: while (BinlogStreamingStopRequested = FALSE \/ (BinlogStreamingStopRequested = TRUE /\ lastSuccessfulBinlogPos < TargetBinlogPos)) {
    binlog_read:   if (lastSuccessfulBinlogPos < Len(SourceBinlog)) {
                     currentBinlogEntry := SourceBinlog[lastSuccessfulBinlogPos + 1];
    binlog_write:    if (TargetTable[currentBinlogEntry.pk] = currentBinlogEntry.oldr) {
                       TargetTable[currentBinlogEntry.pk] := currentBinlogEntry.newr;
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
              }
  }

  fair process (ProcFerry = Ferry) {
    ferry_setro:      await pc[TableIterator] = "Done";
                      ApplicationReadonly := TRUE;
    ferry_waitro:     await pc[Application] = "Done";
    ferry_binlogpos:  TargetBinlogPos := Len(SourceBinlog);
    ferry_binlogstop: BinlogStreamingStopRequested := TRUE;
  }
}
 ***************************************************************************)

=============================================================================
\* Modification History
\* Last modified Wed Feb 07 14:02:48 EST 2018 by shuhao
\* Created Wed Feb 07 13:50:39 EST 2018 by shuhao
