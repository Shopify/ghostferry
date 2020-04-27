package ghostferry

import (
	"fmt"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/sirupsen/logrus"
)

type TargetVerifier struct {
	logger           *logrus.Entry
	DB               *sql.DB
	StateTracker     *StateTracker
	CutoverCompleted AtomicBoolean
}

func NewTargetVerifier(targetDB *sql.DB, stateTracker *StateTracker, binlogStreamer *BinlogStreamer) (*TargetVerifier, error) {
	return &TargetVerifier{
		logger:       logrus.WithField("tag", "target_verifier"),
		DB:           targetDB,
		StateTracker: stateTracker,
	}, nil
}

// Verify that all DMLs against the target are coming from Ghostferry for the
// duration of the move. Once cutover has completed, we no longer need to perform
// this verification as all writes from the application are directed to the target
func (t *TargetVerifier) BinlogEventListener(evs []DMLEvent) error {
	// Cutover has completed, we do not need to verify target writes
	if t.CutoverCompleted.Get() {
		return nil
	}

	for _, ev := range evs {
		annotations, err := ev.Annotations()
		if err != nil {
			return err
		}

		// Ghostferry's annotation will alwaays be the first, if available
		if len(annotations) == 0 || annotations[0] != t.DB.Marginalia {
			paginationKey, err := ev.PaginationKey()
			if err != nil {
				return err
			}
			return fmt.Errorf("row data with paginationKey %d on `%s`.`%s` has been corrupted", paginationKey, ev.Database(), ev.Table())
		}
	}

	if t.StateTracker != nil {
		t.StateTracker.UpdateLastResumableTargetBinlogPositionForInlineVerifier(evs[len(evs)-1].ResumableBinlogPosition())
	}

	return nil
}
