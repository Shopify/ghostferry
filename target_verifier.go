package ghostferry

import (
	"fmt"
	"strconv"
	"strings"

	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/sirupsen/logrus"
)

type TargetVerifier struct {
	logger         *logrus.Entry
	DB             *sql.DB
	BinlogStreamer *BinlogStreamer
	StateTracker   *StateTracker
}

func NewTargetVerifier(targetDB *sql.DB, stateTracker *StateTracker, binlogStreamer *BinlogStreamer) (*TargetVerifier, error) {
	targetVerifier := &TargetVerifier{
		logger:         logrus.WithField("tag", "target_verifier"),
		DB:             targetDB,
		BinlogStreamer: binlogStreamer,
		StateTracker:   stateTracker,
	}
	targetVerifier.BinlogStreamer.AddEventListener(targetVerifier.BinlogEventListener)

	return targetVerifier, nil
}

// Verify that all DMLs against the target are coming from Ghostferry for the
// duration of the move. Once cutover has completed, we no longer need to perform
// this verification as all writes from the application are directed to the target
func (t *TargetVerifier) BinlogEventListener(evs []DMLEvent) error {
	for _, ev := range evs {
		annotation, err := ev.Annotation()
		if err != nil {
			return err
		}

		// Ghostferry's annotation will alwaays be the first, if available
		if annotation == "" || annotation != t.DB.Marginalia {
			paginationKey, err := ev.PaginationKey()
			if err != nil {
				return err
			}
			return fmt.Errorf("row data with paginationKey %d on `%s`.`%s` has been corrupted by a change directly performed in the target at binlog file: %s and position: %d", paginationKey, ev.Database(), ev.Table(), ev.BinlogPosition().Name, ev.BinlogPosition().Pos)
		}
	}

	if t.StateTracker != nil {
		t.StateTracker.UpdateLastResumableBinlogPositionForTargetVerifier(evs[len(evs)-1].ResumableBinlogPosition())

		lastTargetPos := t.StateTracker.lastStoredBinlogPositionForTargetVerifier

		binlogFile := strings.Split(lastTargetPos.Name, ",")[1]

		// last_filename=binlog.259484 last_position=105060633
		// If we do 259484.105060633
		// this wil truncate the decimal place, which is why I reversed
		// it so we use <position>.<
		pos, err := strconv.ParseFloat(fmt.Sprintf("%s.%s", lastTargetPos.Pos, binlogFile), 64)

		if err != nil {
			// Can't parse
		} else {
			metrics.Gauge(
				"LastStoredBinlogPositionForTargetVerifier",
				pos,
				[]MetricTag{MetricTag{"origin", "Writer"}},
				1.0)
		}
	}

	return nil
}
