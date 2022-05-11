package copydb

import (
	"sync"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type CopydbFerry struct {
	Ferry  *ghostferry.Ferry
	config *Config
}

func NewFerry(config *Config) *CopydbFerry {
	ferry := &ghostferry.Ferry{
		Config: config.Config,
	}

	return &CopydbFerry{
		Ferry:  ferry,
		config: config,
	}
}

func (this *CopydbFerry) Initialize() error {
	if this.config.RunFerryFromReplica {
		err := this.initializeWaitUntilReplicaIsCaughtUpToMasterConnection()
		if err != nil {
			return err
		}
	}

	return this.Ferry.Initialize()
}

func (this *CopydbFerry) Start() error {
	return this.Ferry.Start()
}

func (this *CopydbFerry) Run() {
	copyWG := &sync.WaitGroup{}
	copyWG.Add(1)
	go func() {
		defer copyWG.Done()
		this.Ferry.Run()
	}()

	// If AutomaticCutover == false, it will pause below the following line
	this.Ferry.WaitUntilRowCopyIsComplete()

	// This waits until we're pretty close in the binlog before making the
	// source readonly. This is to avoid excessive downtime caused by the
	// binlog streamer catching up.
	this.Ferry.WaitUntilBinlogStreamerCatchesUp()

	// Optionally (configurable) POST to an HTTP endpoint telling that service that Ghostferry is ready for cutover.
	// The external service can then perform steps needed immediately prior to cutover. For example, on receiving the callback, the service can set the database to be readonly.
	cutoverStart := this.Ferry.StartCutover()

	// This is when the source database should be set as read only, whether it
	// is done in application level or the database level.
	// Must ensure that all transactions are flushed to the binlog before
	// proceeding.
	this.Ferry.FlushBinlogAndStopStreaming()

	// After waiting for the binlog streamer to stop, the source and the target
	// should be identical.
	copyWG.Wait()

	this.Ferry.StopTargetVerifier()

	// Optionally (configurable) POST to an HTTP endpoint telling that service Ghostferry has completed cutover and has stopped streaming the binlog.
	// The external service can then perform steps needed after cutover. For example, on receiving the callback, the service can set the target database to allow writes.
	this.Ferry.EndCutover(cutoverStart)

	// This is where you cutover from using the source database to
	// using the target database

	logrus.Info("ghostferry main operations has terminated but the control server remains online")
	logrus.Info("press CTRL+C or send an interrupt to stop the control server and end this process")

	this.Ferry.ControlServer.Wait()
}

func (this *CopydbFerry) initializeWaitUntilReplicaIsCaughtUpToMasterConnection() error {
	masterDB, err := this.config.SourceReplicationMaster.SqlDB(logrus.WithField("tag", "copydb"))
	if err != nil {
		return err
	}

	positionFetcher := ghostferry.ReplicatedMasterPositionViaCustomQuery{Query: this.config.ReplicatedMasterPositionQuery}

	var timeout time.Duration
	if this.config.WaitForReplicationTimeout == "" {
		timeout = time.Duration(0)
	} else {
		timeout, err = time.ParseDuration(this.config.WaitForReplicationTimeout)
		if err != nil {
			return err
		}
	}

	this.Ferry.WaitUntilReplicaIsCaughtUpToMaster = &ghostferry.WaitUntilReplicaIsCaughtUpToMaster{
		MasterDB:                        masterDB,
		Timeout:                         timeout,
		ReplicatedMasterPositionFetcher: positionFetcher,
	}
	return nil
}
