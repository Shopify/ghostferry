package ghostferry

import (
	"fmt"
	"html/template"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

func setKeys(m map[string]bool) []string {
	ks := make([]string, len(m))
	i := 0
	for k, _ := range m {
		ks[i] = k
		i++
	}

	return ks
}

type TableStatus struct {
	TableName        string
	PrimaryKeyName   string
	Status           string
	LastSuccessfulPK int64
	TargetPK         int64
}

type StatusPage struct {
	SourceHostPort      string
	TargetHostPort      string
	ApplicableDatabases []string
	ApplicableTables    []string

	OverallState     string
	StartTime        time.Time
	CurrentTime      time.Time
	TimeTaken        time.Duration
	ETA              time.Duration
	AutomaticCutover bool

	BinlogStreamerStopRequested bool
	LastSuccessfulBinlogPos     mysql.Position
	TargetBinlogPos             mysql.Position

	Throttled      bool
	ThrottledUntil time.Time

	CompletedTableCount int
	TotalTableCount     int
	TableStatuses       []*TableStatus

	VerifierAvailable   bool
	VerificationStarted bool
	VerificationDone    bool
	VerifiedCorrect     bool
	VerificationErr     error
}

type ControlServer struct {
	F       *Ferry
	Addr    string
	Basedir string

	server    *http.Server
	logger    *logrus.Entry
	router    *mux.Router
	templates *template.Template
}

func (this *ControlServer) Initialize() (err error) {
	this.logger = logrus.WithField("tag", "control_server")
	this.logger.Info("initializing")

	this.router = mux.NewRouter()
	this.router.HandleFunc("/", this.HandleIndex).Methods("GET")

	staticFiles := http.StripPrefix("/static/", http.FileServer(http.Dir(filepath.Join(this.Basedir, "webui", "static"))))
	this.router.PathPrefix("/static/").Handler(staticFiles)

	this.router.HandleFunc("/api/actions/pause", this.HandlePause).Methods("POST")
	this.router.HandleFunc("/api/actions/unpause", this.HandleUnpause).Methods("POST")
	this.router.HandleFunc("/api/actions/throttle", this.HandleThrottle).Methods("POST")
	this.router.HandleFunc("/api/actions/cutover", this.HandleCutover).Queries("type", "{type:automatic|manual}").Methods("POST")
	this.router.HandleFunc("/api/actions/stop", this.HandleStop).Methods("POST")
	this.router.HandleFunc("/api/actions/verify", this.HandleVerify).Methods("POST")

	this.templates, err = template.ParseFiles(filepath.Join(this.Basedir, "webui", "index.html"))
	if err != nil {
		return err
	}

	this.server = &http.Server{
		Addr:    this.Addr,
		Handler: this,
	}

	return nil
}

func (this *ControlServer) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	this.logger.Infof("running on %s", this.Addr)
	err := this.server.ListenAndServe()
	if err != nil {
		logrus.WithError(err).Error("error on ListenAndServe")
	}
}

func (this *ControlServer) Shutdown() error {
	return this.server.Shutdown(nil)
}

func (this *ControlServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	this.router.ServeHTTP(w, r)

	this.logger.WithFields(logrus.Fields{
		"method": r.Method,
		"path":   r.RequestURI,
		"time":   time.Now().Sub(start),
	}).Info("served http request")
}

func (this *ControlServer) HandleIndex(w http.ResponseWriter, r *http.Request) {

	page := &StatusPage{
		SourceHostPort:      fmt.Sprintf("%s:%d", this.F.SourceHost, this.F.SourcePort),
		TargetHostPort:      fmt.Sprintf("%s:%d", this.F.TargetHost, this.F.TargetPort),
		ApplicableDatabases: setKeys(this.F.ApplicableDatabases),
		ApplicableTables:    setKeys(this.F.ApplicableTables),

		// OverallStatus
		OverallState:     this.F.OverallState,
		StartTime:        this.F.StartTime,
		CurrentTime:      time.Now(),
		AutomaticCutover: this.F.Config.AutomaticCutover,
		// ETA

		BinlogStreamerStopRequested: this.F.BinlogStreamer.stopRequested,
		LastSuccessfulBinlogPos:     this.F.BinlogStreamer.lastStreamedBinlogPosition,
		TargetBinlogPos:             this.F.BinlogStreamer.targetBinlogPosition,
		// BinlogStreamerLag

		ThrottledUntil: this.F.Throttler.ThrottleUntil(),

		TableStatuses: make([]*TableStatus, 0),

		VerifierAvailable: this.F.Verifier != nil,
	}

	if page.VerifierAvailable {
		page.VerificationStarted = this.F.Verifier.VerificationStarted()
		page.VerificationDone = this.F.Verifier.VerificationDone()
		page.VerifiedCorrect, page.VerificationErr = this.F.Verifier.VerifiedCorrect()
	}

	if this.F.DoneTime.IsZero() {
		page.TimeTaken = page.CurrentTime.Sub(page.StartTime)
	} else {
		page.TimeTaken = this.F.DoneTime.Sub(page.StartTime)
	}

	page.Throttled = page.ThrottledUntil.After(page.CurrentTime)

	// Getting all the table statuses
	// In the order of completed, running, waiting
	completedTables := this.F.DataIterator.CurrentState.CompletedTables()
	targetPks := this.F.DataIterator.CurrentState.TargetPrimaryKeys()
	lastSuccessfulPks := this.F.DataIterator.CurrentState.LastSuccessfulPrimaryKeys()

	page.CompletedTableCount = len(completedTables)
	page.TotalTableCount = len(this.F.Tables)

	for tableName, _ := range completedTables {
		page.TableStatuses = append(page.TableStatuses, &TableStatus{
			TableName:        tableName,
			PrimaryKeyName:   this.F.Tables[tableName].GetPKColumn(0).Name,
			Status:           "Complete",
			TargetPK:         targetPks[tableName],
			LastSuccessfulPK: lastSuccessfulPks[tableName],
		})
	}

	for tableName, _ := range lastSuccessfulPks {
		if _, ok := completedTables[tableName]; ok {
			continue
		}

		page.TableStatuses = append(page.TableStatuses, &TableStatus{
			TableName:        tableName,
			PrimaryKeyName:   this.F.Tables[tableName].GetPKColumn(0).Name,
			Status:           "Copying",
			TargetPK:         targetPks[tableName],
			LastSuccessfulPK: lastSuccessfulPks[tableName],
		})
	}

	for tableName, _ := range this.F.Tables {
		if _, ok := lastSuccessfulPks[tableName]; ok {
			continue
		}

		page.TableStatuses = append(page.TableStatuses, &TableStatus{
			TableName:        tableName,
			PrimaryKeyName:   this.F.Tables[tableName].GetPKColumn(0).Name,
			Status:           "Waiting",
			TargetPK:         -1,
			LastSuccessfulPK: -1,
		})
	}

	err := this.templates.ExecuteTemplate(w, "index.html", page)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (this *ControlServer) HandlePause(w http.ResponseWriter, r *http.Request) {
	this.F.Throttler.Pause()

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (this *ControlServer) HandleUnpause(w http.ResponseWriter, r *http.Request) {
	this.F.Throttler.Unpause()

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (this *ControlServer) HandleThrottle(w http.ResponseWriter, r *http.Request) {
	this.F.Throttler.AddThrottleDuration(5 * time.Minute)

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (this *ControlServer) HandleCutover(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	if vars["type"] == "automatic" {
		this.F.AutomaticCutover = true
	} else if vars["type"] == "manual" {
		this.F.AutomaticCutover = false
	} else {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (this *ControlServer) HandleStop(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

func (this *ControlServer) HandleVerify(w http.ResponseWriter, r *http.Request) {
	if this.F.Verifier == nil {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	if !this.F.Verifier.VerificationStarted() {
		this.F.Verifier.StartVerification(this.F)
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}
