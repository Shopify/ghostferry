package ghostferry

import (
	"fmt"
	"html/template"
	"net/http"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

type ControlServerTableStatus struct {
	TableName                   string
	PaginationKeyName           string
	Status                      string
	LastSuccessfulPaginationKey uint64
	TargetPaginationKey         uint64
}

type ControlServerStatus struct {
	Progress
	GhostferryVersion string

	SourceHostPort string
	TargetHostPort string

	OverallState      string
	StartTime         time.Time
	CurrentTime       time.Time
	TimeTaken         time.Duration
	ETA               time.Duration
	BinlogStreamerLag time.Duration

	AutomaticCutover            bool
	BinlogStreamerStopRequested bool

	CompletedTableCount int
	TotalTableCount     int
	TableStatuses       []*ControlServerTableStatus
	AllTableNames       []string
	AllDatabaseNames    []string

	VerifierSupport     bool
	VerifierAvailable   bool
	VerificationStarted bool
	VerificationDone    bool
	VerificationResult  VerificationResult
	VerificationErr     error
}

type ControlServer struct {
	F        *Ferry
	Verifier Verifier
	Addr     string
	Basedir  string

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
	this.router.HandleFunc("/api/actions/pause", this.HandlePause).Methods("POST")
	this.router.HandleFunc("/api/actions/unpause", this.HandleUnpause).Methods("POST")
	this.router.HandleFunc("/api/actions/cutover", this.HandleCutover).Queries("type", "{type:automatic|manual}").Methods("POST")
	this.router.HandleFunc("/api/actions/stop", this.HandleStop).Methods("POST")
	this.router.HandleFunc("/api/actions/verify", this.HandleVerify).Methods("POST")

	if WebUiBasedir != "" {
		this.Basedir = WebUiBasedir
	}

	staticFiles := http.StripPrefix("/static/", http.FileServer(http.Dir(filepath.Join(this.Basedir, "webui", "static"))))
	this.router.PathPrefix("/static/").Handler(staticFiles)

	this.templates, err = template.New("").ParseFiles(filepath.Join(this.Basedir, "webui", "index.html"))

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
	err := this.templates.ExecuteTemplate(w, "index.html", this.fetchStatus())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (this *ControlServer) HandlePause(w http.ResponseWriter, r *http.Request) {
	this.F.Throttler.SetPaused(true)

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (this *ControlServer) HandleUnpause(w http.ResponseWriter, r *http.Request) {
	this.F.Throttler.SetPaused(false)

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
	if this.Verifier == nil {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	err := this.Verifier.StartInBackground()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (this *ControlServer) fetchStatus() *ControlServerStatus {
	status := &ControlServerStatus{}
	status.Progress = *(this.F.Progress())

	status.GhostferryVersion = VersionString

	status.SourceHostPort = fmt.Sprintf("%s:%d", this.F.Source.Host, this.F.Source.Port)
	status.TargetHostPort = fmt.Sprintf("%s:%d", this.F.Target.Host, this.F.Target.Port)

	status.OverallState = fmt.Sprintf("%s", this.F.OverallState.Load())

	status.StartTime = this.F.StartTime
	status.CurrentTime = time.Now()
	status.TimeTaken = time.Duration(status.TimeTaken)

	status.BinlogStreamerLag = time.Duration(status.BinlogStreamerLag)

	status.AutomaticCutover = this.F.Config.AutomaticCutover
	status.BinlogStreamerStopRequested = this.F.BinlogStreamer.stopRequested

	// Getting all table statuses
	status.TableStatuses = make([]*ControlServerTableStatus, 0, len(status.Tables))
	status.CompletedTableCount = 0
	status.TotalTableCount = len(status.Tables)
	status.AllTableNames = make([]string, 0, len(status.Tables))

	dbSet := make(map[string]bool)
	for name, tableProgress := range status.Tables {
		status.TableStatuses = append(status.TableStatuses, &ControlServerTableStatus{
			TableName:                   name,
			PaginationKeyName:           this.F.Tables[name].GetPaginationColumn().Name,
			Status:                      tableProgress.CurrentAction,
			LastSuccessfulPaginationKey: tableProgress.LastSuccessfulPaginationKey,
			TargetPaginationKey:         tableProgress.LastSuccessfulPaginationKey,
		})

		status.AllTableNames = append(status.AllTableNames, name)
		dbSet[this.F.Tables[name].Schema] = true

		if tableProgress.CurrentAction == TableActionCompleted {
			status.CompletedTableCount++
		}
	}

	status.AllDatabaseNames = make([]string, 0, len(dbSet))
	for dbName := range dbSet {
		status.AllDatabaseNames = append(status.AllDatabaseNames, dbName)
	}

	sort.Strings(status.AllDatabaseNames)
	sort.Strings(status.AllTableNames)

	// ETA estimation
	// We do it here rather than in DataIteratorState to give the lock back
	// ASAP. It's not supposed to be that accurate anyway.
	status.ETA = time.Duration(status.ETA)

	// Verifier display
	if this.Verifier != nil {
		status.VerifierSupport = true

		result, err := this.Verifier.Result()
		status.VerificationStarted = result.IsStarted()
		status.VerificationDone = result.IsDone()

		// We can only run the verifier if we're not copying and not verifying
		status.VerifierAvailable = status.OverallState != StateStarting && status.OverallState != StateCopying && (!status.VerificationStarted || status.VerificationDone)
		status.VerificationResult = result.VerificationResult
		status.VerificationErr = err
	} else {
		status.VerifierSupport = false
		status.VerifierAvailable = false
	}

	return status
}
