package ghostferry

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"os/exec"
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

type CustomScriptStatus struct {
	Name     string
	Status   string
	Logs     string
	ExitCode int
	Running  bool
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

	// TODO: this is populated by the control server. Clearly this all needs a refactor.
	CustomScriptStatuses map[string]CustomScriptStatus
}

type ControlServer struct {
	F             *Ferry
	Verifier      Verifier
	Addr          string
	Basedir       string
	CustomScripts map[string][]string

	server    *http.Server
	logger    *logrus.Entry
	router    *mux.Router
	templates *template.Template

	customScriptsLock     sync.RWMutex
	customScriptsRunning  map[string]bool
	customScriptsLogs     map[string]string
	customScriptsStatus   map[string]string
	customScriptsExitCode map[string]int
}

func (this *ControlServer) Initialize() (err error) {
	this.logger = logrus.WithField("tag", "control_server")
	this.logger.Info("initializing")

	this.customScriptsRunning = make(map[string]bool)
	this.customScriptsLogs = make(map[string]string)
	this.customScriptsStatus = make(map[string]string)
	this.customScriptsExitCode = make(map[string]int)

	this.router = mux.NewRouter()
	this.router.HandleFunc("/", this.HandleIndex).Methods("GET")
	this.router.HandleFunc("/api/status", this.HandleStatus).Headers("Content-Type", "application/json").Methods("GET")
	this.router.HandleFunc("/api/pause", this.HandlePause).Methods("POST")
	this.router.HandleFunc("/api/unpause", this.HandleUnpause).Methods("POST")
	this.router.HandleFunc("/api/cutover", this.HandleCutover).Methods("POST")
	this.router.HandleFunc("/api/stop", this.HandleStop).Methods("POST")
	this.router.HandleFunc("/api/verify", this.HandleVerify).Methods("POST")
	this.router.HandleFunc("/api/script", this.HandleScript).Methods("POST")
	this.router.HandleFunc("/api/config", this.HandleConfig).Methods("POST")

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

func (this *ControlServer) Run() {
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
	status := this.fetchStatus()

	err := this.templates.ExecuteTemplate(w, "index.html", status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (this *ControlServer) HandlePause(w http.ResponseWriter, r *http.Request) {
	this.F.Throttler.SetPaused(true)

	returnSuccess(w, r)
}

func (this *ControlServer) HandleUnpause(w http.ResponseWriter, r *http.Request) {
	this.F.Throttler.SetPaused(false)

	returnSuccess(w, r)
}

func (this *ControlServer) HandleCutover(w http.ResponseWriter, r *http.Request) {
	cutover, err := cutoverType(r)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	if cutover == "automatic" {
		this.F.AutomaticCutover = true
	} else if cutover == "manual" {
		this.F.AutomaticCutover = false
	} else {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	returnSuccess(w, r)
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

	returnSuccess(w, r)
}

func (this *ControlServer) HandleStatus(w http.ResponseWriter, r *http.Request) {
	status := this.fetchStatus()
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(status)
	if err != nil {
		this.logger.WithError(err).Error("failed to send progress")
	}
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

	status.TimeTaken = time.Duration(status.Progress.TimeTaken * float64(time.Second))
	status.BinlogStreamerLag = time.Duration(status.Progress.BinlogStreamerLag * float64(time.Second))
	status.ETA = time.Duration(status.Progress.ETA * float64(time.Second))

	status.AutomaticCutover = this.F.Config.AutomaticCutover
	status.BinlogStreamerStopRequested = this.F.BinlogStreamer.stopRequested

	// Getting all table statuses
	status.TableStatuses = make([]*ControlServerTableStatus, 0, len(status.Tables))
	status.CompletedTableCount = 0
	status.TotalTableCount = len(status.Tables)
	status.AllTableNames = make([]string, 0, len(status.Tables))

	dbSet := make(map[string]bool)
	for name, tableProgress := range status.Tables {
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

	// Group the Tables by their status and then their TableName
	tablesGroupByStatus := make(map[string][]*ControlServerTableStatus)

	completedTables := make([]*ControlServerTableStatus, 0, len(status.Tables))
	copyingTables := make([]*ControlServerTableStatus, 0, len(status.Tables))
	waitingTables := make([]*ControlServerTableStatus, 0, len(status.Tables))

	tablesGroupByStatus[TableActionCompleted] = completedTables
	tablesGroupByStatus[TableActionCopying] = copyingTables
	tablesGroupByStatus[TableActionWaiting] = waitingTables

	for _, name := range status.AllTableNames {
		tableProgress := status.Tables[name]
		tableStatus := tableProgress.CurrentAction

		lastSuccessfulPaginationKey := tableProgress.LastSuccessfulPaginationKey
		if tableProgress.CurrentAction == TableActionWaiting {
			lastSuccessfulPaginationKey = 0
		}
		controlStatus := &ControlServerTableStatus{
			TableName:                   name,
			PaginationKeyName:           this.F.Tables[name].GetPaginationColumn().Name,
			Status:                      tableProgress.CurrentAction,
			LastSuccessfulPaginationKey: lastSuccessfulPaginationKey,
			TargetPaginationKey:         tableProgress.TargetPaginationKey,
		}

		tablesGroupByStatus[tableStatus] = append(tablesGroupByStatus[tableStatus], controlStatus)
	}

	status.TableStatuses = append(
		append(tablesGroupByStatus[TableActionCompleted], tablesGroupByStatus[TableActionCopying]...),
		tablesGroupByStatus[TableActionWaiting]...,
	)

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

	if this.CustomScripts != nil {
		status.CustomScriptStatuses = map[string]CustomScriptStatus{}

		this.customScriptsLock.RLock()
		for name := range this.CustomScripts {
			scriptStatus := this.customScriptsStatus[name]
			if scriptStatus == "" {
				scriptStatus = "not started yet"
			}
			status.CustomScriptStatuses[name] = CustomScriptStatus{
				Name:     name,
				Logs:     this.customScriptsLogs[name],
				Status:   scriptStatus,
				Running:  this.customScriptsRunning[name],
				ExitCode: this.customScriptsExitCode[name],
			}
		}

		this.customScriptsLock.RUnlock()
	}

	return status
}

func (this *ControlServer) HandleScript(w http.ResponseWriter, r *http.Request) {
	if this.CustomScripts == nil {
		http.NotFound(w, r)
		return
	}

	name, err := scriptName(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	cmd, found := this.CustomScripts[name]
	if !found {
		http.NotFound(w, r)
		return
	}

	this.startScriptInBackground(name, cmd)
	returnSuccess(w, r)
}

func (this *ControlServer) HandleConfig(w http.ResponseWriter, r *http.Request) {
	var decoder = json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var updatableConfig UpdatableConfigs
	err := decoder.Decode(&updatableConfig)

	if err != nil {
		this.logger.WithError(err).Error("failed to parse json")
		http.Error(w, "failed to parse json", http.StatusBadRequest)
		return
	}

	this.F.Config.Update(updatableConfig)
	returnSuccess(w, r)
}

func cutoverType(r *http.Request) (string, error) {
	if requestType(r) == "json" {
		type data struct {
			CutoverType string `json:"type"`
		}
		var d data
		err := json.NewDecoder(r.Body).Decode(&d)
		if err != nil {
			return "", err
		}

		return d.CutoverType, nil
	}

	err := r.ParseForm()
	if err != nil {
		return "", err
	}

	return r.Form.Get("type"), nil
}

func requestType(r *http.Request) string {
	contentType := r.Header.Get("Content-type")
	switch contentType {
	case "application/json":
		return "json"
	default:
		return "text"
	}
}

func scriptName(r *http.Request) (string, error) {
	if requestType(r) == "json" {
		type data struct {
			ScriptName string `json:"script-name"`
		}

		var d data
		err := json.NewDecoder(r.Body).Decode(&d)
		if err != nil {
			return "", err
		}

		return d.ScriptName, nil
	}
	err := r.ParseForm()
	if err != nil {
		return "", err
	}

	return r.Form.Get("script"), nil
}

func returnSuccess(w http.ResponseWriter, r *http.Request) {
	if requestType(r) == "json" {
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (this *ControlServer) startScriptInBackground(scriptName string, scriptCmd []string) {
	logger := this.logger.WithField("script", scriptName)

	this.customScriptsLock.Lock()
	defer this.customScriptsLock.Unlock()

	running := this.customScriptsRunning[scriptName]
	if running {
		logger.Warn("script already running, ignoring start request")
		return
	}

	logger.Infof("running custom script %v", scriptCmd)
	this.customScriptsStatus[scriptName] = "starting"
	this.customScriptsRunning[scriptName] = true
	this.customScriptsLogs[scriptName] = ""

	go func() {
		cmd := exec.Command(scriptCmd[0], scriptCmd[1:]...)

		this.customScriptsLock.Lock()
		this.customScriptsStatus[scriptName] = "running"
		this.customScriptsLock.Unlock()

		stdoutStderr, err := cmd.CombinedOutput()

		this.customScriptsLock.Lock()
		if err != nil {
			this.customScriptsStatus[scriptName] = fmt.Sprintf("exitted with error: %v", err)

			var exitError *exec.ExitError
			defaultExitCode := 1
			if errors.As(err, &exitError) {
				this.customScriptsExitCode[scriptName] = exitError.ExitCode()
				logrus.WithFields(logrus.Fields{
					"scriptName": scriptName,
					"error":      err.Error(),
					"exitCode":   exitError.ExitCode(),
				}).Error("custom script ran with errors")
			} else {
				this.customScriptsExitCode[scriptName] = defaultExitCode
				logrus.WithFields(logrus.Fields{
					"scriptName": scriptName,
					"error":      err.Error(),
					"exitCode":   exitError.ExitCode(),
				}).Error("custom script ran with errors but could not determine the exit code")
			}
		} else {
			this.customScriptsStatus[scriptName] = "success"
			this.customScriptsExitCode[scriptName] = 0
			logger.Info("custom script ran successfully")
		}
		this.customScriptsLogs[scriptName] = string(stdoutStderr)
		this.customScriptsRunning[scriptName] = false
		this.customScriptsLock.Unlock()
	}()
}
