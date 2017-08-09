package ghostferry

import (
	"html/template"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

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
	this.router.HandleFunc("/api/actions/pause", this.HandlePause).Methods("POST")
	this.router.HandleFunc("/api/actions/unpause", this.HandleUnpause).Methods("POST")
	this.router.HandleFunc("/api/actions/throttle", this.HandleThrottle).Methods("POST")
	this.router.HandleFunc("/api/actions/cutover", this.HandleCutover).Queries("type", "{type:automatic|manual}").Methods("POST")
	this.router.HandleFunc("/api/actions/stop", this.HandleStop).Methods("POST")
	this.router.HandleFunc("/api/actions/verify", this.HandleVerify).Methods("POST")

	if WebUiBasedir != "" {
		this.Basedir = WebUiBasedir
	}

	staticFiles := http.StripPrefix("/static/", http.FileServer(http.Dir(filepath.Join(this.Basedir, "webui", "static"))))
	this.router.PathPrefix("/static/").Handler(staticFiles)

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
	status := FetchStatus(this.F)

	err := this.templates.ExecuteTemplate(w, "index.html", status)
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
