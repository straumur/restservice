package restservice

//TODO: Better use of errchan
import (
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
	"github.com/howbazaar/loggo"
	"github.com/nu7hatch/gouuid"
	"github.com/straumur/straumur"
	"io"
	"net/http"
	"strconv"
	"time"
)

var (
	ErrSaveExisting      = errors.New("Save existing resource")
	ErrUpdateNonExisting = errors.New("Update non-existing resource")
	ErrMissingType       = errors.New("Missing type param")
	ErrInvalidEntity     = errors.New("Invalid entity")
	logger               = loggo.GetLogger("straumur.rest")
	sessionName          = "straumur"
	clientVarName        = "client-id"
)

type RESTService struct {
	Headers     map[string]string
	Store       sessions.Store
	databackend straumur.DataBackend
	events      chan *straumur.Event
	WsServer    *WebSocketServer
	errchan     chan error
}

// Returns the entity prefix
func getEntity(req *http.Request) (string, error) {
	vars := mux.Vars(req)
	entity := vars["entity"]
	id := vars["id"]
	if id == "" || entity == "" {
		return "", ErrInvalidEntity
	}
	return entity + "/" + id, nil
}

// Parses a Query from the request
func getQuery(req *http.Request) (*straumur.Query, error) {
	req.ParseForm()
	return straumur.QueryFromValues(req.Form)
}

// Parses an event from the post body
func (r *RESTService) parseEvent(body io.ReadCloser) (straumur.Event, error) {
	decoder := json.NewDecoder(body)
	defer body.Close()
	var e straumur.Event
	err := decoder.Decode(&e)
	return e, err
}

// Appends a unique session id to the request headers
func (r *RESTService) AddSessionIdHeader(f func(http.ResponseWriter, *http.Request)) http.HandlerFunc {

	return func(w http.ResponseWriter, req *http.Request) {

		session, err := r.Store.Get(req, sessionName)
		if err != nil {
			logger.Errorf("%v", err)
		}

		cidi, ok := session.Values[clientVarName]
		cid := ""

		if !ok {
			logger.Infof("New session")
			u4, err := uuid.NewV4()
			if err != nil {
				logger.Errorf("%v", err)
			}
			cid = u4.String()
			session.Values[clientVarName] = cid
			session.Save(req, w)
		} else {
			cid = cidi.(string)
		}

		req.Header.Add("X-User-Id", cid)
		f(w, req)
	}

}

// Wraps http.HandlerFunc, adds error response and frequently used headers
func (r *RESTService) Middleware(f func(http.ResponseWriter, *http.Request) (error, int)) http.HandlerFunc {

	v := func(w http.ResponseWriter, req *http.Request) {

		for k, v := range r.Headers {
			w.Header().Set(k, v)
		}

		t := time.Now()
		err, status := f(w, req)

		logger.Infof("Processed request: %s:%s in %f seconds", req.Method, req.URL, time.Now().Sub(t).Seconds())

		if err != nil {
			logger.Warningf("Error - [%s]%s, status: %d", req.Method, req.URL, status)
			http.Error(w, err.Error(), status)
		}
	}

	return r.AddSessionIdHeader(v)
}

// GET: /api/entity/id/
func (r *RESTService) entityHandler(w http.ResponseWriter, req *http.Request) (error, int) {
	e, err := getEntity(req)
	if err != nil {
		return err, http.StatusBadRequest
	}
	q, err := getQuery(req)
	if err != nil {
		return err, http.StatusInternalServerError
	}
	q.Entities = append(q.Entities, e)
	events, err := r.databackend.Query(*q)
	if err != nil {
		return err, http.StatusInternalServerError
	}
	enc := json.NewEncoder(w)
	enc.Encode(events)
	r.WsServer.Filters <- FilterPair{req.Header.Get("X-User-Id"), *q, 1}
	return nil, http.StatusOK
}

// GET: /api/id/
func (r *RESTService) retrieveHandler(w http.ResponseWriter, req *http.Request) (error, int) {

	vars := mux.Vars(req)
	id := vars["id"]
	idAsInt, err := strconv.Atoi(id)
	if err != nil {
		return err, http.StatusBadRequest
	}
	event, err := r.databackend.GetById(idAsInt)
	if err != nil {
		return err, http.StatusInternalServerError
	}
	enc := json.NewEncoder(w)
	enc.Encode(event)
	return nil, http.StatusOK
}

// POST: /
// PUT: /id/
// Save or update
func (r *RESTService) saveHandler(w http.ResponseWriter, req *http.Request) (error, int) {

	vars := mux.Vars(req)
	id := vars["id"]
	e, err := r.parseEvent(req.Body)
	if err != nil {
		return err, http.StatusBadRequest
	}
	if id == "" && e.ID != 0 {
		return ErrSaveExisting, http.StatusBadRequest
	}
	if id != "" && e.ID == 0 {
		return ErrUpdateNonExisting, http.StatusBadRequest
	}
	if e.ID == 0 {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusAccepted)
	}

	logger.Infof("Saved event for key %s", e.Key)
	//fix block on error
	go func() { r.events <- &e }()
	return nil, 0
}

// GET: /api/search
func (r *RESTService) searchHandler(w http.ResponseWriter, req *http.Request) (error, int) {
	q, err := getQuery(req)
	if err != nil {
		return err, http.StatusBadRequest
	}
	events, err := r.databackend.Query(*q)
	if err != nil {
		return err, http.StatusInternalServerError
	}
	enc := json.NewEncoder(w)
	enc.Encode(events)
	r.WsServer.Filters <- FilterPair{req.Header.Get("X-User-Id"), *q, 1}
	return nil, http.StatusOK
}

func (r *RESTService) aggregateHandler(w http.ResponseWriter, req *http.Request) (error, int) {
	vars := mux.Vars(req)
	agtype := vars["type"]
	if agtype == "" {
		return ErrMissingType, http.StatusBadRequest
	}
	q, err := getQuery(req)
	if err != nil {
		return err, http.StatusInternalServerError
	}

	m, err := r.databackend.AggregateType(*q, agtype)
	if err != nil {
		return err, http.StatusInternalServerError
	}

	enc := json.NewEncoder(w)
	enc.Encode(m)
	return nil, http.StatusOK
}

func (r *RESTService) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	router := r.getRouter()
	router.ServeHTTP(w, req)
}

func (r *RESTService) getRouter() *mux.Router {
	router := mux.NewRouter()
	s := router.PathPrefix("/api").Subrouter()
	s.HandleFunc("/{entity}/{id}/", r.Middleware(r.entityHandler)).Methods("GET")
	s.HandleFunc("/", r.Middleware(r.saveHandler)).Methods("POST")
	s.HandleFunc("/{id}/", r.Middleware(r.retrieveHandler)).Methods("GET")
	s.HandleFunc("/{id}/", r.Middleware(r.saveHandler)).Methods("PUT")
	s.HandleFunc("/search", r.Middleware(r.searchHandler)).Methods("GET")
	s.HandleFunc("/aggregate/{type}", r.Middleware(r.aggregateHandler)).Methods("GET")
	s.HandleFunc("/ws", r.AddSessionIdHeader(r.WsServer.GetHandler().ServeHTTP))
	return router
}

// Creates a new REST dataservice
func NewRESTService(d straumur.DataBackend, errorChan chan error) *RESTService {

	defaultHeaders := make(map[string]string)
	defaultHeaders["Content-Type"] = "application/json; charset=utf-8"
	defaultHeaders["Access-Control-Allow-Origin"] = "*"
	defaultHeaders["Access-Control-Allow-Headers"] = "Origin, X-Requested-With, Content-Type, Accept"

	rs := RESTService{
		Headers:     defaultHeaders,
		WsServer:    NewWebSocketServer(),
		events:      make(chan *straumur.Event),
		databackend: d,
		errchan:     errorChan,
	}
	go rs.WsServer.Run(errorChan)
	return &rs
}

//EventFeed interface
func (r *RESTService) Updates() <-chan *straumur.Event {
	return r.events
}

func (r *RESTService) Close() error {
	close(r.events)
	//Investigate way to perform graceful shutdown of http
	return nil
}
