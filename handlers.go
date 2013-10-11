package restservice

//TODO: Better use of errchan
import (
	"encoding/json"
	"errors"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/straumur/straumur"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	ErrSaveExisting      = errors.New("Save existing resource")
	ErrUpdateNonExisting = errors.New("Update non-existing resource")
	ErrMissingType       = errors.New("Missing type param")
	ErrInvalidEntity     = errors.New("Invalid entity")
)

type RESTService struct {
	databackend straumur.DataBackend
	address     string
	prefix      string
	events      chan *straumur.Event
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

// Wraps http.HandlerFunc, adds error response and frequently used headers
func handlerWrapper(f func(http.ResponseWriter, *http.Request) (error, int)) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")

		t := time.Now()
		err, status := f(w, r)

		glog.V(2).Infof("Processed request: %s:%s in %f", r.Method, r.URL, time.Now().Sub(t).Seconds())

		if err != nil {
			glog.Warningf("Error - [%s]%s, status: %d", r.Method, r.URL, status)
			http.Error(w, err.Error(), status)
		}
	}
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
	r.events <- &e
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

func (r *RESTService) getRouter() (*mux.Router, error) {

	router := mux.NewRouter()
	s := router.PathPrefix(r.prefix).Subrouter()
	s.HandleFunc("/{entity}/{id}/", handlerWrapper(r.entityHandler)).Methods("GET")
	s.HandleFunc("/", handlerWrapper(r.saveHandler)).Methods("POST")
	s.HandleFunc("/{id}/", handlerWrapper(r.retrieveHandler)).Methods("GET")
	s.HandleFunc("/{id}/", handlerWrapper(r.saveHandler)).Methods("PUT")
	s.HandleFunc("/search", handlerWrapper(r.searchHandler)).Methods("GET")
	s.HandleFunc("/aggregate/{type}", handlerWrapper(r.aggregateHandler)).Methods("GET")
	return router, nil
}

// Creates a new REST dataservice
func NewRESTService(prefix string, address string) *RESTService {
	return &RESTService{
		prefix:  strings.TrimRight(prefix, "/"),
		address: address,
		events:  make(chan *straumur.Event),
	}
}

//DataService interface
func (r *RESTService) Run(d straumur.DataBackend, ec chan error) {

	r.databackend = d

	router, err := r.getRouter()
	if err != nil {
		ec <- err
	}

	http.Handle(r.prefix+"/", router)

	err = http.ListenAndServe(r.address, nil)

	glog.V(2).Info("Server started")

	if err != nil {
		ec <- err
	}

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
