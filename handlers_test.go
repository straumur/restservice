package restservice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/go-querystring/query"
	"github.com/gorilla/sessions"
	"github.com/straumur/straumur"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"sync"
	"testing"
)

var serverAddr string
var once sync.Once
var client *http.Client
var firstEvent *straumur.Event
var secondEvent *straumur.Event
var errChan chan error
var broadcaster straumur.Broadcaster

func startServer() {

	errChan = make(chan error)
	d := straumur.NewLocalMemoryStore()

	firstEvent = straumur.NewEvent(
		"myapp.user.login",
		nil,
		nil,
		"User foobar logged in",
		3,
		"myapp",
		[]string{"user/foo", "ns/moo"},
		nil,
		[]string{"actor1", "actor2"},
		nil)

	secondEvent = straumur.NewEvent(
		"myapp.user.logout",
		nil,
		nil,
		"User foobar logged out",
		2,
		"myapp",
		[]string{"user/foo", "ns/moo"},
		nil,
		[]string{"actor1", "actor2", "actor3"},
		nil)

	err := d.Save(firstEvent)
	if err != nil {
		panic(err)
	}

	err = d.Save(secondEvent)
	if err != nil {
		panic(err)
	}

	rest := NewRESTService(d, errChan)
	rest.Store = sessions.NewCookieStore([]byte("something-very-secret"))

	http.Handle("/", rest)

	server := httptest.NewServer(nil)
	serverAddr = server.Listener.Addr().String() + "/api"
	broadcaster = rest.WsServer

	go func() {
		for {
			select {
			case err := <-errChan:
				log.Fatalf("errChan: %+v", err)
			}
		}
	}()

	go func() {
		for {
			select {
			case event := <-rest.Updates():
				d.Save(event)
			}
		}
	}()

	log.Print("Test Server running on ", serverAddr)
	client = http.DefaultClient
	jar, err := cookiejar.New(nil)
	if err != nil {
		panic(err)
	}
	client.Jar = jar
	log.Print("Test Client created")
}

func makeRequest(t *testing.T, method, url string, v interface{}) *http.Response {

	var r *http.Response

	t.Logf("[%s]: %s", method, url)

	switch method {
	case "POST", "PUT":
		buf, err := json.Marshal(v)
		if err != nil {
			t.Errorf("Unable to serialize %v to json", v)
		}

		log.Printf("[%s] JSON: %s", method, string(buf))
		req, err := http.NewRequest(method, url, bytes.NewReader(buf))
		req.Header.Add("Content-Type", "application/json")
		if err != nil {
			t.Errorf("[%s] %s, error: %v", method, url, err)
		}
		r, err = client.Do(req)
		if err != nil {
			t.Errorf("Error when posting to %s, error: %v", url, err)
		}
	default:
		r, err := client.Get(url)
		if err != nil {
			t.Errorf("Error: %v\n", err)
		}

		if r.StatusCode != http.StatusOK {
			t.Errorf("Wrong status code: %d\n", r.StatusCode)
		}

		dec := json.NewDecoder(r.Body)
		defer r.Body.Close()
		err = dec.Decode(v)
		if err != nil {
			t.Errorf("Error: %v\n", err)
		}
	}

	return r

}

func getJSON(t *testing.T, url string, v interface{}) *http.Response {
	return makeRequest(t, "GET", url, v)
}

func postJSON(t *testing.T, url string, v interface{}) *http.Response {
	return makeRequest(t, "POST", url, v)
}

func putJSON(t *testing.T, url string, v interface{}) *http.Response {
	return makeRequest(t, "PUT", url, v)
}

func TestGetByEntity(t *testing.T) {
	log.Println("TestGetByEntity")
	once.Do(startServer)

	url := fmt.Sprintf("http://%s/user/foo/", serverAddr)
	t.Log(url)
	events := []straumur.Event{}
	getJSON(t, url, &events)
	log.Printf("%v", events)
}

func TestGetById(t *testing.T) {
	log.Println("TestGetById")
	once.Do(startServer)

	url := fmt.Sprintf("http://%s/1/", serverAddr)
	t.Log(url)
	event := straumur.Event{}
	getJSON(t, url, &event)
	log.Printf("%v", event)
}

func TestPostNewEvent(t *testing.T) {
	log.Println("TestPostNewEvent")
	once.Do(startServer)

	e := straumur.Event{
		Key:         "myapp.user.delete",
		Description: "User foobar deleted",
		Importance:  3,
		Origin:      "myapp",
		Entities:    []string{"user/foo"},
	}

	url := fmt.Sprintf("http://%s/", serverAddr)
	r := postJSON(t, url, &e)
	if r.StatusCode != http.StatusCreated {
		t.Errorf("Status code expected %d, got %d", http.StatusCreated, r.StatusCode)
	}
	log.Print("Post succeded")
}

func TestPutEvent(t *testing.T) {
	log.Println("TestPutEvent")
	once.Do(startServer)

	firstEvent.Description = "baz bar foo"
	url := fmt.Sprintf("http://%s/1/", serverAddr)
	r := putJSON(t, url, &firstEvent)
	if r.StatusCode != http.StatusAccepted {
		t.Errorf("Status code expected %d, got %d", http.StatusCreated, r.StatusCode)
	}
	log.Print("PUT succeded")
}

func TestSearch(t *testing.T) {
	log.Println("TestSearch")
	once.Do(startServer)

	tests := []struct {
		Q      straumur.Query
		Status int
	}{{
		Q: straumur.Query{
			Key: "myapp.user.login",
		},
		Status: http.StatusOK,
	}, {
		Q: straumur.Query{
			Key: "myapp.user.login OR myapp.user.logout",
		},
		Status: http.StatusOK,
	}}

	for _, test := range tests {
		v, err := query.Values(test.Q)
		if err != nil {
			t.Fatal(err)
		}
		url := fmt.Sprintf("http://%s/search?%s", serverAddr, v.Encode())
		log.Println(url)
		results := []straumur.Event{}
		getJSON(t, url, &results)
	}

	q := straumur.Query{
		Key: "myapp.user.login OR myapp.user.logout",
	}
	v, err := query.Values(q)
	if err != nil {
		t.Fatal(err)
	}

	url := fmt.Sprintf("http://%s/user/foo/?%s", serverAddr, v.Encode())
	results := []straumur.Event{}
	getJSON(t, url, &results)

}

func TestAggregateType(t *testing.T) {
	once.Do(startServer)
	q := straumur.Query{
		Key: "myapp.user.login OR myapp.user.logout",
	}
	v, err := query.Values(q)
	if err != nil {
		t.Fatal(err)
	}

	url := fmt.Sprintf("http://%s/aggregate/actors?%s", serverAddr, v.Encode())
	m := make(map[string]int)
	getJSON(t, url, &m)

	if m["actor1"] != 2 {
		t.Fatalf("Expected 2, got %d, m: %+v", m["actor1"], m)
	}

	if m["actor3"] != 1 {
		t.Fatalf("Expected 2, got %d, m: %+v", m["actor3"], m)
	}

}

func TestImportanceFilter(t *testing.T) {

	//Tests entity + filter
	q := straumur.Query{
		Importance: "lt3",
	}
	v, err := query.Values(q)
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("http://%s/user/foo/?%s&from=01.01.1994", serverAddr, v.Encode())
	log.Println(url)
	results := []straumur.Event{}
	getJSON(t, url, &results)

	if len(results) != 1 {
		t.Fatalf("Got %d, expected 1, q: %+v, events: %+v", len(results), q, results)
	}

}
