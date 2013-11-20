package restservice

import (
	"code.google.com/p/go.net/websocket"
	"fmt"
	"github.com/straumur/straumur"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

var echoServerAddr string
var echoOnce sync.Once

func newConfig(t *testing.T, path string) *websocket.Config {
	config, _ := websocket.NewConfig(fmt.Sprintf("ws://%s%s", echoServerAddr, path), "http://localhost")
	return config
}

func eventServer(ws *websocket.Conn) {
	for {
		var event straumur.Event
		err := websocket.JSON.Receive(ws, &event)
		if err != nil {
			return
		}
		event.ID++
		err = websocket.JSON.Send(ws, event)
		if err != nil {
			return
		}
	}
}

func startEchoServer() {
	http.Handle("/event", websocket.Handler(eventServer))
	server := httptest.NewServer(nil)
	echoServerAddr = server.Listener.Addr().String()
}

func TestSimpleEcho(t *testing.T) {

	echoOnce.Do(startEchoServer)

	url := fmt.Sprintf("ws://%s%s", echoServerAddr, "/event")
	conn, err := websocket.Dial(url, "", "http://localhost/")
	if err != nil {
		t.Errorf("WebSocket handshake error: %v", err)
		return
	}

	var event straumur.Event
	event.Description = "hello"
	if err := websocket.JSON.Send(conn, event); err != nil {
		t.Errorf("Write: %v", err)
	}
	if err := websocket.JSON.Receive(conn, &event); err != nil {
		t.Errorf("Read: %v", err)
	}
	if event.ID != 1 {
		t.Errorf("event: expected %d got %d", 1, event.ID)
	}
	if event.Description != "hello" {
		t.Errorf("event: expected %q got %q", "hello", event.Description)
	}
	if err := websocket.JSON.Send(conn, event); err != nil {
		t.Errorf("Write: %v", err)
	}
	if err := websocket.JSON.Receive(conn, &event); err != nil {
		t.Errorf("Read: %v", err)
	}
	if event.ID != 2 {
		t.Errorf("event: expected %d got %d", 2, event.ID)
	}
	conn.Close()
}
