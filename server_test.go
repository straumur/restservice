package restservice

import (
	"code.google.com/p/go.net/websocket"
	"fmt"
	"github.com/straumur/straumur"
	"io"
	"log"
	"syscall"
	"testing"
	"time"
)

func TestWebSocketBroadcaster(t *testing.T) {

	url := fmt.Sprintf("ws://%s%s", serverAddr, "/ws")
	conn, err := websocket.Dial(url, "", "http://localhost/")
	if err != nil {
		t.Errorf("WebSocket handshake error: %v", err)
		return
	}

	q := straumur.Query{}
	q.Entities = []string{"ns/moo"}
	t.Logf("Query filter: %+v", q)
	websocket.JSON.Send(conn, q)

	time.Sleep(300 * time.Millisecond)

	e := straumur.NewEvent(
		"myapp.user.login",
		nil,
		nil,
		"User foobar logged in",
		3,
		"myapp",
		[]string{"ns/foo", "ns/moo"},
		nil,
		nil,
		nil)

	broadcaster.Broadcast(e)

	var event straumur.Event
	if err := websocket.JSON.Receive(conn, &event); err != nil {
		t.Errorf("Read: %v", err)
	}

	incoming := make(chan straumur.Event)
	go readEvents(conn, incoming)

	filtered := straumur.NewEvent(
		"Should filter",
		nil,
		nil,
		"This event should be filtered",
		3,
		"myapp",
		[]string{"ns/foo", "ns/boo"},
		nil,
		nil,
		nil)

	if q.Match(*filtered) == true {
		t.Errorf("Query %+v should not pass %+v", q, filtered)
	}

	broadcaster.Broadcast(filtered)

	broadcaster.Broadcast(straumur.NewEvent(
		"foo.bar",
		nil,
		nil,
		"This event should pass",
		3,
		"myapp",
		[]string{"ns/foo", "ns/moo"},
		nil,
		nil,
		nil))

	ev := <-incoming

	if ev.Key != "foo.bar" {
		t.Errorf("Unexpected %s", ev)
	}

}

func readEvents(ws *websocket.Conn, incoming chan straumur.Event) {
	for {
		var event straumur.Event
		err := websocket.JSON.Receive(ws, &event)
		if err == nil {
			log.Println(event)
			incoming <- event
			continue
		}
		if err == io.EOF || err == syscall.EINVAL || err == syscall.ECONNRESET {
			log.Println("Peer disconnected", err.Error())
			return
		}
	}
}
