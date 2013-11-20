package restservice

import (
	"code.google.com/p/go.net/websocket"
	"github.com/straumur/straumur"
	"net/http"
	"time"
)

type WebSocketServer struct {
	events  chan *straumur.Event
	clients map[string]*Client
	addCh   chan *Client
	delCh   chan *Client
	doneCh  chan bool
	errCh   chan error
	Filters chan FilterPair
}

type FilterPair struct {
	Id       string
	Query    straumur.Query
	Attempts int
}

// Create a new Websocket broadcaster
func NewWebSocketServer() *WebSocketServer {

	clients := make(map[string]*Client)
	addCh := make(chan *Client)
	delCh := make(chan *Client)
	doneCh := make(chan bool)
	errCh := make(chan error)
	events := make(chan *straumur.Event)
	filters := make(chan FilterPair)

	return &WebSocketServer{
		events,
		clients,
		addCh,
		delCh,
		doneCh,
		errCh,
		filters,
	}
}

func (s *WebSocketServer) Add(c *Client) {
	s.addCh <- c
}

func (s *WebSocketServer) Del(c *Client) {
	s.delCh <- c
}

func (s *WebSocketServer) Done() {
	s.doneCh <- true
}

func (s *WebSocketServer) Err(err error) {
	s.errCh <- err
}

func (s *WebSocketServer) sendAll(event *straumur.Event) {
	for _, c := range s.clients {
		logger.Infof("%s %+v", c.Id, c.query)
		if c.query.Match(*event) {
			c.Write(event)
		}
	}
}

func (s *WebSocketServer) Broadcast(e *straumur.Event) {
	s.events <- e
}

func (s *WebSocketServer) GetHandler() http.Handler {

	onConnected := func(ws *websocket.Conn) {

		defer func() {
			err := ws.Close()
			if err != nil {
				s.errCh <- err
			}
		}()
		clientId := ws.Request().Header.Get("X-User-Id")
		logger.Infof("Added client:%s", clientId)
		client := NewClient(ws, s, clientId)
		s.Add(client)
		client.Listen()
	}

	return websocket.Handler(onConnected)

}

func (s *WebSocketServer) FindClientById(uuid string) *Client {
	for idx, c := range s.clients {
		if c.Id == uuid {
			return s.clients[idx]
		}
	}
	return nil
}

func (s *WebSocketServer) Run(ec chan error) {

	for {
		select {

		// Add new a client
		case c := <-s.addCh:
			logger.Debugf("Added new client")
			s.clients[c.Id] = c
			logger.Debugf("Now", len(s.clients), "clients connected.")

		// Attempt to pair the filter with the websocket connection,
		// requeue in case the client hasn't been added or isn't using
		// websockets
		case filter := <-s.Filters:
			client := s.FindClientById(filter.Id)
			if client != nil {
				logger.Infof("Client filter matched %s", client.Id)
				client.query = filter.Query
			} else {
				if filter.Attempts < 3 {
					time.AfterFunc(2*time.Second, func() {
						logger.Infof("Requeing %+v", filter)
						filter.Attempts++
						s.Filters <- filter
					})
				} else {
					logger.Infof("Dropping %+v", filter)
				}
			}

		// del a client
		case c := <-s.delCh:
			logger.Debugf("Delete client")
			delete(s.clients, c.Id)

		// consume event feed
		case event := <-s.events:
			logger.Debugf("Send all:", event)
			s.sendAll(event)

		case err := <-s.errCh:
			logger.Errorf("Error:", err.Error())
			ec <- err

		case <-s.doneCh:
			return
		}
	}
}
