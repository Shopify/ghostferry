package websocket

import (
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWSClient(t *testing.T) {
	http.HandleFunc("/test/client", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Upgrade(w, r, nil, 1024, 1024)
		if err != nil {
			t.Fatal(err.Error())
		}

		msgType, msg, err := conn.ReadMessage()
		conn.WriteMessage(websocket.TextMessage, msg)

		if err != nil {
			t.Fatal(err.Error())
		}

		if msgType != websocket.TextMessage {
			t.Fatal("invalid msg type", msgType)
		}

		msgType, msg, err = conn.ReadMessage()
		if err != nil {
			t.Fatal(err.Error())
		}

		if msgType != websocket.PingMessage {
			t.Fatal("invalid msg type", msgType)
		}

		conn.WriteMessage(websocket.PongMessage, []byte{})

		conn.WriteMessage(websocket.PingMessage, []byte{})

		msgType, msg, err = conn.ReadMessage()
		if err != nil {
			t.Fatal(err.Error())
		}
		println(msgType)
		if msgType != websocket.PongMessage {

			t.Fatal("invalid msg type", msgType)
		}
	})

	go http.ListenAndServe(":65500", nil)

	time.Sleep(time.Second * 1)

	conn, err := net.Dial("tcp", "127.0.0.1:65500")

	if err != nil {
		t.Fatal(err.Error())
	}
	ws, _, err := NewClient(conn, &url.URL{Host: "127.0.0.1:65500", Path: "/test/client"}, nil)

	if err != nil {
		t.Fatal(err.Error())
	}

	payload := make([]byte, 4*1024)
	for i := 0; i < 4*1024; i++ {
		payload[i] = 'x'
	}

	ws.WriteString(payload)

	msgType, msg, err := ws.Read()
	if err != nil {
		t.Fatal(err.Error())
	}
	if msgType != TextMessage {
		t.Fatal("invalid msg type", msgType)
	}

	if string(msg) != string(payload) {
		t.Fatal("invalid msg", string(msg))

	}

	//test ping
	ws.Ping([]byte{})
	msgType, msg, err = ws.ReadMessage()
	if err != nil {
		t.Fatal(err.Error())
	}
	if msgType != PongMessage {
		t.Fatal("invalid msg type", msgType)
	}

}
