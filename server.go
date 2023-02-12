package server

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"
	"strings"
	"time"

	doc "github.com/leisure-tools/document"
	hist "github.com/leisure-tools/history"
)

var ErrBadFormat = errors.New("Bad format, expecting an object with a command property but got")
var ErrCommandFormat = errors.New("Bad command format, expecting")

const (
	DEFAULT_COOKIE_TIMEOUT = 5 * time.Minute
)

type LeisureService struct {
	unixSocket      string // path of unix domain socket
	serial          int
	documents       map[string]*hist.History
	documentAliases map[string]string
	sessions        map[string]*LeisureSession
	logger          *log.Logger
	service         chanSvc
	storageFactory  func(docId, content string) hist.DocStorage
	updates         map[*LeisureSession]chan bool
	cookieTimeout   time.Duration
}

type LeisureSession struct {
	*hist.Session
	sessionId string
	key       *http.Cookie
	service   *LeisureService
	hasUpdate bool
	updates   chan bool
	lastUsed  time.Time
}

var emptyConnection LeisureSession

const (
	VERSION         = "/v1"
	DOC_CREATE      = VERSION + "/doc/create/"
	DOC_GET         = VERSION + "/doc/get/"
	DOC_LIST        = VERSION + "/doc/list"
	SESSION_CONNECT = VERSION + "/session/connect/"
	SESSION_CREATE  = VERSION + "/session/create/"
	SESSION_LIST    = VERSION + "/session/list"
	SESSION_GET     = VERSION + "/session/get"
	SESSION_EDIT    = VERSION + "/session/edit"
	SESSION_UPDATE  = VERSION + "/session/update"
)

func isGood(w http.ResponseWriter, err any, msg any, status int) bool {
	if err != nil {
		w.Write([]byte(fmt.Sprint(msg)))
		w.WriteHeader(status)
		return false
	}
	return true
}

func readJson(w http.ResponseWriter, r *http.Request) (jsonObj, bool) {
	if bytes, err := io.ReadAll(r.Body); isGood(w, err, "bad request format", http.StatusNotAcceptable) {
		if obj, err := jsonFor(bytes); isGood(w, err, "bad request format", http.StatusNotAcceptable) {
			return jsonV(obj), true
		}
	}
	return jsonV(nil), false
}

func jsonFor(data []byte) (any, error) {
	var msg any
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (session *LeisureSession) expired() bool {
	return time.Now().Sub(session.lastUsed) > time.Duration(session.key.MaxAge*int(time.Second))
}

func (session *LeisureSession) connect(w http.ResponseWriter) {
	key := make([]byte, 16)
	rand.Read(key)
	keyStr := hex.EncodeToString(key)
	session.key = &http.Cookie{
		Name:     "session",
		Value:    session.sessionId + "=" + keyStr,
		Path:     "/",
		MaxAge:   int(session.service.cookieTimeout / time.Second),
		HttpOnly: true,
	}
	session.lastUsed = time.Now()
	http.SetCookie(w, session.key)
}

func NewWebService(id string, storageFactory func(string, string) hist.DocStorage) *LeisureService {
	return &LeisureService{
		unixSocket:      id,
		documents:       map[string]*hist.History{},
		documentAliases: map[string]string{},
		sessions:        map[string]*LeisureSession{},
		logger:          log.Default(),
		service:         make(chanSvc, 10),
		storageFactory:  storageFactory,
		cookieTimeout:   DEFAULT_COOKIE_TIMEOUT,
	}
}

func writeError(w http.ResponseWriter, status int, msg any) {
	w.WriteHeader(status)
	w.Write([]byte(fmt.Sprint(msg)))
}

func writeSuccess(w http.ResponseWriter, msg any) {
	w.WriteHeader(http.StatusOK)
	switch m := msg.(type) {
	case string:
		w.Write([]byte(m))
	case []byte:
		w.Write(m)
	default:
		w.Write([]byte(fmt.Sprint(msg)))
	}
}

// URL: /doc/create/ID
// URL: /doc/create/ID?alias=ALIAS -- optional alias for document
// creates a new document id from content in body
func (sv *LeisureService) docCreate(w http.ResponseWriter, r *http.Request) {
	sv.svcSync(func() {
		id := path.Base(r.URL.Path)
		alias := r.URL.Query().Get("alias")
		if sv.documents[id] != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("There is already a document with id %s", id))
		} else if content, err := io.ReadAll(r.Body); err != nil {
			writeError(w, http.StatusBadRequest, "Error reading document content")
		} else if alias != "" && sv.documentAliases[alias] != "" {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("Document alias %s already exists", alias))
		} else {
			str := string(content)
			sv.documents[id] = hist.NewHistory(sv.storageFactory(id, str), str)
			if alias != "" {
				sv.documentAliases[alias] = id
			}
			writeSuccess(w, "")
		}
	})
}

func writeResponse(w http.ResponseWriter, obj any) {
	if data, err := json.Marshal(obj); isGood(w, err, "internal error", http.StatusInternalServerError) {
		writeSuccess(w, data)
	}
}

///
/// LeisureService
///

// URL: /doc/list
// list the documents and their aliases
func (sv *LeisureService) docList(w http.ResponseWriter, r *http.Request) {
	sv.svcSync(func() {
		docs := [][]string{}
		revAlias := map[string]string{}
		for alias, docId := range sv.documentAliases {
			revAlias[docId] = alias
		}
		for docId := range sv.documents {
			docs = append(docs, []string{docId, revAlias[docId]})
		}
		writeResponse(w, docs)
	})
}

// URL: /doc/latest/DOC_ID
// Retrieve the latest document contents
func (sv *LeisureService) docGet(w http.ResponseWriter, r *http.Request) {
	sv.jsonResponseSvc(w, r, func() (any, error) {
		docId, _ := sv.getDocId(r)
		if docId == "" {
			return nil, sv.error(r, "Expected document ID in url: %s", r.URL)
		} else if doc := sv.documents[docId]; doc == nil {
			return nil, sv.error(r, "No document for id: %s", docId)
		} else {
			return doc.GetLatestDocument().String(), nil
		}
	})
}

// get the the document id from the end of the path (or "") and the remaining path
func (sv *LeisureService) getDocId(r *http.Request) (string, string) {
	p := r.URL.Path
	docId := path.Base(p)
	if sv.documentAliases[docId] != "" {
		docId = sv.documentAliases[docId]
	}
	return docId, path.Dir(p)
}

// URL: /session/create/SESSION/DOC
// creates a new session with peer name = sv.id + sv.serial and connects to it.
func (sv *LeisureService) sessionCreate(w http.ResponseWriter, r *http.Request) {
	sv.svcSync(func() {
		docId, p := sv.getDocId(r)
		sessionId := path.Base(p)
		//fmt.Println("docId:", docId, "sessionId:", sessionId)
		if docId == "" {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("No document ID. Session create requires a session ID and a document ID"))
		} else if sessionId == "" {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("No session ID. Session create requires a session ID and a document ID"))
		} else if sv.documents[docId] == nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("There is no document with id %s", docId))
		} else if sv.sessions[sessionId] != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("There is already a session named %s", sessionId))
		} else {
			//fmt.Printf("Create session: %s on document %s\n", sessionId, docId)
			sv.addSession(w, sessionId, docId)
			writeSuccess(w, "")
		}
	})
}

func (sv *LeisureService) removeStaleSessions(d time.Duration) {
	minTime := time.Now().Add(-d)
	for sessionId, session := range sv.sessions {
		if session.lastUsed.Before(minTime) {
			delete(sv.sessions, sessionId)
		}
	}
}

func (sv *LeisureService) addSession(w http.ResponseWriter, sessionId, docId string) {
	session := &LeisureSession{
		hist.NewSession(fmt.Sprint(sv.unixSocket, '-', sessionId), sv.documents[docId]),
		sessionId,
		nil,
		sv,
		false,
		nil,
		time.Now(),
	}
	sv.sessions[sessionId] = session
	session.connect(w)
}

// URL: /doc/list
// list the documents and their aliases
func (sv *LeisureService) sessionList(w http.ResponseWriter, r *http.Request) {
	sv.svcSync(func() {
		sessions := []string{}
		for name := range sv.sessions {
			sessions = append(sessions, name)
			//fmt.Println("session:", name)
		}
		writeResponse(w, sessions)
	})
}

// URL: /session/connect/SESSION_ID
// URL: /session/connect/SESSION_ID?docId=ID -- automatically create session SESSION_NAME
func (sv *LeisureService) sessionConnect(w http.ResponseWriter, r *http.Request) {
	sv.jsonResponseSvc(w, r, func() (any, error) {
		if sessionId, ok := urlTail(r, SESSION_CONNECT); !ok {
			return nil, sv.error(r, "Bad connect format, should be %sID but was %s", SESSION_CONNECT, r.URL.RequestURI())
		} else {
			docId := r.URL.Query().Get("docId")
			if docId != "" && sv.documents[docId] == nil && sv.documentAliases[docId] != "" {
				docId = sv.documentAliases[docId]
			}
			if session := sv.sessions[sessionId]; session == nil && docId == "" {
				return nil, sv.error(r, "No session %s", sessionId)
			} else if session == nil && docId != "" && sv.documents[docId] == nil {
				return nil, sv.error(r, "No document %s", docId)
			} else if !session.expired() {
				return nil, sv.error(r, "There is already a connection for %s", sessionId)
			} else {
				if session == nil {
					sv.addSession(w, sessionId, docId)
				} else {
					session.connect(w)
				}
				return true, nil
			}
		}
	})
}

func urlTail(r *http.Request, parent string) (string, bool) {
	head, tail := path.Split(r.URL.Path)
	return tail, parent == head
}

func (sv *LeisureService) sessionFor(w http.ResponseWriter, r *http.Request) (*LeisureSession, error) {
	cookie, err := r.Cookie("session")
	if err != nil {
		return nil, sv.error(r, "No session key")
	} else if keyValue := strings.Split(cookie.Value, "="); len(keyValue) != 2 {
		return nil, sv.error(r, "Bad format for session key")
	} else if session := sv.sessions[keyValue[0]]; session == nil {
		return nil, sv.error(r, "No session for key")
	} else if session.key.Value != cookie.Value {
		return nil, sv.error(r, "Bad key for session")
	} else if session.expired() {
		return nil, sv.error(r, "Session expired")
	} else {
		session.lastUsed = time.Now()
		http.SetCookie(w, session.key)
		return session, nil
	}
}

func (sv *LeisureService) error(r *http.Request, format string, args ...any) error {
	err := fmt.Errorf(format, args...)
	if cookie, e := r.Cookie("session"); e != nil && cookie != nil {
		if keyValue := strings.Split(cookie.Value, "="); len(keyValue) != 2 {
			if session := sv.sessions[keyValue[0]]; session == nil {
				if session.key.Value != cookie.Value {
					sv.logger.Output(2, fmt.Sprintf("Connection %s got error: %s", session.sessionId, err))
					return err
				}
			}
		}
	}
	sv.logger.Output(2, fmt.Sprintf("got error: %s", err))
	return err
}

// URL: /session/edit
// Replace text from replacements in body, commit them, and return the resulting edits
func (sv *LeisureService) sessionEdit(w http.ResponseWriter, r *http.Request) {
	sv.jsonSvc(w, r, func(repls jsonObj) (any, error) {
		//fmt.Printf("\nREPLACE: %v\n\n", repls)
		if session, err := sv.sessionFor(w, r); err != nil {
			return nil, err
		} else if !repls.isMap() {
			return nil, sv.error(r, "%w a replacement map but got %v", ErrCommandFormat, repls.v)
		} else if offset := repls.getJson("selectionOffset"); !offset.isNumber() {
			println("NO SELECTION OFFSET")
			return nil, fmt.Errorf("%w a selection offset in the replacement but got: %v", ErrCommandFormat, repls.v)
		} else if length := repls.getJson("selectionLength"); !length.isNumber() {
			println("NO SELECTION LENGTH")
			return nil, fmt.Errorf("%w a selection length in the replacement but got: %v", ErrCommandFormat, repls.v)
		} else if repls := repls.getJson("replacements"); !repls.isArray() {
			println("BAD REPLACEMENTS")
			return nil, fmt.Errorf("%w replacement map with a replacement array but got: %v", ErrCommandFormat, repls.v)
		} else {
			repl := make([]hist.Replacement, 0, 4)
			l := repls.len()
			for i := 0; i < l; i++ {
				el := jsonV(repls.get(i))
				//fmt.Println("OBJ:", obj.v, "EL:", el.v)
				if !el.isMap() {
					println("NOT MAP")
					return nil, fmt.Errorf("%w an array of replacements but got %v", ErrCommandFormat, repls.v)
				}
				offset := el.getJson("offset")
				length := el.getJson("length")
				text := el.getJson("text")
				if !(offset.isNumber() && length.isNumber() && (text.isNil() || text.isString())) {
					//fmt.Printf("BAD ARGS OFFSET: %v, LENGTH: %v, TEXT: %v", offset.v, length.v, text.v)
					return nil, fmt.Errorf("%w an array of replacements but got %v", ErrCommandFormat, repls.v)
				} else if text.isNil() {
					text = jsonV("")
				}
				repl = append(repl, doc.Replacement{
					Offset: offset.asInt(),
					Length: length.asInt(),
					Text:   text.asString(),
				})
			}
			//fmt.Printf("\n\nREPLACE INTO DOC: %s\n", session.History.GetLatestDocument())
			//fmt.Printf("\n\nREPLACE INTO DOC: %s\n", session.History.Latest[session.Peer].GetDocument(session.History).String())
			blk := session.History.Latest[session.Peer]
			if blk == nil {
				blk = session.History.Source
			}
			//fmt.Printf("\n\nREPLACE INTO DOC: %s\n", blk.GetDocument(session.History).String())
			//if bytes.Compare(session.History.LatestHashes()[0][:], session.History.Source.Hash[:]) == 0 {
			//	fmt.Println("DOC IS SOURCE")
			//}
			// done validating arguments
			session.ReplaceAll(repl)
			repl, off, len := session.Commit(offset.asInt(), length.asInt())
			latest := session.History.LatestHashes()
			for _, session := range sv.sessions {
				oldLatest := session.History.Latest[session.Peer]
				if oldLatest == nil {
					oldLatest = session.History.Source
				}
				//fmt.Printf("VARS:\n  session: %v\n  oldLatest: %v\n", session, oldLatest)
				if !session.hasUpdate && !hist.SameHashes(latest, oldLatest.Parents) {
					session.hasUpdate = true
					if session.updates != nil {
						// no need to potentially block here
						s := session
						go func() { s.updates <- true }()
					}
				}
			}
			return jmap("replacements", repl, "selectionOffset", off, "selectionLength", len), nil
		}
	})
}

// URL: /session/get -- get the document for the session
func (sv *LeisureService) httpSessionGet(w http.ResponseWriter, r *http.Request) {
	sv.svcSync(func() {
		if session, err := sv.sessionFor(w, r); err != nil {
			writeError(w, http.StatusBadRequest, err)
		} else {
			blk := session.History.Latest[session.Peer]
			if blk == nil {
				blk = session.History.Source
			}
			doc := blk.GetDocument(session.History).String()
			//fmt.Println("GOT DOC:", doc)
			writeSuccess(w, doc)
		}
	})
}

func (sv *LeisureService) httpSessionUpdate(w http.ResponseWriter, r *http.Request) {
	ch := make(chan bool)
	svc(sv.service, func() {
		session, err := sv.sessionFor(w, r)
		if err == nil && !session.hasUpdate && session.updates == nil {
			timer := time.After(2 * time.Minute)
			newUpdates := make(chan bool)
			oldUpdates := session.updates
			session.updates = newUpdates
			go func() {
				hadUpdate := false
				select {
				case hadUpdate = <-newUpdates:
				case <-timer:
				}
				sv.jsonResponseSvc(w, r, func() (any, error) {
					if session.updates == newUpdates {
						if oldUpdates != nil {
							go func() { oldUpdates <- hadUpdate }()
						}
						session.updates = nil
						session.hasUpdate = false
					}
					return hadUpdate, nil
				})
				ch <- true
			}()
		} else {
			sv.jsonResponse(w, r, func() (any, error) {
				return true, err
			})
			ch <- true
		}
	})
	<-ch
}

func (sv *LeisureService) jsonResponseSvc(w http.ResponseWriter, r *http.Request, fn func() (any, error)) {
	sv.svcSync(func() {
		sv.jsonResponse(w, r, fn)
	})
}

func (sv *LeisureService) jsonSvc(w http.ResponseWriter, r *http.Request, fn func(jsonObj) (any, error)) {
	sv.jsonResponseSvc(w, r, func() (any, error) {
		if body, err := io.ReadAll(r.Body); err != nil {
			return nil, fmt.Errorf("%w expected a json body", ErrCommandFormat)
		} else {
			var msg any
			if len(body) > 0 {
				if err := json.Unmarshal(body, &msg); err != nil {
					return nil, fmt.Errorf("%w expected a json body but got %v", ErrCommandFormat, body)
				}
			}
			return fn(jsonV(msg))
		}
	})
}

func (sv *LeisureService) jsonResponse(w http.ResponseWriter, r *http.Request, fn func() (any, error)) {
	if obj, err := fn(); err != nil {
		writeError(w, http.StatusBadRequest, err)
	} else {
		writeResponse(w, obj)
	}
}

// svcSync protects WebService's internals
func (sv *LeisureService) svcSync(fn func()) {
	// the callers are each in their own goroutine so sync is fine here
	svcSync(sv.service, func() (bool, error) {
		fn()
		return true, nil
	})
}

func (sv *LeisureService) shutdown() {
	close(sv.service)
}

func MemoryStorage(id, content string) hist.DocStorage {
	return hist.NewMemoryStorage(content)
}

func Initialize(id string, mux *http.ServeMux, storageFactory func(string, string) hist.DocStorage) *LeisureService {
	sv := NewWebService(id, storageFactory)
	mux.HandleFunc(DOC_CREATE, sv.docCreate)
	mux.HandleFunc(DOC_LIST, sv.docList)
	mux.HandleFunc(DOC_GET, sv.docGet)
	mux.HandleFunc(SESSION_CREATE, sv.sessionCreate)
	mux.HandleFunc(SESSION_LIST, sv.sessionList)
	mux.HandleFunc(SESSION_CONNECT, sv.sessionConnect)
	mux.HandleFunc(SESSION_EDIT, sv.sessionEdit)
	mux.HandleFunc(SESSION_GET, sv.httpSessionGet)
	mux.HandleFunc(SESSION_UPDATE, sv.httpSessionUpdate)
	runSvc(sv.service)
	return sv
}

//func start(id string, storageFactory func(string, string) hist.DocStorage) {
//	Initialize(id, http.DefaultServeMux, storageFactory)
//	log.Fatal(http.ListenAndServe(":8080", nil))
//}
