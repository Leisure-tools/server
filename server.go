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
	diff "github.com/sergi/go-diff/diffmatchpatch"
)

type LeisureError struct {
	Type string
	Data map[string]any
}

func NewLeisureError(errorType string, values ...any) LeisureError {
	e := LeisureError{Type: errorType}
	if len(values) > 1 {
		e.Data = map[string]any{}
	}
	for i := 0; i+1 < len(values); i += 2 {
		if str, ok := values[i].(string); ok {
			e.Data[str] = values[i+1]
		}
	}
	return e
}

var ErrUnknown = NewLeisureError("unknownError")
var ErrDocumentExists = NewLeisureError("documentExists")
var ErrDocumentAliasExists = NewLeisureError("documentAliasExists")
var ErrCommandFormat = NewLeisureError("badCommandFormat")
var ErrUnknownDocument = NewLeisureError("unknownDocument")
var ErrUnknownSession = NewLeisureError("unknownSession")
var ErrDuplicateSession = NewLeisureError("duplicateSession")
var ErrDuplicateConnection = NewLeisureError("duplicateConnection")
var ErrExpiredSession = NewLeisureError("expiredSession")
var ErrInternalError = NewLeisureError("internalError")

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

type LeisureContext struct {
	*LeisureService
	w http.ResponseWriter
	r *http.Request
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

func (err LeisureError) Error() string {
	return err.Type
}

func ErrorType(errObj any) string {
	if err, ok := errObj.(error); ok {
		for err != nil {
			if e, ok := err.(LeisureError); ok {
				return e.Type
			}
			err = errors.Unwrap(err)
		}
	}
	return ErrUnknown.Type
}

func ErrorData(errObj any) map[string]any {
	if err, ok := errObj.(error); ok {
		for err != nil {
			if e, ok := err.(LeisureError); ok {
				return e.Data
			}
			err = errors.Unwrap(err)
		}
	}
	return nil
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

func (sv *LeisureContext) writeError(lerr LeisureError, format string, args ...any) {
	sv.writeRawError(sv.error(lerr, format, args...))
}

func ErrorJSON(err any) string {
	return string(ErrorJSONBytes(err))
}

func ErrorJSONBytes(err any) []byte {
	m := jmap("error", ErrorType(err), "message", fmt.Sprint(err))
	for key, value := range ErrorData(err) {
		m[key] = value
	}
	bytes, _ := json.Marshal(m)
	return bytes
}

func (sv *LeisureContext) writeRawError(err error) {
	sv.w.WriteHeader(http.StatusBadRequest)
	sv.w.Write(ErrorJSONBytes(err))
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
func (sv *LeisureContext) docCreate() {
	sv.svcSync(func() {
		id := path.Base(sv.r.URL.Path)
		alias := sv.r.URL.Query().Get("alias")
		if sv.documents[id] != nil {
			sv.writeError(ErrDocumentExists, "There is already a document with id %s", id)
		} else if content, err := io.ReadAll(sv.r.Body); err != nil {
			sv.writeError(ErrCommandFormat, "Error reading document content")
		} else if alias != "" && sv.documentAliases[alias] != "" {
			sv.writeError(ErrDocumentAliasExists, "Document alias %s already exists", alias)
		} else {
			str := string(content)
			sv.documents[id] = hist.NewHistory(sv.storageFactory(id, str), str)
			if alias != "" {
				sv.documentAliases[alias] = id
			}
			writeSuccess(sv.w, "")
		}
	})
}

func (sv *LeisureContext) writeResponse(obj any) {
	if data, err := json.Marshal(obj); err != nil {
		sv.writeRawError(err)
	} else {
		writeSuccess(sv.w, data)
	}
}

///
/// LeisureService
///

// URL: /doc/list
// list the documents and their aliases
func (sv *LeisureContext) docList() {
	sv.svcSync(func() {
		docs := [][]string{}
		revAlias := map[string]string{}
		for alias, docId := range sv.documentAliases {
			revAlias[docId] = alias
		}
		for docId := range sv.documents {
			docs = append(docs, []string{docId, revAlias[docId]})
		}
		sv.writeResponse(docs)
	})
}

// URL: /doc/latest/DOC_ID
// Retrieve the latest document contents
func (sv *LeisureContext) docGet() {
	sv.jsonResponseSvc(sv.w, sv.r, func() (any, error) {
		docId, _ := sv.getDocId(sv.r)
		if docId == "" {
			return nil, sv.error(ErrCommandFormat, "Expected document ID in url: %s", sv.r.URL)
		} else if doc := sv.documents[docId]; doc == nil {
			return nil, sv.error(ErrUnknownDocument, "No document for id: %s", docId)
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

// add replacements on the session for changes between the current doc and newDoc
func addChanges(session *LeisureSession, newDoc string) bool {
	// check document and add edits to session
	doc := session.latestBlock().GetDocument(session.History)
	doc.Apply(session.Peer, session.PendingOps)
	dmp := diff.New()
	pos := 0
	changed := false
	for _, dif := range dmp.DiffMain(doc.String(), newDoc, true) {
		switch dif.Type {
		case diff.DiffDelete:
			session.Replace(pos, len(dif.Text), "")
			changed = true
		case diff.DiffEqual:
			pos += len(dif.Text)
		case diff.DiffInsert:
			session.Replace(pos, 0, dif.Text)
			changed = true
		}
	}
	return changed
}

// URL: /session/create/SESSION/DOC
// creates a new session with peer name = sv.id + sv.serial and connects to it.
func (sv *LeisureContext) sessionCreate() {
	sv.svcSync(func() {
		docId, p := sv.getDocId(sv.r)
		sessionId := path.Base(p)
		//fmt.Println("docId:", docId, "sessionId:", sessionId)
		if docId == "" {
			sv.writeError(ErrCommandFormat, "No document ID. Session create requires a session ID and a document ID")
		} else if sessionId == "" {
			sv.writeError(ErrCommandFormat, "No session ID. Session create requires a session ID and a document ID")
		} else if sv.documents[docId] == nil {
			sv.writeError(ErrUnknownDocument, "There is no document with id %s", docId)
		} else if sv.sessions[sessionId] != nil {
			sv.writeError(ErrDuplicateSession, "There is already a session named %s", sessionId)
		} else {
			//fmt.Printf("Create session: %s on document %s\n", sessionId, docId)
			session := sv.addSession(sv.w, sessionId, docId)
			if sv.r.Method == http.MethodPost {
				if incoming, err := io.ReadAll(sv.r.Body); err != nil {
					sv.writeError(ErrCommandFormat, "Could not read document contents")
				} else {
					sv.writeResponse(addChanges(session, string(incoming)))
					return
				}
			}
			writeSuccess(sv.w, "null")
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

func (sv *LeisureService) addSession(w http.ResponseWriter, sessionId, docId string) *LeisureSession {
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
	return session
}

// URL: /doc/list
// list the documents and their aliases
func (sv *LeisureContext) sessionList() {
	sv.svcSync(func() {
		sessions := []string{}
		for name := range sv.sessions {
			sessions = append(sessions, name)
			//fmt.Println("session:", name)
		}
		sv.writeResponse(sessions)
	})
}

// URL: /session/connect/SESSION_ID
// URL: /session/connect/SESSION_ID?doc=ID -- automatically create session SESSION_NAME
// If this is a post
//
//	the body must contain the client's current version of the doc
//	the session computes edits based on its version of the doc and applies them to the session.
//
// returns whether the body contained changes to the session's document
func (sv *LeisureContext) sessionConnect() {
	sv.jsonResponseSvc(sv.w, sv.r, func() (any, error) {
		if sessionId, ok := urlTail(sv.r, SESSION_CONNECT); !ok {
			return nil, sv.error(ErrCommandFormat, "Bad connect format, should be %sID but was %s", SESSION_CONNECT, sv.r.URL.RequestURI())
		} else {
			docId := sv.r.URL.Query().Get("doc")
			if docId != "" && sv.documents[docId] == nil && sv.documentAliases[docId] != "" {
				docId = sv.documentAliases[docId]
			}
			if session := sv.sessions[sessionId]; session == nil && docId == "" {
				return nil, sv.error(ErrUnknownSession, "No session %s", sessionId)
			} else if session == nil && docId != "" && sv.documents[docId] == nil {
				return nil, sv.error(ErrUnknownDocument, "No document %s", docId)
			} else if session != nil && !session.expired() {
				return nil, sv.error(ErrDuplicateConnection, "There is already a connection for %s", sessionId)
			} else {
				if session == nil {
					session = sv.addSession(sv.w, sessionId, docId)
				} else {
					session.connect(sv.w)
				}
				if sv.r.Method == http.MethodPost {
					if incoming, err := io.ReadAll(sv.r.Body); err != nil {
						return nil, sv.error(ErrCommandFormat, "Could not read document contents")
					} else {
						return addChanges(session, string(incoming)), nil
					}
				}
				return false, nil
			}
		}
	})
}

func urlTail(r *http.Request, parent string) (string, bool) {
	head, tail := path.Split(r.URL.Path)
	return tail, parent == head
}

func (sv *LeisureContext) sessionFor(w http.ResponseWriter, r *http.Request) (*LeisureSession, error) {
	cookie, err := r.Cookie("session")
	if err != nil {
		return nil, sv.error(ErrCommandFormat, "No session key")
	} else if keyValue := strings.Split(cookie.Value, "="); len(keyValue) != 2 {
		return nil, sv.error(ErrCommandFormat, "Bad format for session key")
	} else if session := sv.sessions[keyValue[0]]; session == nil {
		return nil, sv.error(ErrCommandFormat, "No session for key")
	} else if session.key.Value != cookie.Value {
		return nil, sv.error(ErrCommandFormat, "Bad key for session")
	} else if session.expired() {
		return nil, sv.error(ErrExpiredSession, "Session expired")
	} else {
		session.lastUsed = time.Now()
		http.SetCookie(w, session.key)
		return session, nil
	}
}

func (sv *LeisureContext) error(lerr LeisureError, format string, args ...any) error {
	args = append(append(make([]any, 0, len(args)+1), lerr), args...)
	err := fmt.Errorf("%w:"+format, args...)
	if cookie, e := sv.r.Cookie("session"); e != nil && cookie != nil {
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
func (sv *LeisureContext) sessionEdit() {
	sv.jsonSvc(sv.w, sv.r, func(repls jsonObj) (result any, err error) {
		// set err if there's a panic
		defer func() { err = asError(recover()) }()
		//fmt.Printf("\nREPLACE: %v\n\n", repls)
		if session, err := sv.sessionFor(sv.w, sv.r); err != nil {
			return nil, err
		} else if !repls.isMap() {
			return nil, sv.error(ErrCommandFormat, "expected a replacement map but got %v", repls.v)
		} else if offset := repls.getJson("selectionOffset"); !offset.isNumber() || offset.asInt() < 0 {
			println("BAD SELECTION OFFSET")
			return nil, sv.error(ErrCommandFormat, "expected a selection offset in the replacement but got: %v", repls.v)
		} else if length := repls.getJson("selectionLength"); !length.isNumber() || length.asInt() < -1 {
			println("BAD SELECTION LENGTH")
			return nil, sv.error(ErrCommandFormat, "expected a selection length in the replacement but got: %v", repls.v)
		} else if repls := repls.getJson("replacements"); !repls.isArray() {
			println("BAD REPLACEMENTS")
			return nil, sv.error(ErrCommandFormat, "expected replacement map with a replacement array but got: %v", repls.v)
		} else {
			repl := make([]hist.Replacement, 0, 4)
			l := repls.len()
			for i := 0; i < l; i++ {
				el := repls.getJson(i)
				//fmt.Println("OBJ:", obj.v, "EL:", el.v)
				if !el.isMap() {
					println("NOT MAP")
					return nil, sv.error(ErrCommandFormat, "expected replacements but got %v", el)
				}
				offset := el.getJson("offset")
				length := el.getJson("length")
				text := el.getJson("text")
				if !(offset.isNumber() && length.isNumber() && (text.isNil() || text.isString())) {
					//fmt.Printf("BAD ARGS OFFSET: %v, LENGTH: %v, TEXT: %v", offset.v, length.v, text.v)
					return nil, sv.error(ErrCommandFormat, "expected replacements but got %v", el)
				} else if text.isNil() {
					text = jsonV("")
				}
				curRepl := doc.Replacement{
					Offset: offset.asInt(),
					Length: length.asInt(),
					Text:   text.asString(),
				}
				if curRepl.Offset < 0 || (curRepl.Length < 0 && curRepl.Offset != 0) {
					return nil, sv.error(ErrCommandFormat, "expected replacements but got %v", el)
				}
				if curRepl.Length == -1 {
					// a complete replacement destroys earlier edits
					repl = repl[:0]
				}
				repl = append(repl, curRepl)
			}
			// Apply will panic if there's a problem with repl and cause the defer to return an erorr
			blk := session.latestBlock()
			blk.GetDocument(session.History).Apply(blk.Peer, repl)
			// done validating inputs
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

func asError(err any) error {
	if result, ok := err.(error); ok || err == nil {
		return result
	}
	return fmt.Errorf("%s", err)
}

func (session *LeisureSession) latestBlock() *hist.OpBlock {
	blk := session.History.Latest[session.Peer]
	if blk == nil {
		blk = session.History.Source
	}
	return blk
}

// URL: /session/get -- get the document for the session
func (sv *LeisureContext) sessionGet() {
	sv.svcSync(func() {
		if session, err := sv.sessionFor(sv.w, sv.r); err != nil {
			sv.writeRawError(err)
		} else {
			doc := session.latestBlock().GetDocument(session.History)
			doc.Apply(session.Peer, session.PendingOps)
			writeSuccess(sv.w, doc.String())
		}
	})
}

func (sv *LeisureContext) sessionUpdate() {
	ch := make(chan bool)
	svc(sv.service, func() {
		session, err := sv.sessionFor(sv.w, sv.r)
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
				sv.jsonResponseSvc(sv.w, sv.r, func() (any, error) {
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
			sv.jsonResponse(sv.w, sv.r, func() (any, error) {
				return true, err
			})
			ch <- true
		}
	})
	<-ch
}

func (sv *LeisureContext) jsonResponseSvc(w http.ResponseWriter, r *http.Request, fn func() (any, error)) {
	sv.svcSync(func() {
		sv.jsonResponse(w, r, fn)
	})
}

func (sv *LeisureContext) jsonSvc(w http.ResponseWriter, r *http.Request, fn func(jsonObj) (any, error)) {
	sv.jsonResponseSvc(w, r, func() (any, error) {
		if body, err := io.ReadAll(r.Body); err != nil {
			return nil, sv.error(ErrCommandFormat, "expected a json body")
		} else {
			var msg any
			if len(body) > 0 {
				if err := json.Unmarshal(body, &msg); err != nil {
					return nil, sv.error(ErrCommandFormat, "expected a json body but got %v", body)
				}
			}
			return fn(jsonV(msg))
		}
	})
}

func (sv *LeisureContext) jsonResponse(w http.ResponseWriter, r *http.Request, fn func() (any, error)) {
	if obj, err := fn(); err != nil {
		sv.writeRawError(err)
	} else {
		sv.writeResponse(obj)
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

func (sv *LeisureService) handle(mux *http.ServeMux, url string, fn func(*LeisureContext)) {
	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		fn(&LeisureContext{sv, w, r})
	})
}

func Initialize(id string, mux *http.ServeMux, storageFactory func(string, string) hist.DocStorage) *LeisureService {
	sv := NewWebService(id, storageFactory)
	sv.handle(mux, DOC_CREATE, (*LeisureContext).docCreate)
	sv.handle(mux, DOC_LIST, (*LeisureContext).docList)
	sv.handle(mux, DOC_GET, (*LeisureContext).docGet)
	sv.handle(mux, SESSION_CREATE, (*LeisureContext).sessionCreate)
	sv.handle(mux, SESSION_LIST, (*LeisureContext).sessionList)
	sv.handle(mux, SESSION_CONNECT, (*LeisureContext).sessionConnect)
	sv.handle(mux, SESSION_EDIT, (*LeisureContext).sessionEdit)
	sv.handle(mux, SESSION_GET, (*LeisureContext).sessionGet)
	sv.handle(mux, SESSION_UPDATE, (*LeisureContext).sessionUpdate)
	runSvc(sv.service)
	return sv
}
