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
	"reflect"
	"time"

	doc "github.com/leisure-tools/document"
	hist "github.com/leisure-tools/history"
)

var ErrBadFormat = errors.New("Bad format, expecting an object with a command property but got")
var ErrCommandFormat = errors.New("Bad command format, expecting")

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
}

type LeisureSession struct {
	*hist.Session
	key       string
	service   *LeisureService
	hasUpdate bool
	updates   chan bool
}

var emptyConnection LeisureSession

const (
	VERSION         = "/v1"
	DOC_CREATE      = VERSION + "/doc/create/"
	DOC_LIST        = VERSION + "/doc/list"
	SESSION_CONNECT = VERSION + "/session/connect/"
	SESSION_CREATE  = VERSION + "/session/create/"
	SESSION_LIST    = VERSION + "/session/list"
	SESSION_GET     = VERSION + "/session/get/"
	SESSION_REPLACE = VERSION + "/session/replace/"
	SESSION_UPDATE  = VERSION + "/session/update"
)

var minutes, _ = time.ParseDuration("1m")

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
			return jsonObj{obj}, true
		}
	}
	return jsonObj{nil}, false
}

func jsonFor(data []byte) (any, error) {
	var msg any
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
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

// URL: /session/create/SESSION/DOC
// creates a new session with peer name = sv.id + sv.serial.
// starts by copying one of the other sessions (they should all have the same trees
func (sv *LeisureService) sessionCreate(w http.ResponseWriter, r *http.Request) {
	sv.svcSync(func() {
		p := r.URL.Path
		docId := path.Base(p)
		if sv.documentAliases[docId] != "" {
			docId = sv.documentAliases[docId]
		}
		p = path.Dir(p)
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
			sv.addConnection(sessionId, docId)
			writeSuccess(w, "")
		}
	})
}

func (sv *LeisureService) addConnection(sessionId, docId string) *LeisureSession {
	session := &LeisureSession{
		hist.NewSession(fmt.Sprint(sv.unixSocket, '-', sessionId), sv.documents[docId]),
		"",
		sv,
		false,
		nil,
	}
	sv.sessions[sessionId] = session
	return session
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
			return nil, fmt.Errorf("Bad connect format, should be %sID but was %s", SESSION_CONNECT, r.URL.RequestURI())
		} else {
			docId := r.URL.Query().Get("docId")
			if docId != "" && sv.documents[docId] == nil && sv.documentAliases[docId] != "" {
				docId = sv.documentAliases[docId]
			}
			if sv.sessions[sessionId] == nil && docId == "" {
				return nil, fmt.Errorf("No session %s", sessionId)
			} else if sv.sessions[sessionId] == nil && docId != "" && sv.documents[docId] == nil {
				return nil, fmt.Errorf("No document %s", docId)
			} else {
				session := sv.sessions[sessionId]
				if session == nil {
					session = sv.addConnection(sessionId, docId)
				} else if session.key != "" {
					return nil, fmt.Errorf("There is already a connection for %s", sessionId)
				}
				key := make([]byte, 16)
				rand.Read(key)
				keyStr := hex.EncodeToString(key)
				session.key = keyStr
				return keyStr, nil
			}
		}
	})
}

func urlTail(r *http.Request, parent string) (string, bool) {
	head, tail := path.Split(r.URL.Path)
	return tail, parent == head
}

func (sv *LeisureService) sessionFor(r *http.Request, url string) (*LeisureSession, error) {
	key := r.URL.Query().Get("key")
	if key == "" {
		return nil, fmt.Errorf("No session key for url %s", r.URL.RequestURI())
	} else if sessionName, ok := urlTail(r, url); !ok {
		return nil, fmt.Errorf("Expected URL %s/SESSION_NAME but got %s", url, r.URL.RequestURI())
	} else if session := sv.sessions[sessionName]; session == nil {
		return nil, fmt.Errorf("No session for %s", sessionName)
	} else if session.key != key {
		return nil, fmt.Errorf("Bad key for session for %s in url: %s", sessionName, r.URL.RequestURI())
	} else {
		return session, nil
	}
}

func (sv *LeisureService) error(r *http.Request, format string, args ...any) error {
	err := fmt.Errorf(format, args...)
	//	sv.logger.Output(2, fmt.Sprintf("Connection %s got error: %s", sv., err))
	return err
}

// URL: /session/replacements/SESSION_KEY -- add sessionReplace in body to session for SESSION_KEY
func (sv *LeisureService) sessionReplace(w http.ResponseWriter, r *http.Request) {
	sv.jsonSvc(w, r, func(repls jsonObj) (any, error) {
		if session, err := sv.sessionFor(r, SESSION_REPLACE); err != nil {
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
				el := jsonObj{repls.get(i)}
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
					text = jsonObj{""}
				}
				repl = append(repl, doc.Replacement{
					Offset: offset.asInt(),
					Length: length.asInt(),
					Text:   text.asString(),
				})
			}
			// done validating arguments
			session.ReplaceAll(repl)
			repl, off, len := session.Commit(offset.asInt(), length.asInt())
			latest := session.History.LatestHashes()
			for _, session := range sv.sessions {
				oldLatest := session.History.Latest[session.Peer]
				if !session.hasUpdate && !hist.SameHashes(latest, oldLatest.Parents) {
					session.hasUpdate = true
					if session.updates != nil {
						// no need to potentially block here
						go func() { session.updates <- true }()
					}
				}
			}
			return jmap("replacements", repl, "selectionOffset", off, "selectionLength", len), nil
		}
	})
}

// URL: /session/get/SESSION_KEY -- get the document for the session
func (sv *LeisureService) httpSessionGet(w http.ResponseWriter, r *http.Request) {
	sv.jsonResponseSvc(w, r, func() (any, error) {
		if session, err := sv.sessionFor(r, SESSION_GET); err != nil {
			return "", err
		} else {
			blk := session.History.Latest[session.Peer]
			if blk == nil {
				blk = session.History.Source
			}
			return blk.GetDocument(session.History).String(), nil
		}
	})
}

func (sv *LeisureService) httpSessionUpdate(w http.ResponseWriter, r *http.Request) {
	sv.svcSync(func() {
		session, err := sv.sessionFor(r, SESSION_GET)
		if err == nil && !session.hasUpdate && session.updates == nil {
			timer := time.After(2 * minutes)
			newUpdates := make(chan bool)
			oldUpdates := session.updates
			session.updates = newUpdates
			go func() {
				hadUpdate := false
				select {
				case hadUpdate = <-newUpdates:
				case <-timer:
				}
				sv.svcSync(func() {
					jsonResponse(w, r, func() (any, error) {
						return hadUpdate, nil
					})
					if session.updates == newUpdates {
						if oldUpdates != nil {
							go func() { oldUpdates <- hadUpdate }()
						}
						session.updates = nil
						session.hasUpdate = false
					}
				})
			}()
		} else {
			jsonResponse(w, r, func() (any, error) {
				return true, err
			})
		}
	})
}

///
/// json
///

func jmap(items ...any) map[string]any {
	result := map[string]any{}
	for i := 0; i < len(items); i += 2 {
		switch s := items[i].(type) {
		case string:
			result[string(s)] = items[i+1]
		default:
			panic(fmt.Sprintf("Expected a string for map key but got %v", items[i]))
		}
	}
	return result
}

type jsonObj struct {
	v any
}

func jsonV(value any) jsonObj {
	return jsonObj{value}
}

func (j jsonObj) value() reflect.Value {
	return reflect.ValueOf(j.v)
}

func (j jsonObj) len() int {
	if j.isArray() {
		return j.value().Len()
	}
	return 0
}

func (j jsonObj) typeof() string {
	switch j.value().Kind() {
	case reflect.Map:
		return "object"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64:
		return "number"
	default:
		return "string"
	}
}

func (j jsonObj) getJson(key any) jsonObj {
	return jsonObj{j.get(key)}
}

func (j jsonObj) get(key any) any {
	v := j.value()
	switch v.Kind() {
	case reflect.Map:
		switch k := key.(type) {
		case string:
			return value(v.MapIndex(reflect.ValueOf(k)))
		}
	case reflect.Slice, reflect.Array:
		var ind int
		switch i := key.(type) {
		case int:
			ind = i
		case int8:
			ind = int(i)
		case int16:
			ind = int(i)
		case int32:
			ind = int(i)
		case int64:
			ind = int(i)
		}
		return value(v.Index(ind))
	}
	return nil
}

func value(v reflect.Value) any {
	switch v.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array:
		return v.Interface()
	case reflect.Bool:
		return v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.String:
		return v.String()
	case reflect.Interface:
		return v.Interface()
	default:
		return nil
	}
}

func (j jsonObj) isNil() bool {
	return j.v == nil
}

func (j jsonObj) isString() bool {
	switch j.v.(type) {
	case string:
		return true
	}
	return false
}

func (j jsonObj) asString() string {
	switch s := j.v.(type) {
	case string:
		return s
	}
	return ""
}

func (j jsonObj) isNumber() bool {
	switch j.v.(type) {
	case int, int8, int16, int32, int64, float32, float64:
		return true
	}
	return false
}

func (j jsonObj) asInt() int {
	switch i := j.v.(type) {
	case int:
		return i
	case int8:
		return int(i)
	case int16:
		return int(i)
	case int32:
		return int(i)
	case int64:
		return int(i)
	case float32:
		return int(i)
	case float64:
		return int(i)
	}
	return 0
}

func (j jsonObj) asInt64() int64 {
	switch i := j.v.(type) {
	case int:
		return int64(i)
	case int8:
		return int64(i)
	case int16:
		return int64(i)
	case int32:
		return int64(i)
	case int64:
		return i
	case float32:
		return int64(i)
	case float64:
		return int64(i)
	}
	return 0
}

func (j jsonObj) isBoolean() bool {
	switch j.v.(type) {
	case bool:
		return true
	}
	return false
}

func (j jsonObj) asFloat64() float64 {
	switch i := j.v.(type) {
	case int:
		return float64(i)
	case int8:
		return float64(i)
	case int16:
		return float64(i)
	case int32:
		return float64(i)
	case int64:
		return float64(i)
	case float32:
		return float64(i)
	case float64:
		return float64(i)
	}
	return 0
}

func (j jsonObj) isArray() bool {
	return j.value().Kind() == reflect.Slice
}

func (j jsonObj) isMap() bool {
	return j.value().Kind() == reflect.Map
}

func (sv *LeisureService) jsonResponseSvc(w http.ResponseWriter, r *http.Request, fn func() (any, error)) {
	sv.svcSync(func() {
		jsonResponse(w, r, fn)
	})
}

func (sv *LeisureService) jsonSvc(w http.ResponseWriter, r *http.Request, fn func(jsonObj) (any, error)) {
	sv.jsonResponseSvc(w, r, func() (any, error) {
		if bodyStream, err := r.GetBody(); err != nil {
			return nil, fmt.Errorf("%w expected a json body", ErrCommandFormat)
		} else if body, err := io.ReadAll(bodyStream); err != nil {
			return nil, fmt.Errorf("%w expected a json body", ErrCommandFormat)
		} else {
			var msg any
			if len(body) > 0 {
				if err := json.Unmarshal(body, &msg); err != nil {
					return nil, fmt.Errorf("%w expected a json body but got %v", ErrCommandFormat, body)
				}
			}
			return fn(jsonObj{msg})
		}
	})
}

func jsonResponse(w http.ResponseWriter, r *http.Request, fn func() (any, error)) {
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
	mux.HandleFunc(SESSION_CREATE, sv.sessionCreate)
	mux.HandleFunc(SESSION_LIST, sv.sessionList)
	mux.HandleFunc(SESSION_CONNECT, sv.sessionConnect)
	mux.HandleFunc(SESSION_REPLACE, sv.sessionReplace)
	mux.HandleFunc(SESSION_GET, sv.httpSessionGet)
	mux.HandleFunc(SESSION_UPDATE, sv.httpSessionUpdate)
	runSvc(sv.service)
	return sv
}

//func start(id string, storageFactory func(string, string) hist.DocStorage) {
//	Initialize(id, http.DefaultServeMux, storageFactory)
//	log.Fatal(http.ListenAndServe(":8080", nil))
//}
