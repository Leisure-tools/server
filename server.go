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

	doc "github.com/leisure-tools/document"
	hist "github.com/leisure-tools/history"
)

var ErrBadFormat = errors.New("Bad format, expecting an object with a command property but got")
var ErrCommandFormat = errors.New("Bad command format, expecting")

type WebService struct {
	id              string
	serial          int
	documents       map[string]*hist.History
	documentAliases map[string]string
	sessions        map[string]*hist.Session // changes by doc ID
	connectionKeys  map[string]string        // session -> key
	logger          *log.Logger
	service         chanSvc
	storageFactory  func(docId, content string) hist.DocStorage
}

const (
	VERSION         = "/v1"
	DOC_CREATE      = VERSION + "/doc/create/"
	DOC_LIST        = VERSION + "/doc/list"
	SESSION_CONNECT = VERSION + "/session/connect/"
	SESSION_CREATE  = VERSION + "/session/create/"
	SESSION_LIST    = VERSION + "/session/list"
	SESSION_GET     = VERSION + "/session/get/"
	SESSION_REPLACE = VERSION + "/session/replace/"
	SESSION_COMMIT  = VERSION + "/session/commit/"
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

func NewWebService(id string, storageFactory func(string, string) hist.DocStorage) *WebService {
	return &WebService{
		id:              id,
		documents:       map[string]*hist.History{},
		documentAliases: map[string]string{},
		sessions:        map[string]*hist.Session{},
		connectionKeys:  map[string]string{},
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
func (sv *WebService) docCreate(w http.ResponseWriter, r *http.Request) {
	sv.svc(func() {
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

// URL: /doc/list
// list the documents and their aliases
func (sv *WebService) docList(w http.ResponseWriter, r *http.Request) {
	sv.svc(func() {
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
func (sv *WebService) sessionCreate(w http.ResponseWriter, r *http.Request) {
	sv.svc(func() {
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
		} else {
			//fmt.Printf("Create session: %s on document %s\n", sessionId, docId)
			sv.sessions[sessionId] = hist.NewSession(fmt.Sprint(sv.id, '-', sessionId), sv.documents[docId])
			writeSuccess(w, "")
		}
	})
}

// URL: /doc/list
// list the documents and their aliases
func (sv *WebService) sessionList(w http.ResponseWriter, r *http.Request) {
	sv.svc(func() {
		sessions := []string{}
		for name := range sv.sessions {
			sessions = append(sessions, name)
			//fmt.Println("session:", name)
		}
		writeResponse(w, sessions)
	})
}

// URL: /session/connect/SESSION_NAME
// URL: /session/connect/SESSION_NAME?docId=ID -- automatically create session SESSION_NAME
func (sv *WebService) sessionConnect(w http.ResponseWriter, r *http.Request) {
	sv.svc(func() {
		jsonResponse(w, r, func() (any, error) {
			if sessionName, ok := urlTail(r, SESSION_CONNECT); !ok {
				return nil, fmt.Errorf("Bad connect format, should be %sID but was %s", SESSION_CONNECT, r.URL.RequestURI())
			} else {
				docId := r.URL.Query().Get("docId")
				if docId != "" && sv.documents[docId] == nil && sv.documentAliases[docId] != "" {
					docId = sv.documentAliases[docId]
				}
				if sv.connectionKeys[sessionName] != "" {
					return nil, fmt.Errorf("There is already a connection for %s", sessionName)
				} else if sv.sessions[sessionName] == nil && docId == "" {
					return nil, fmt.Errorf("No session %s", sessionName)
				} else if sv.sessions[sessionName] == nil && docId != "" && sv.documents[docId] == nil {
					return nil, fmt.Errorf("No document %s", docId)
				} else {
					if sv.sessions[sessionName] == nil {
						sv.sessions[sessionName] = hist.NewSession(fmt.Sprint(sv.id, '-', sessionName), sv.documents[docId])
					}
					key := make([]byte, 16)
					rand.Read(key)
					keyStr := hex.EncodeToString(key)
					sv.connectionKeys[sessionName] = keyStr
					return keyStr, nil
				}
			}
		})
	})
}

func urlTail(r *http.Request, parent string) (string, bool) {
	head, tail := path.Split(r.URL.Path)
	return tail, parent == head
}

func (sv *WebService) sessionFor(r *http.Request, url string) (*hist.Session, error) {
	key := r.URL.Query().Get("key")
	if key == "" {
		return nil, fmt.Errorf("No session key for url %s", r.URL.RequestURI())
	} else if sessionName, ok := urlTail(r, url); !ok {
		return nil, fmt.Errorf("Expected URL %s/SESSION_NAME but got %s", url, r.URL.RequestURI())
	} else if sv.connectionKeys[sessionName] == "" {
		return nil, fmt.Errorf("No session for %s", sessionName)
	} else if sv.connectionKeys[sessionName] != key {
		return nil, fmt.Errorf("Bad key for session for %s in url: %s", sessionName, r.URL.RequestURI())
	} else {
		return sv.sessions[sessionName], nil
	}
}

// URL: /session/replacements/SESSION_KEY -- add sessionReplace in body to session for SESSION_KEY
func (sv *WebService) sessionReplace(w http.ResponseWriter, r *http.Request) {
	sv.svc(func() {
		withJson(w, r, func(obj jsonObj) (any, error) {
			//fmt.Println("REPLACE:", obj)
			if session, err := sv.sessionFor(r, SESSION_REPLACE); err != nil {
				return nil, err
			} else if !obj.isArray() {
				println("NOT ARRAY")
				return nil, fmt.Errorf("%w an array of replacements but got %v", ErrCommandFormat, obj)
			} else {
				repl := make([]hist.Replacement, 0, 4)
				l := obj.len()
				for i := 0; i < l; i++ {
					el := jsonObj{obj.get(i)}
					//fmt.Println("OBJ:", obj.v, "EL:", el.v)
					if !el.isMap() {
						println("NOT MAP")
						return nil, fmt.Errorf("%w an array of replacements but got %v", ErrCommandFormat, obj.v)
					}
					offset := el.getJson("offset")
					length := el.getJson("length")
					text := el.getJson("text")
					if !(offset.isNumber() && length.isNumber() && (text.isNil() || text.isString())) {
						//fmt.Printf("BAD ARGS OFFSET: %v, LENGTH: %v, TEXT: %v", offset.v, length.v, text.v)
						return nil, fmt.Errorf("%w an array of replacements but got %v", ErrCommandFormat, obj.v)
					} else if text.isNil() {
						text = jsonObj{""}
					}
					repl = append(repl, doc.Replacement{
						Offset: offset.asInt(),
						Length: length.asInt(),
						Text:   text.asString(),
					})
				}
				session.ReplaceAll(repl)
				return nil, nil
			}
		})
	})
}

// URL: /session/replacements/SESSION_KEY -- add replacements in body to session for SESSION_KEY
func (sv *WebService) sessionCommit(w http.ResponseWriter, r *http.Request) {
	sv.svc(func() {
		withJson(w, r, func(obj jsonObj) (any, error) {
			if session, err := sv.sessionFor(r, SESSION_COMMIT); err != nil {
				return nil, err
			} else if !obj.isMap() {
				println("obj is not a map")
				return nil, fmt.Errorf("%w offset and length but got %v", ErrCommandFormat, obj.v)
			} else {
				offset := obj.getJson("selectionOffset")
				length := obj.getJson("selectionLength")
				if !(offset.isNumber() && length.isNumber()) {
					println("length and offset are not both numbers")
					return nil, fmt.Errorf("%w offset and length but got %v", ErrCommandFormat, obj.v)
				}
				repl, off, len := session.Commit(offset.asInt(), length.asInt())
				return jmap("replacements", repl, "selectionOffset", off, "selectionLength", len), nil
			}
		})
	})
}

// URL: /session/get/SESSION_KEY -- add replacements in body to session for SESSION_KEY
func (sv *WebService) sessionGet(w http.ResponseWriter, r *http.Request) {
	sv.svc(func() {
		jsonResponse(w, r, func() (any, error) {
			if session, err := sv.sessionFor(r, SESSION_GET); err != nil {
				return nil, err
			} else {
				blk := session.History.Latest[session.Peer]
				if blk == nil {
					blk = session.History.Source
				}
				return blk.GetDocument(session.History).String(), nil
			}
		})
	})
}

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

func withJson(w http.ResponseWriter, r *http.Request, fn func(jsonObj) (any, error)) {
	var msg any
	if bodyStream, err := r.GetBody(); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("%w expected a json body", ErrCommandFormat))
	} else if body, err := io.ReadAll(bodyStream); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("%w expected a json body", ErrCommandFormat))
	} else {
		if len(body) > 0 {
			if err := json.Unmarshal(body, &msg); err != nil {
				writeError(w, http.StatusBadRequest, fmt.Errorf("%w expected a json body but got %v", ErrCommandFormat, body))
				return
			}
		}
		jsonResponse(w, r, func() (any, error) {
			return fn(jsonObj{msg})
		})
	}
}

func jsonResponse(w http.ResponseWriter, r *http.Request, fn func() (any, error)) {
	if obj, err := fn(); err != nil {
		writeError(w, http.StatusBadRequest, err)
	} else {
		writeResponse(w, obj)
	}
}

// svc protects WebService's internals
func (sv *WebService) svc(fn func()) {
	// the callers are each in their own goroutine so sync is fine here
	svcSync(sv.service, func() bool {
		fn()
		return true
	})
}

func (sv *WebService) shutdown() {
	close(sv.service)
}

func MemoryStorage(id, content string) hist.DocStorage {
	return hist.NewMemoryStorage(content)
}

func initialize(id string, mux *http.ServeMux, storageFactory func(string, string) hist.DocStorage) *WebService {
	sv := NewWebService(id, storageFactory)
	mux.HandleFunc(DOC_CREATE, sv.docCreate)
	mux.HandleFunc(DOC_LIST, sv.docList)
	mux.HandleFunc(SESSION_CREATE, sv.sessionCreate)
	mux.HandleFunc(SESSION_LIST, sv.sessionList)
	mux.HandleFunc(SESSION_CONNECT, sv.sessionConnect)
	mux.HandleFunc(SESSION_REPLACE, sv.sessionReplace)
	mux.HandleFunc(SESSION_COMMIT, sv.sessionCommit)
	mux.HandleFunc(SESSION_GET, sv.sessionGet)
	runSvc(sv.service)
	return sv
}

func start(id string, storageFactory func(string, string) hist.DocStorage) {
	initialize(id, http.DefaultServeMux, storageFactory)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
