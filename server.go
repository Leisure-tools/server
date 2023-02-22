package server

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"time"

	doc "github.com/leisure-tools/document"
	hist "github.com/leisure-tools/history"
	gouuid "github.com/nu7hatch/gouuid"
	diff "github.com/sergi/go-diff/diffmatchpatch"
)

type Sha = [sha256.Size]byte

type LeisureError struct {
	Type    string
	Data    map[string]any
	wrapped error
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
	cookieTimeout   time.Duration
	verbosity       int
}

type LeisureContext struct {
	*LeisureService
	w       http.ResponseWriter
	r       *http.Request
	session *LeisureSession
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
	SESSION_CLOSE   = VERSION + "/session/close"
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

func (err LeisureError) Unwrap() error {
	return err.wrapped
}

func asLeisureError(err any) LeisureError {
	if le, ok := err.(LeisureError); ok {
		return le
	}
	return LeisureError{fmt.Sprint(err), nil, asError(err)}
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
	session.service.verbose("New session cookie: %v", session.key)
	session.lastUsed = time.Now()
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
	sv.verboseN(1, "Writing error %s", err)
	if sv.verbosity > 0 {
		debug.PrintStack()
	}
	// clear out session
	if sv.session.updates != nil {
		close(sv.session.updates)
		sv.session.hasUpdate = false
	}
	http.SetCookie(sv.w, &http.Cookie{
		Name:     "session",
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(0, 0),
		HttpOnly: true,
	})
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

func (sv *LeisureContext) writeSuccess(msg any) {
	if sv.session != nil {
		http.SetCookie(sv.w, sv.session.key)
	}
	writeSuccess(sv.w, msg)
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
			sv.addDoc(id, alias, string(content))
			sv.writeSuccess("")
		}
	})
}

func (sv *LeisureContext) addDoc(id, alias, content string) {
	sv.documents[id] = hist.NewHistory(sv.storageFactory(id, content), content)
	if alias != "" {
		sv.documentAliases[alias] = id
	}
}

func (sv *LeisureContext) writeResponse(obj any) {
	if data, err := json.Marshal(obj); err != nil {
		sv.verboseN(1, "Writing error %s", err)
		if sv.verbosity > 0 {
			debug.PrintStack()
		}
		sv.writeRawError(err)
	} else {
		sv.verboseN(2, "Writing response %s", string(data))
		sv.writeSuccess(data)
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

// URL: /doc/get/DOC_ID
// Retrieve the latest document contents
func (sv *LeisureContext) docGet() {
	sv.jsonResponseSvc(func() (any, error) {
		docId, _ := sv.getDocId(sv.r)
		if docId == "" {
			return nil, sv.error(ErrCommandFormat, "Expected document ID in url: %s", sv.r.URL)
		} else if doc := sv.documents[docId]; doc == nil {
			return nil, sv.error(ErrUnknownDocument, "No document for id: %s", docId)
		} else if sv.r.URL.Query().Has("hash") {
			var hash Sha
			hashCode := sv.r.URL.Query().Get("hash")
			n, err := hex.Decode(hash[:], []byte(sv.r.URL.Query().Get("hash")))
			if n != len(hash) || err != nil {
				return nil, sv.error(ErrUnknownDocument, "Bad document hash: %s", hashCode)
			}
			result := doc.GetDocument(hash)
			if result == "" {
				return nil, sv.error(ErrUnknownDocument, "No document for hash: %s", hashCode)
			}
			return result, nil
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
// return whether there were changes
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
		if docId == "" {
			sv.writeError(ErrCommandFormat, "No document ID. Session create requires a session ID and a document ID")
		} else if sessionId == "" {
			sv.writeError(ErrCommandFormat, "No session ID. Session create requires a session ID and a document ID")
		} else if doc := sv.documents[docId]; doc == nil {
			sv.writeError(ErrUnknownDocument, "There is no document with id %s", docId)
		} else if sv.sessions[sessionId] != nil {
			sv.writeError(ErrDuplicateSession, "There is already a session named %s", sessionId)
		} else {
			session := sv.addSession(sessionId, docId)
			if sv.r.Method == http.MethodPost {
				if incoming, err := io.ReadAll(sv.r.Body); err != nil {
					sv.writeError(ErrCommandFormat, "Could not read document contents")
				} else {
					sv.writeResponse(addChanges(session, string(incoming)))
					return
				}
			} else if hashStr := sv.r.URL.Query().Get("hash"); hashStr != "" {
				var hash Sha
				if count, err := hex.Decode(hash[:], []byte(hashStr)); err != nil || count < len(hash) {
					sv.writeError(ErrCommandFormat, "Bad hash %s", hashStr)
				} else if incoming := doc.GetDocument(hash); incoming == "" {
					sv.writeError(ErrCommandFormat, "No document for hash %s", hashStr)
				} else {
					sv.writeResponse(addChanges(session, incoming))
				}
			}
			sv.writeSuccess(session.latestBlock().GetDocument(session.History))
		}
	})
}

// URL: /session/close
// Close the session, if it exists
func (sv *LeisureContext) sessionClose() {
	sv.jsonResponseSvc(func() (any, error) {
		if sv.session != nil {
			delete(sv.sessions, sv.session.sessionId)
		}
		return true, nil
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

func (sv *LeisureContext) addSession(sessionId, docId string) *LeisureSession {
	return sv.addSessionWithHistory(sessionId, sv.documents[docId])
}

func (sv *LeisureContext) addSessionWithHistory(sessionId string, history *hist.History) *LeisureSession {
	session := &LeisureSession{
		hist.NewSession(fmt.Sprint(sv.unixSocket, '-', sessionId), history),
		sessionId,
		nil,
		sv.LeisureService,
		false,
		nil,
		time.Now(),
	}
	sv.sessions[sessionId] = session
	sv.session = session
	session.connect(sv.w)
	return session
}

// URL: /doc/list
// list the documents and their aliases
func (sv *LeisureContext) sessionList() {
	sv.svcSync(func() {
		sessions := []string{}
		for name := range sv.sessions {
			sessions = append(sessions, name)
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
	sv.jsonResponseSvc(func() (any, error) {
		if sessionId, ok := urlTail(sv.r, SESSION_CONNECT); !ok {
			return nil, sv.error(ErrCommandFormat, "Bad connect format, should be %sID but was %s", SESSION_CONNECT, sv.r.URL.RequestURI())
		} else {
			docId := sv.r.URL.Query().Get("doc")
			force := strings.ToLower(sv.r.URL.Query().Get("force")) == "true"
			history := sv.documents[docId]
			if history == nil && sv.documentAliases[docId] != "" {
				history = sv.documents[sv.documentAliases[docId]]
			}
			if session := sv.sessions[sessionId]; session == nil && docId == "" {
				return nil, sv.error(ErrUnknownSession, "No session %s", sessionId)
			} else if session != nil && sv.findSession(sv.r) == nil && !force {
				return nil, sv.error(ErrDuplicateConnection, "There is already a connection for %s", sessionId)
			} else if sv.r.Method == http.MethodPost {
				if incoming, err := io.ReadAll(sv.r.Body); err != nil {
					return nil, sv.error(ErrCommandFormat, "Could not read document contents")
				} else if session != nil {
					return addChanges(session, string(incoming)), nil
				} else if history != nil {
					session := sv.addSession(sessionId, docId)
					return addChanges(session, string(incoming)), nil
				} else {
					alias := ""
					uuid, err := gouuid.ParseHex(docId)
					if err != nil {
						// unparseable doc ID means it's an alias, so create an ID
						if uuid, err = gouuid.NewV4(); err != nil {
							return false, sv.error(ErrInternalError, "could not generate uuid: %s", err)
						}
						alias = docId
						docId = uuid.String()
					}
					sv.addDoc(docId, alias, string(incoming))
					sv.addSession(sessionId, docId)
					return true, nil
				}
			} else if session != nil {
				//session.connect(sv.w)
				session.lastUsed = time.Now()
				sv.session = session
			} else if history == nil {
				return nil, sv.error(ErrUnknownDocument, "No document %s", docId)
			} else {
				sv.session = sv.addSessionWithHistory(sessionId, history)
			}
			content := sv.session.latestBlock().GetDocument(sv.session.History).String()
			d := doc.NewDocument("")
			d.Replace("", 0, 0, content)
			return d.Edits(), nil
		}
	})
}

func urlTail(r *http.Request, parent string) (string, bool) {
	head, tail := path.Split(r.URL.Path)
	return tail, parent == head
}

func (sv *LeisureService) findSession(r *http.Request) *LeisureSession {
	if cookie, err := r.Cookie("session"); err == nil {
		if keyValue := strings.Split(cookie.Value, "="); len(keyValue) == 2 {
			if session := sv.sessions[keyValue[0]]; session != nil {
				if session.key.Value == cookie.Value {
					return session
				}
			}
		}
	}
	return nil
}

func (sv *LeisureContext) checkSession() error {
	cookie, err := sv.r.Cookie("session")
	if err != nil {
		return sv.error(ErrCommandFormat, "No session key")
	} else if keyValue := strings.Split(cookie.Value, "="); len(keyValue) != 2 {
		return sv.error(ErrCommandFormat, "Bad format for session key")
	} else if session := sv.sessions[keyValue[0]]; session == nil {
		return sv.error(ErrCommandFormat, "No session for key")
	} else if session.key.Value != cookie.Value {
		return sv.error(ErrCommandFormat, "Bad key for session, expected %s but got %s", session.key.Value, keyValue[1])
	} else if sv.session == nil {
		return sv.error(ErrCommandFormat, "Internal session error, context does not have session %s\n", session.key.Value)
	}
	sv.session.lastUsed = time.Now()
	return nil
}

func (sv *LeisureContext) error(errObj any, format string, args ...any) error {
	lerr := asLeisureError(errObj)
	args = append(append(make([]any, 0, len(args)+1), lerr), args...)
	err := fmt.Errorf("%w:"+format, args...)
	if sv.session != nil {
		fmt.Fprintf(os.Stderr, "Session error for %s: %s\n%s", sv.session.Peer, err.Error(), debug.Stack())
		debug.PrintStack()
		sv.logger.Output(2, fmt.Sprintf("Connection %s got error: %s", sv.session.sessionId, err))
		return err
	}
	fmt.Fprintf(os.Stderr, "Session error [no session]: %s\n%s", err.Error(), debug.Stack())
	debug.PrintStack()
	sv.logger.Output(2, fmt.Sprintf("got error: %s", err))
	return err
}

// URL: /session/edit
// Replace text from replacements in body, commit them, and return the resulting edits
func (sv *LeisureContext) sessionEdit() {
	sv.jsonSvc(func(repls jsonObj) (result any, err error) {
		// set err if there's a panic
		defer func() {
			if errObj := recover(); errObj != nil {
				err = sv.error(errObj, "")
			}
		}()
		if err := sv.checkSession(); err != nil {
			return nil, sv.error(err, "")
		} else if !repls.isMap() {
			return nil, sv.error(ErrCommandFormat, "expected a replacement array but got %v", repls.v)
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
				if !el.isMap() {
					println("NOT MAP")
					return nil, sv.error(ErrCommandFormat, "expected replacements but got %v", el)
				}
				offset := el.getJson("offset")
				length := el.getJson("length")
				text := el.getJson("text")
				if !(offset.isNumber() && length.isNumber() && (text.isNil() || text.isString())) {
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
			blk := sv.session.latestBlock()
			blk.GetDocument(sv.session.History).Apply(blk.Peer, repl)
			// done validating inputs
			sv.verbose("edit: %v", repl)
			sv.session.ReplaceAll(repl)
			sv.session.hasUpdate = false
			result, off, length := sv.session.Commit(offset.asInt(), length.asInt())
			if len(repl) > 0 {
				for _, s := range sv.sessions {
					if s == sv.session || s.hasUpdate {
						continue
					}
					s.hasUpdate = true
					if s.updates != nil {
						// no need to potentially block here
						curSession := s
						go func() { curSession.updates <- true }()
					}
				}
			}
			return jmap("replacements", result, "selectionOffset", off, "selectionLength", length), nil
		}
	})
}

// return whether there is actually new data
// hashes may have changed but new blocks might all be empty
func hasNewData(h *hist.History, parents []Sha) bool {
	//return !hist.SameHashes(h.LatestHashes(), parents)
	latest := h.LatestHashes()
	if !hist.SameHashes(latest, parents) {
		// check for nontrivial descendants
		h.GetBlockOrder()
		for _, parentHash := range parents {
			for descHash := range h.GetBlock(parentHash).GetDescendants() {
				if descHash != parentHash && len(h.GetBlock(descHash).Replacements) > 0 {
					return true
				}
			}
		}
	}
	return false
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
		if err := sv.checkSession(); err != nil {
			sv.writeRawError(err)
		} else {
			doc := sv.session.latestBlock().GetDocument(sv.session.History)
			doc.Apply(sv.session.Peer, sv.session.PendingOps)
			sv.writeSuccess(doc.String())
		}
	})
}

func (sv *LeisureContext) sessionUpdate() {
	ch := make(chan bool)
	svc(sv.service, func() {
		err := sv.checkSession()
		if err == nil && !sv.session.hasUpdate {
			timer := time.After(2 * time.Minute)
			oldUpdates := sv.session.updates
			newUpdates := make(chan bool)
			sv.session.updates = newUpdates
			go func() {
				hadUpdate := false
				select {
				case hadUpdate = <-newUpdates:
				case <-timer:
				}
				sv.jsonResponseSvc(func() (any, error) {
					if oldUpdates != nil {
						go func() { oldUpdates <- hadUpdate }()
					}
					if sv.session.updates == newUpdates {
						sv.session.updates = nil
						sv.session.hasUpdate = false
					}
					return hadUpdate, nil
				})
				ch <- true
			}()
		} else {
			sv.jsonResponse(func() (any, error) {
				if err != nil {
					return nil, sv.error(err, "")
				}
				if sv.session.updates != nil {
					sv.session.updates <- true
				}
				return true, nil
			})
			ch <- true
		}
	})
	<-ch
}

func (sv *LeisureContext) jsonResponseSvc(fn func() (any, error)) {
	sv.svcSync(func() {
		sv.jsonResponse(fn)
	})
}

func (sv *LeisureContext) jsonSvc(fn func(jsonObj) (any, error)) {
	sv.jsonResponseSvc(func() (any, error) {
		if body, err := io.ReadAll(sv.r.Body); err != nil {
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

func (sv *LeisureContext) safeCall(fn func() (any, error)) (good any, bad error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s\n", err)
			debug.PrintStack()
			if err, ok := err.(error); !ok {
				bad = sv.error(err, "")
			} else {
				bad = err
			}
		}
	}()
	good, bad = fn()
	return
}

func (sv *LeisureContext) jsonResponse(fn func() (any, error)) {
	peer := "no session"
	if sv.session != nil {
		peer = sv.session.Peer
	}
	sv.verboseN(2, "Got %s request[%s]: %s", sv.r.Method, peer, sv.r.URL)
	if obj, err := sv.safeCall(fn); err != nil {
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
		fn(&LeisureContext{sv, w, r, sv.findSession(r)})
	})
}

func (sv *LeisureService) verbose(format string, args ...any) {
	sv.verboseN(1, format, args...)
}

func (sv *LeisureService) verboseN(n int, format string, args ...any) {
	if sv.verbosity >= n {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

func (sv *LeisureService) SetVerbose(n int) {
	sv.verbosity = n
}

func Initialize(id string, mux *http.ServeMux, storageFactory func(string, string) hist.DocStorage) *LeisureService {
	sv := NewWebService(id, storageFactory)
	sv.handle(mux, DOC_CREATE, (*LeisureContext).docCreate)
	sv.handle(mux, DOC_LIST, (*LeisureContext).docList)
	sv.handle(mux, DOC_GET, (*LeisureContext).docGet)
	sv.handle(mux, SESSION_CREATE, (*LeisureContext).sessionCreate)
	sv.handle(mux, SESSION_CLOSE, (*LeisureContext).sessionClose)
	sv.handle(mux, SESSION_LIST, (*LeisureContext).sessionList)
	sv.handle(mux, SESSION_CONNECT, (*LeisureContext).sessionConnect)
	sv.handle(mux, SESSION_EDIT, (*LeisureContext).sessionEdit)
	sv.handle(mux, SESSION_GET, (*LeisureContext).sessionGet)
	sv.handle(mux, SESSION_UPDATE, (*LeisureContext).sessionUpdate)
	runSvc(sv.service)
	return sv
}
