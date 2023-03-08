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
	"github.com/leisure-tools/org"
	gouuid "github.com/nu7hatch/gouuid"
	diff "github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/yaml.v3"
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
var ErrDataMissing = NewLeisureError("dataMissing")
var ErrDataMismatch = NewLeisureError("dataMismatch")

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
	sessionId    string
	key          *http.Cookie
	service      *LeisureService
	hasUpdate    bool
	updates      chan bool
	lastUsed     time.Time
	wantsOrg     bool
	wantsStrings bool
	dataMode     bool
	chunks       *org.OrgChunks
}

var emptyConnection LeisureSession

const (
	VERSION          = "/v1"
	DOC_CREATE       = VERSION + "/doc/create/"
	DOC_GET          = VERSION + "/doc/get/"
	DOC_LIST         = VERSION + "/doc/list"
	SESSION_CLOSE    = VERSION + "/session/close"
	SESSION_CONNECT  = VERSION + "/session/connect/"
	SESSION_CREATE   = VERSION + "/session/create/"
	SESSION_LIST     = VERSION + "/session/list"
	SESSION_DOCUMENT = VERSION + "/session/document"
	SESSION_EDIT     = VERSION + "/session/edit"
	SESSION_UPDATE   = VERSION + "/session/update"
	SESSION_GET      = VERSION + "/session/get/"
	SESSION_SET      = VERSION + "/session/set/"
	SESSION_REMOVE   = VERSION + "/session/remove/"
	ORG_PARSE        = VERSION + "/org/parse"
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

// URL: POST /doc/create/ID
// URL: POST /doc/create/ID?alias=ALIAS -- optional alias for document
// creates a new document id from content in body
func (sv *LeisureContext) docCreate() {
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

// URL: GET /doc/list
// list the documents and their aliases
func (sv *LeisureContext) docList() {
	docs := [][]string{}
	revAlias := map[string]string{}
	for alias, docId := range sv.documentAliases {
		revAlias[docId] = alias
	}
	for docId := range sv.documents {
		docs = append(docs, []string{docId, revAlias[docId]})
	}
	sv.writeResponse(docs)
}

// URL: GET /doc/get/DOC_ID
// Retrieve the latest document contents
func (sv *LeisureContext) docGet() (any, error) {
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

// URL: GET or POST /session/create/SESSION/DOC
// creates a new session with peer name = sv.id + sv.serial and connects to it.
// if this was a get request
//
//	string mode returns the current document
//	string & org mode returns {document: doc, chunks: chunks}
//	data mode returns a data map
//
// if the request posted a version of the document
//
//	returns whether there are pending changes
func (sv *LeisureContext) sessionCreate() {
	docId, p := sv.getDocId(sv.r)
	_, wantsOrg, wantsStrings, dataMode := sv.getParams()
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
		session := sv.addSession(sessionId, docId, wantsOrg, wantsStrings, dataMode)
		if sv.r.Method == http.MethodPost {
			if incoming, err := io.ReadAll(sv.r.Body); err != nil {
				sv.writeError(ErrCommandFormat, "Could not read document contents")
			} else {
				sv.writeResponse(addChanges(session, string(incoming)))
			}
			return
		} else if hashStr := sv.r.URL.Query().Get("hash"); hashStr != "" {
			var hash Sha
			if count, err := hex.Decode(hash[:], []byte(hashStr)); err != nil || count < len(hash) {
				sv.writeError(ErrCommandFormat, "Bad hash %s", hashStr)
			} else if incoming := doc.GetDocument(hash); incoming == "" {
				sv.writeError(ErrCommandFormat, "No document for hash %s", hashStr)
			} else {
				latest := session.latestBlock()
				sv.writeResponse(latest.GetDocumentHash(sv.session.History) != hash)
			}
			return
		}
		doc := session.latestBlock().GetDocument(session.History).String()
		if sv.session.dataMode {
			sv.session.chunks = org.Parse(doc)
			data := map[string]any{}
			sv.session.chunks.Chunks.Each(func(ch org.Chunk) bool {
				if src, ok := ch.(*org.SourceBlock); ok && src.IsNamedData() && src.NameStart != -1 {
					data[src.Name()] = src.Value
				}
				return true
			})
			sv.jsonResponse(func() (any, error) {
				return data, nil
			})
		} else if session.wantsOrg {
			content := doc
			sv.session.chunks = org.Parse(content)
			sv.jsonResponse(func() (any, error) {
				return jmap(
					"document", content,
					"chunks", sv.session.chunks,
				), nil
			})
		} else {
			sv.writeSuccess(doc)
		}
	}
}

// URL: GET /session/close
// Close the session, if it exists
func (sv *LeisureContext) sessionClose() (any, error) {
	if sv.session != nil {
		delete(sv.sessions, sv.session.sessionId)
	}
	return true, nil
}

func (sv *LeisureService) removeStaleSessions(d time.Duration) {
	minTime := time.Now().Add(-d)
	for sessionId, session := range sv.sessions {
		if session.lastUsed.Before(minTime) {
			delete(sv.sessions, sessionId)
		}
	}
}

func (sv *LeisureContext) addSession(sessionId, docId string, wantsOrg, wantsStrings, dataOnly bool) *LeisureSession {
	history := sv.documents[docId]
	if history == nil && sv.documentAliases[docId] != "" {
		history = sv.documents[sv.documentAliases[docId]]
	}
	return sv.addSessionWithHistory(sessionId, history, wantsOrg, wantsStrings, dataOnly)
}

func (sv *LeisureContext) addSessionWithHistory(sessionId string, history *hist.History, wantsOrg, wantsStrings, dataOnly bool) *LeisureSession {
	session := &LeisureSession{
		Session:      hist.NewSession(fmt.Sprint(sv.unixSocket, '-', sessionId), history),
		sessionId:    sessionId,
		key:          nil,
		service:      sv.LeisureService,
		hasUpdate:    false,
		updates:      nil,
		lastUsed:     time.Now(),
		wantsOrg:     wantsOrg,
		wantsStrings: wantsStrings,
		dataMode:     dataOnly,
		chunks:       &org.OrgChunks{},
	}
	sv.sessions[sessionId] = session
	sv.session = session
	session.connect(sv.w)
	return session
}

// URL: GET /doc/list
// list the documents and their aliases
func (sv *LeisureContext) sessionList() {
	sessions := []string{}
	for name := range sv.sessions {
		sessions = append(sessions, name)
	}
	sv.writeResponse(sessions)
}

func (sv *LeisureContext) getParams() (string, bool, bool, bool) {
	docId := sv.r.URL.Query().Get("doc")
	wantsStrings := strings.ToLower(sv.r.URL.Query().Get("strings")) != "false"
	wantsOrg := strings.ToLower(sv.r.URL.Query().Get("org")) == "true"
	dataMode := strings.ToLower(sv.r.URL.Query().Get("dataOnly")) == "true"
	return docId, wantsOrg, wantsStrings, dataMode
}

// URL: GET or POST /session/connect/SESSION_ID
//
//	     parameter:
//				doc=ID        -- automatically create session SESSION_NAME
//				org=true      -- request org results
//				strings=false -- stop receiving string results
//
// If this is a post
//
//	the body must contain the client's current version of the doc
//	the session computes edits based on its version of the doc and applies them to the session.
//
// returns whether the body contained changes to the session's document
func (sv *LeisureContext) sessionConnect() (any, error) {
	if sessionId, ok := urlTail(sv.r, SESSION_CONNECT); !ok {
		return nil, sv.error(ErrCommandFormat, "Bad connect format, should be %sID but was %s", SESSION_CONNECT, sv.r.URL.RequestURI())
	} else {
		docId, wantsOrg, wantsStrings, dataMode := sv.getParams()
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
				session := sv.addSession(sessionId, docId, wantsOrg, wantsStrings, dataMode)
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
				sv.addSession(sessionId, docId, wantsOrg, wantsStrings, dataMode)
				return true, nil
			}
		} else if session != nil {
			session.lastUsed = time.Now()
			sv.session = session
		} else if history == nil {
			return nil, sv.error(ErrUnknownDocument, "No document %s", docId)
		} else {
			sv.session = sv.addSessionWithHistory(sessionId, history, wantsOrg, wantsStrings, dataMode)
		}
		content := sv.session.latestBlock().GetDocument(sv.session.History).String()
		result := any(content)
		if sv.session.dataMode {
			sv.session.chunks = org.Parse(content)
			data := map[string]any{}
			sv.session.chunks.Chunks.Each(func(ch org.Chunk) bool {
				if src, ok := ch.(*org.SourceBlock); ok && src.IsNamedData() {
					data[src.Name()] = src.Value
				}
				return true
			})
			result = data
		} else if sv.session.wantsOrg {
			sv.session.chunks = org.Parse(content)
			result = jmap("document", result, "chunks", sv.session.chunks)
		}
		return result, nil
	}
}

func (sv *LeisureContext) orgParse() (any, error) {
	if incoming, err := io.ReadAll(sv.r.Body); err != nil {
		return nil, sv.error(ErrCommandFormat, "Could not read document contents")
	} else {
		return org.Parse(string(incoming)), nil
	}
}

func (sv *LeisureContext) selectedChunks(edits []doc.Replacement) []org.Chunk {
	ch := make([]org.Chunk, 0, sv.session.chunks.Chunks.Measure().Count)
	chSet := make(doc.Set[org.OrgId], cap(ch))
	for _, repl := range edits {
		_, right := sv.session.chunks.Chunks.Split(func(m org.OrgMeasure) bool {
			return repl.Offset > m.Width
		})
		mid, _ := right.Split(func(m org.OrgMeasure) bool {
			return repl.Length > m.Width
		})
		mid.Each(func(chunk org.Chunk) bool {
			if !chSet.Has(chunk.AsOrgChunk().Id) {
				chSet.Add(chunk.AsOrgChunk().Id)
				ch = append(ch, chunk)
			}
			return true
		})
	}
	return ch
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

// URL: POST /session/edit
// Replace text from replacements in body, commit them, and return the resulting edits
func (sv *LeisureContext) sessionEdit(repls jsonObj) (result any, err error) {
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
	} else if repls := repls.getJson("replacements"); !repls.isArray() && !repls.isNil() {
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
		replacements, off, length := sv.session.Commit(offset.asInt(), length.asInt())
		if offset.asInt() > 0 {
			fmt.Printf("OFFSET: %d -> %d\n", offset.asInt(), off)
		}
		return sv.changes(off, length, replacements, len(repl) > 0), nil
	}
}

func (sv *LeisureContext) changes(selOff, selLen int, replacements []doc.Replacement, update bool) map[string]any {
	if update {
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
	result := map[string]any{}
	if !sv.session.dataMode {
		result["replacements"] = replacements
		result["selectionOffset"] = selOff
		result["selectionLength"] = selLen
	}
	if (sv.session.wantsOrg || sv.session.dataMode) && len(replacements) > 0 {
		latest := sv.session.latestBlock()
		prev := latest.PeerParent(sv.session.History).GetDocument(sv.session.History)
		// apply latest replacements to reconstruct the peer's current document
		prev.Apply("", latest.Replacements)
		chunks := org.Parse(prev.String())
		changes := &org.ChunkChanges{}
		for _, repl := range replacements {
			changes.Merge(chunks.Replace(repl.Offset, repl.Length, repl.Text))
		}
		chunks.RelinkHierarchy(changes)
		fmt.Printf("CHUNK CHANGES: %+v\n", changes)
		linkCount := len(changes.Linked)
		changeCount := len(changes.Added) + len(changes.Changed) + len(changes.Removed) + linkCount
		if changeCount == 0 || (changeCount == linkCount && sv.session.dataMode) {
			return nil
		} else if sv.session.dataMode {
			return changes.DataChanges(chunks)
		} else if changeCount > 0 {
			result["order"] = changes.Order(chunks)
			if !changes.Changed.IsEmpty() {
				result["changed"] = sv.chunkSlice(chunks, changes.Changed)
			}
			if !changes.Added.IsEmpty() {
				result["added"] = sv.chunkSlice(chunks, changes.Added)
			}
			if len(changes.Removed) > 0 {
				result["removed"] = changes.Removed
			}
			if len(changes.Linked) > 0 {
				links := make(map[org.OrgId]map[string]any, len(changes.Linked))
				for id, changes := range changes.Linked {
					these := make(map[string]any, len(changes))
					links[id] = these
					for change := range changes {
						switch change {
						case "prev":
							these["prev"] = chunks.Prev[id]
						case "next":
							these["next"] = chunks.Next[id]
						case "parent":
							these["parent"] = chunks.Parent[id]
						case "children":
							these["children"] = chunks.Children[id]
						}
					}
				}
				result["linked"] = links
			}
		}
	}
	return result
}

func (sv *LeisureContext) chunkSlice(chunks *org.OrgChunks, ids doc.Set[org.OrgId]) []org.ChunkRef {
	result := make([]org.ChunkRef, 0, len(ids))
	for id := range ids {
		result = append(result, chunks.GetChunk(string(id)))
	}
	return result
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

// URL: GET /session/document -- get the document for the session
func (sv *LeisureContext) sessionDocument() {
	if err := sv.checkSession(); err != nil {
		sv.writeRawError(err)
	} else {
		doc := sv.session.latestBlock().GetDocument(sv.session.History)
		doc.Apply(sv.session.Peer, sv.session.PendingOps)
		sv.writeSuccess(doc.String())
	}
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

// URL: GET /session/get/NAME
func (sv *LeisureContext) sessionGet() (result any, err error) {
	// set err if there's a panic
	defer func() {
		if errObj := recover(); errObj != nil {
			err = sv.error(errObj, "")
		}
	}()
	if err := sv.checkSession(); err != nil {
		return nil, sv.error(err, "")
	} else if name := path.Base(sv.r.URL.Path); name == "" {
		return nil, sv.error(ErrCommandFormat, "Bad get command, no name")
	} else if data := sv.session.chunks.GetChunkNamed(name); data.Chunks.IsEmpty() {
		return nil, sv.error(ErrDataMissing, "No data named %s", name)
	} else if src, ok := data.Chunk.(*org.SourceBlock); ok {
		if src.IsData() {
			return src.Value, nil
		}
		return nil, sv.error(ErrDataMismatch, "%s is not a data block", name)
	} else if tbl, ok := data.Chunk.(*org.TableBlock); ok {
		return tbl.Value, nil
	}
	return nil, sv.error(ErrUnknown, "Unknown error")
}

// URL: POST /session/set/NAME
func (sv *LeisureContext) sessionSet(arg jsonObj) (result any, err error) {
	force := strings.ToLower(sv.r.URL.Query().Get("force")) == "true"
	// set err if there's a panic
	defer func() {
		if errObj := recover(); errObj != nil {
			err = sv.error(errObj, "")
		}
	}()
	if err := sv.checkSession(); err != nil {
		return nil, sv.error(err, "")
	} else if name := path.Base(sv.r.URL.Path); name == "" {
		return nil, sv.error(ErrCommandFormat, "Bad set command, no name")
	} else if offset, data := sv.session.chunks.LocateChunkNamed(name); data.Chunks.IsEmpty() {
		if !force {
			return nil, sv.error(ErrDataMissing, "No data named %s", name)
		}
		sv.addData(name, arg)
		return true, nil
	} else if src, ok := data.Chunk.(*org.SourceBlock); ok {
		if src.IsData() {
			return sv.setData(offset, src.Content, src.End, src, arg)
		}
		return nil, sv.error(ErrDataMismatch, "%s is not a data block", name)
	} else if tbl, ok := data.Chunk.(*org.TableBlock); ok {
		return sv.setData(offset, tbl.TblStart, 0, tbl, arg)
	}
	return nil, sv.error(ErrUnknown, "Unknown error")
}

func (sv *LeisureContext) addData(name string, value any) (any, error) {
	str := sv.session.latestBlock().GetDocument(sv.session.History).String()
	sb := strings.Builder{}
	if str[len(str)-1] != '\n' {
		sb.WriteRune('\n')
	}
	if bytes, err := yaml.Marshal(value); err != nil {
		return nil, err
	} else {
		text := strings.TrimSpace(string(bytes))
		fmt.Fprintf(&sb, `#+name: %s\n#+begin_src yaml\n%s\n`, name, text)
	}
	return sv.replaceText(-1, -1, len(str), 0, sb.String())
}

func (sv *LeisureContext) replaceText(selOff, selLen, offset, length int, text string) (any, error) {
	sv.session.Replace(offset, length, text)
	sv.session.hasUpdate = false
	replacements, newSelOff, newSelLen := sv.session.Commit(selOff, selLen)
	return sv.changes(newSelOff, newSelLen, replacements, true), nil
}

func (sv *LeisureContext) setData(offset, start, end int, ch org.DataBlock, value jsonObj) (any, error) {
	if newText, err := ch.SetValue(value.v); err != nil {
		return nil, err
	} else {
		fmt.Println("NEW DATA:", newText)
		end += len(newText) - len(ch.AsOrgChunk().Text)
		return sv.replaceText(-1, -1, offset+start, end-start, newText[start:end])
	}
}

// URL: POST /session/add/NAME
func (sv *LeisureContext) sessionAdd(value jsonObj) (result any, err error) {
	// set err if there's a panic
	defer func() {
		if errObj := recover(); errObj != nil {
			err = sv.error(errObj, "")
		}
	}()
	if err := sv.checkSession(); err != nil {
		return nil, sv.error(err, "")
	} else if name := path.Base(sv.r.URL.Path); name == "" {
		return nil, sv.error(ErrCommandFormat, "Bad add command, no name")
	} else if _, data := sv.session.chunks.LocateChunkNamed(name); !data.Chunks.IsEmpty() {
		return nil, sv.error(ErrDataMismatch, "Already data named %s", name)
	} else {
		return sv.addData(name, value)
	}
}

// URL: POST /session/remove/NAME
func (sv *LeisureContext) sessionRemove() (result any, err error) {
	// set err if there's a panic
	defer func() {
		if errObj := recover(); errObj != nil {
			err = sv.error(errObj, "")
		}
	}()
	if err := sv.checkSession(); err != nil {
		return nil, sv.error(err, "")
	} else if name := path.Base(sv.r.URL.Path); name == "" {
		return nil, sv.error(ErrCommandFormat, "Bad add command, no name")
	} else if offset, data := sv.session.chunks.LocateChunkNamed(name); data.Chunks.IsEmpty() {
		return nil, sv.error(ErrDataMismatch, "No data named %s", name)
	} else {
		return sv.replaceText(-1, -1, offset, len(data.Chunk.AsOrgChunk().Text), "")
	}
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

func (sv *LeisureService) handleJson(mux *http.ServeMux, url string, fn func(*LeisureContext, jsonObj) (any, error)) {
	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		ct := &LeisureContext{sv, w, r, sv.findSession(r)}
		ct.jsonSvc(func(arg jsonObj) (any, error) { return fn(ct, arg) })
	})
}

func (sv *LeisureService) handleJsonResponse(mux *http.ServeMux, url string, fn func(ct *LeisureContext) (any, error)) {
	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		ct := &LeisureContext{sv, w, r, sv.findSession(r)}
		ct.jsonResponse(func() (any, error) { return fn(ct) })
	})
}

func (sv *LeisureService) handleSync(mux *http.ServeMux, url string, fn func(*LeisureContext)) {
	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		ct := &LeisureContext{sv, w, r, sv.findSession(r)}
		ct.svcSync(func() { fn(ct) })
	})
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
	sv.handleSync(mux, DOC_CREATE, (*LeisureContext).docCreate)
	sv.handleSync(mux, DOC_LIST, (*LeisureContext).docList)
	sv.handleJsonResponse(mux, DOC_GET, (*LeisureContext).docGet)
	sv.handleSync(mux, SESSION_CREATE, (*LeisureContext).sessionCreate)
	sv.handleJsonResponse(mux, SESSION_CLOSE, (*LeisureContext).sessionClose)
	sv.handleSync(mux, SESSION_LIST, (*LeisureContext).sessionList)
	sv.handleJsonResponse(mux, SESSION_CONNECT, (*LeisureContext).sessionConnect)
	sv.handleJson(mux, SESSION_EDIT, (*LeisureContext).sessionEdit)
	sv.handleSync(mux, SESSION_DOCUMENT, (*LeisureContext).sessionDocument)
	sv.handle(mux, SESSION_UPDATE, (*LeisureContext).sessionUpdate)
	sv.handleJsonResponse(mux, SESSION_GET, (*LeisureContext).sessionGet)
	sv.handleJson(mux, SESSION_SET, (*LeisureContext).sessionSet)
	sv.handleJsonResponse(mux, ORG_PARSE, (*LeisureContext).orgParse)
	runSvc(sv.service)
	return sv
}
