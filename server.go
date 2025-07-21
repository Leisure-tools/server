package server

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	doc "github.com/leisure-tools/document"
	"github.com/leisure-tools/history"
	hist "github.com/leisure-tools/history"
	"github.com/leisure-tools/org"
	u "github.com/leisure-tools/utils"
	gouuid "github.com/nu7hatch/gouuid"
	diff "github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/yaml.v3"
)

// API
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
	SESSION_TAG      = VERSION + "/session/tag/"
	ORG_PARSE        = VERSION + "/org/parse"
	STOP             = VERSION + "/stop"
	UPDATE_TIME      = 2 * time.Minute
)

var MONITOR_BLOCK_TYPES = u.NewSet("monitor", "data", "code", "delete")

type Sha = [sha256.Size]byte

type LeisureError struct {
	Type    string
	Data    map[string]any
	wrapped error
}

type NamedBlock struct {
	Language string
	Params   string
	Content  string
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
var ErrSessionType = NewLeisureError("badSessionType")

const (
	DEFAULT_COOKIE_TIMEOUT = 5 * time.Minute
)

type LeisureService struct {
	unixSocket      string // path of unix domain socket
	serial          int
	Documents       map[string]*hist.History
	DocumentAliases map[string]string
	DocumentIds     map[*hist.History]string
	Sessions        map[string]*LeisureSession // see LeisureSession.Exclusive
	Logger          *log.Logger
	service         ChanSvc
	storageFactory  func(docId, content string) hist.DocStorage
	cookieTimeout   time.Duration
	Verbosity       int
	Listeners       []LeisureListener
}

type LeisureListener interface {
	NewDocument(sv *LeisureService, id string)
}

type LeisureContext struct {
	*LeisureService
	w       http.ResponseWriter
	r       *http.Request
	Session *LeisureSession
}

type SessionListener interface {
	DocumentChanged(session *LeisureSession, changes *org.ChunkChanges, removed map[org.OrgId]org.Chunk)
}

type LeisureSession struct {
	*history.History
	Peer           string
	SessionId      string
	PendingOps     []doc.Replacement
	Follow         string // track a sessionId when making new commits
	key            *http.Cookie
	service        *LeisureService
	HasUpdate      bool
	Updates        chan bool
	lastUsed       time.Time
	wantsOrg       bool
	wantsStrings   bool
	dataMode       bool
	Chunks         *org.OrgChunks
	updateTimeout  time.Duration
	ExclusiveDoc   *doc.Document // only set for an exclusive session -- it has no history
	ExternalBlocks *u.ConcurrentQueue[map[string]any]
	ExternalFmt    func(w io.Writer, m map[string]any) error
	Listeners      []SessionListener
}

type DataChange struct {
	name     string
	value    JsonObj
	data     org.ChunkRef
	location int
}

var emptyConnection LeisureSession

func (err LeisureError) With(keysAndValues ...any) LeisureError {
	result := LeisureError{
		Type:    err.Type,
		Data:    make(map[string]any, len(keysAndValues)/2),
		wrapped: err.wrapped,
	}
	for i := 0; i < len(keysAndValues); i += 2 {
		result.Data[keysAndValues[i].(string)] = keysAndValues[i+1]
	}
	return result
}

func (err LeisureError) Error() string {
	return err.Type
}

func (err LeisureError) Unwrap() error {
	return err.wrapped
}

func AsLeisureError(err any) LeisureError {
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

func (s *LeisureSession) AddListener(l SessionListener) {
	if s.Listeners == nil {
		s.Listeners = make([]SessionListener, 0, 10)
	}
	s.Listeners = append(s.Listeners, l)
}

func (s *LeisureSession) notifyListeners(changes *org.ChunkChanges, removed map[org.OrgId]org.Chunk) {
	for _, l := range s.Listeners {
		l.DocumentChanged(s, changes, removed)
	}
}

// commit pending ops into an opBlock, get its document, and return the replacements
// these will unwind the current document to the common ancestor and replay to the current version
func (s *LeisureSession) Commit(selOff int, selLen int, chng *org.ChunkChanges) ([]doc.Replacement, int, int, error) {
	ops := s.PendingOps
	var removed u.Set[org.OrgId]
	if s.wantsOrg || s.dataMode {
		removed = u.NewSet[org.OrgId]()
		s.orgChanges(ops, chng, removed)
		s.verbose("@@@\n%s REPLACING: %#v, FIRST CHANGES: %#v\n", s.SessionId, ops, chng)
	} else {
		s.verbose("NO CHANGES: %#v\n", chng)
	}
	s.PendingOps = s.PendingOps[:0]
	newRepl, newOff, newLen, err := s.History.Commit(s.Peer, s.SessionId, ops, selOff, selLen)
	if err != nil {
		return nil, 0, 0, err
	}
	if s.wantsOrg || s.dataMode {
		s.orgChanges(newRepl, chng, removed)
		s.verbose("@@@\n%s SECOND CHANGES: %#v\n", s.SessionId, chng)
	}
	return newRepl, newOff, newLen, nil
}

func (s *LeisureSession) ChunkRef(id org.OrgId) org.ChunkRef {
	return org.ChunkRef{
		Chunk:     s.Chunks.ChunkIds[id],
		OrgChunks: s.Chunks,
	}
}
func (s *LeisureSession) orgChanges(repls []doc.Replacement, chs *org.ChunkChanges, removed u.Set[org.OrgId]) {
	if len(repls) == 0 {
		return
	}
	if chs.Linked == nil {
		chs.Linked = make(map[org.OrgId]u.Set[string])
	}
	// this is more efficient that reparsing the entire document
	for _, op := range repls {
		ch := s.Chunks.Replace(op.Offset, op.Length, op.Text)
		chs.Changed = chs.Changed.Merge(ch.Changed)
		chs.Added = chs.Added.Merge(ch.Added)
		for _, r := range ch.Removed {
			if !removed.Has(r) {
				chs.Removed = append(chs.Removed, r)
				removed.Add(r)
			}
		}
		for k, v := range ch.Linked {
			chs.Linked[k] = chs.Linked[k].Merge(v)
		}
	}
}

// add a replacement to pendingOps
func (s *LeisureSession) ReplaceAll(replacements []doc.Replacement) {
	for _, repl := range replacements {
		s.Replace(repl.Offset, repl.Length, repl.Text)
	}
}

// add a replacement to pendingOps
func (s *LeisureSession) Replace(offset int, length int, text string) {
	s.PendingOps = append(s.PendingOps, doc.Replacement{Offset: offset, Length: length, Text: text})
}

func (session *LeisureSession) expired() bool {
	return time.Now().Sub(session.lastUsed) > time.Duration(session.key.MaxAge*int(time.Second))
}

func (session *LeisureSession) Connect() {
	key := make([]byte, 16)
	rand.Read(key)
	keyStr := hex.EncodeToString(key)
	session.key = &http.Cookie{
		Name:     "session",
		Value:    session.SessionId + "=" + keyStr,
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
		Documents:       map[string]*hist.History{},
		DocumentIds:     map[*hist.History]string{},
		DocumentAliases: map[string]string{},
		Sessions:        map[string]*LeisureSession{},
		Logger:          log.Default(),
		service:         make(ChanSvc, 10),
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
	m := JMap("error", ErrorType(err), "message", fmt.Sprint(err))
	for key, value := range ErrorData(err) {
		m[key] = value
	}
	bytes, _ := json.Marshal(m)
	return bytes
}

func (sv *LeisureContext) writeRawError(err error) {
	sv.verboseN(1, "Writing error %s\nrequest: %v", err, sv.r)
	if sv.Verbosity > 0 {
		debug.PrintStack()
	}
	// clear out session
	if sv.Session != nil && sv.Session.Updates != nil {
		close(sv.Session.Updates)
		sv.Session.HasUpdate = false
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

func WriteSuccess(w http.ResponseWriter, msg any) {
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
	if sv.Session != nil {
		http.SetCookie(sv.w, sv.Session.key)
	}
	WriteSuccess(sv.w, msg)
}

// URL: POST /doc/create/ID
// URL: POST /doc/create/ID?alias=ALIAS -- optional alias for document
// creates a new document id from content in body
func (sv *LeisureContext) docCreate() {
	id := path.Base(sv.r.URL.Path)
	alias := sv.r.URL.Query().Get("alias")
	if sv.Documents[id] != nil {
		sv.writeError(ErrDocumentExists, "There is already a document with id %s", id)
	} else if content, err := io.ReadAll(sv.r.Body); err != nil {
		sv.writeError(ErrCommandFormat, "Error reading document content")
	} else if alias != "" && sv.DocumentAliases[alias] != "" {
		sv.writeError(ErrDocumentAliasExists, "Document alias %s already exists", alias)
	} else {
		sv.addDoc(id, alias, string(content))
		sv.writeSuccess(id)
	}
	for _, l := range sv.Listeners {
		l.NewDocument(sv.LeisureService, id)
	}
}

func (sv *LeisureContext) addDoc(id, alias, content string) {
	sv.Documents[id] = hist.NewHistory(sv.storageFactory(id, content), content)
	sv.DocumentIds[sv.Documents[id]] = id
	if alias != "" {
		sv.DocumentAliases[alias] = id
	}
}

func (sv *LeisureContext) writeResponse(obj any) {
	if data, err := json.Marshal(obj); err != nil {
		sv.verboseN(1, "Writing error %s", err)
		if sv.Verbosity > 0 {
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
	for alias, docId := range sv.DocumentAliases {
		revAlias[docId] = alias
	}
	for docId := range sv.Documents {
		docs = append(docs, []string{docId, revAlias[docId]})
	}
	sv.writeResponse(docs)
}

// URL: GET /doc/get/DOC_ID
// Retrieve the latest document contents
func (sv *LeisureContext) docGet() (any, error) {
	if docId, _, err := sv.getDocId(sv.r); err != nil {
		return nil, sv.error(ErrCommandFormat, "bad document path encoding: %s", sv.r.URL.Path)
	} else if docId == "" {
		return nil, sv.error(ErrCommandFormat, "Expected document ID in url: %s", sv.r.URL)
	} else if doc := sv.Documents[docId]; doc == nil {
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
		return sv.documentResult(result)
	} else {
		return sv.documentResult(doc.GetLatestDocument().String())
	}
}

func (sv *LeisureContext) documentResult(doc string, modes ...bool) (any, error) {
	dump := false
	wantData := false
	wantOrg := false
	if len(modes) > 0 {
		dump = modes[0]
		wantData = modes[1]
		wantOrg = modes[2]
	}
	if sv.r.URL.Query().Get("dump") != "" {
		dump = sv.r.URL.Query().Get("dump") == "true"
	}
	if sv.r.URL.Query().Get("org") != "" {
		wantOrg = sv.r.URL.Query().Get("org") == "true"
	}
	if sv.r.URL.Query().Get("data") != "" {
		wantData = sv.r.URL.Query().Get("data") == "true"
	}
	if dump {
		parsed := org.Parse(doc)
		buf := &bytes.Buffer{}
		fmt.Fprintln(buf, "CHUNKS:")
		org.DumpChunks(buf, "", parsed.Chunks)
		fmt.Fprintln(buf, "\nTREE:")
		parsed.Chunks.Dump(buf, 0)
		return buf.String(), nil
	} else if wantData {
		return sv.extractData(wantOrg), nil
	} else if wantOrg {
		return JMap("document", doc, "chunks", org.Parse(doc)), nil
	}
	return doc, nil
}

func (sv *LeisureService) AddListener(l LeisureListener) {
	sv.Listeners = append(sv.Listeners, l)
}

// get the the document id from the end of the path (or "") and the remaining path
func (sv *LeisureService) getDocId(r *http.Request) (string, string, error) {
	p := r.URL.Path
	if docId, err := url.PathUnescape(path.Base(p)); err != nil {
		return "", "", err
	} else {
		if sv.DocumentAliases[docId] != "" {
			docId = sv.DocumentAliases[docId]
		}
		return docId, path.Dir(p), nil
	}
}

// add replacements on the session for changes between the current doc and newDoc
// return whether there were changes
func addChanges(session *LeisureSession, newDoc string) bool {
	// check document and add edits to session
	doc := session.LatestBlock().GetDocument(session.History)
	doc.Apply(session.SessionId, 0, session.PendingOps)
	repls := getEdits(doc.String(), newDoc)
	session.ReplaceAll(repls)
	return len(repls) > 0
}

func getEdits(oldDoc, newDoc string) []doc.Replacement {
	repls := make([]doc.Replacement, 0, 8)
	dmp := diff.New()
	pos := 0
	del := 0
	sb := strings.Builder{}
	save := func() {
		if del+sb.Len() > 0 {
			repls = append(repls, doc.Replacement{Offset: pos, Length: del, Text: sb.String()})
			pos += del
			del = 0
			sb.Reset()
		}
	}
	for _, dif := range dmp.DiffMain(oldDoc, newDoc, true) {
		switch dif.Type {
		case diff.DiffDelete:
			del += len(dif.Text)
		case diff.DiffEqual:
			save()
			pos += len(dif.Text)
		case diff.DiffInsert:
			sb.WriteString(dif.Text)
		}
	}
	save()
	return repls
}

func (sv *LeisureContext) extractData(wantsOrgA ...bool) map[string]any {
	wantsOrg := sv.Session.wantsOrg
	if len(wantsOrgA) > 0 {
		wantsOrg = wantsOrgA[0]
	}
	data := map[string]any{}
	sv.Session.Chunks.Chunks.Each(func(ch org.Chunk) bool {
		name, dt := extractData(ch, wantsOrg)
		if name != "" {
			data[name] = dt
		}
		return true
	})
	return data
}

// return name, data, block, table
// only one of data, block, and table can be non-nil
func extractData(ch interface{}, withOrg bool) (string, any) {
	if blk, ok := ch.(org.DataBlock); ok {
		name := blk.Name()
		if name != "" {
			if withOrg {
				return name, blk
			}
			return name, blk.GetValue()
		}
	}
	return "", nil
}

// URL: GET or POST /session/create/SESSION/DOC
//
// -- REFACTOR: THERE'S A LOT OF OVERLAP WITH sessionConnect
//
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
	docId, p, docIdErr := sv.getDocId(sv.r)
	_, wantsOrg, wantsStrings, dataMode, updateTimeout, badTimeout := sv.getParams()
	sessionId := path.Base(p)
	if badTimeout != "" {
		sv.writeError(ErrCommandFormat, "bad timtout paramater: %s", badTimeout)
	} else if docIdErr != nil {
		sv.writeError(ErrCommandFormat, "bad document path encoding: %s", sv.r.URL.Path)
	} else if docId == "" {
		sv.writeError(ErrCommandFormat, "No document ID. Session create requires a session ID and a document ID")
	} else if sessionId == "" {
		sv.writeError(ErrCommandFormat, "No session ID. Session create requires a session ID and a document ID")
	} else if doc := sv.Documents[docId]; doc == nil {
		sv.writeError(ErrUnknownDocument, "There is no document with id %s", docId)
	} else if sv.Sessions[sessionId] != nil {
		sv.writeError(ErrDuplicateSession, "There is already a session named %s", sessionId)
	} else {
		session, err := sv.addSession(sessionId, docId, wantsOrg, wantsStrings, dataMode, updateTimeout)
		if err != nil {
			sv.writeError(ErrInternalError, fmt.Sprint("Could not add session: ", err))
		}
		if sv.r.Method == http.MethodPost {
			if incoming, err := io.ReadAll(sv.r.Body); err != nil {
				sv.writeError(ErrCommandFormat, "Could not read document contents")
				return
			} else if len(getEdits(session.GetLatestDocument().String(), string(incoming))) == 0 {
				// no changes, return false
				sv.writeResponse(false)
				return
			}
			// continue creating session
		} else if hashStr := sv.r.URL.Query().Get("hash"); hashStr != "" {
			var hash Sha
			if count, err := hex.Decode(hash[:], []byte(hashStr)); err != nil || count < len(hash) {
				sv.writeError(ErrCommandFormat, "Bad hash %s", hashStr)
				return
			} else if incoming := doc.GetDocument(hash); incoming == "" {
				sv.writeError(ErrCommandFormat, "No document for hash %s", hashStr)
				return
			} else if session.Source.GetDocumentHash(sv.Session.History) == hash {
				sv.writeResponse(false)
			}
			// otherwise continue creating session
		}
		doc := session.GetLatestDocument().String()
		//fmt.Println("Datamode: ", sv.session.dataMode)
		if sv.Session.dataMode {
			sv.Session.Chunks = org.Parse(doc)
			data := sv.extractData()
			sv.jsonResponse(func() (any, error) {
				return data, nil
			})
		} else if session.wantsOrg {
			content := doc
			sv.Session.Chunks = org.Parse(content)
			sv.jsonResponse(func() (any, error) {
				return JMap(
					"document", content,
					"chunks", sv.Session.Chunks,
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
	if sv.Session != nil {
		delete(sv.Sessions, sv.Session.SessionId)
	}
	return true, nil
}

func (sv *LeisureService) removeStaleSessions(d time.Duration) {
	minTime := time.Now().Add(-d)
	for sessionId, session := range sv.Sessions {
		if session.lastUsed.Before(minTime) {
			delete(sv.Sessions, sessionId)
		}
	}
}

func (sv *LeisureContext) addSession(sessionId, docId string, wantsOrg, wantsStrings, dataOnly bool, updateTimeout time.Duration) (*LeisureSession, error) {
	history := sv.Documents[docId]
	if history == nil && sv.DocumentAliases[docId] != "" {
		history = sv.Documents[sv.DocumentAliases[docId]]
	}
	return sv.addSessionWithHistory(sessionId, history, wantsOrg, wantsStrings, dataOnly, updateTimeout)
}

func (s *LeisureSession) hashNums() map[Sha]int {
	hashes := make(map[Sha]int)
	order := s.GetBlockOrder()
	for i, v := range order {
		hashes[v] = i
	}
	return hashes
}

func (sv *LeisureService) AddSession(sessionId string, history *hist.History, wantsOrg, wantsStrings, dataOnly bool, updateTimeout time.Duration) (*LeisureSession, error) {
	session := &LeisureSession{
		Peer: "",
		//SessionId:    fmt.Sprint(sv.unixSocket, '-', sessionId),
		SessionId:     sessionId,
		PendingOps:    make([]doc.Replacement, 0, 8),
		History:       history,
		Follow:        "",
		key:           nil,
		service:       sv,
		HasUpdate:     false,
		Updates:       nil,
		lastUsed:      time.Now(),
		wantsOrg:      wantsOrg,
		wantsStrings:  wantsStrings,
		dataMode:      dataOnly,
		Chunks:        &org.OrgChunks{},
		updateTimeout: updateTimeout,
	}
	sv.Sessions[session.SessionId] = session
	if len(history.Latest) > 0 && history.Latest[session.SessionId] == nil {
		var trackChunks org.ChunkChanges
		_, _, _, err := session.Commit(0, 0, &trackChunks)
		if err != nil {
			return nil, err
		}
	}
	return session, nil
}

func (sv *LeisureContext) addSessionWithHistory(sessionId string, history *hist.History, wantsOrg, wantsStrings, dataOnly bool, updateTimeout time.Duration) (*LeisureSession, error) {
	session, err := sv.AddSession(sessionId, history, wantsOrg, wantsStrings, dataOnly, updateTimeout)
	if err != nil {
		return nil, err
	}
	sv.Session = session
	session.Connect()
	return session, nil
}

// URL: GET /doc/list
// list the documents and their aliases
func (sv *LeisureContext) sessionList() {
	sessions := []string{}
	for name := range sv.Sessions {
		sessions = append(sessions, name)
	}
	sv.writeResponse(sessions)
}

func (sv *LeisureContext) getParams() (string, bool, bool, bool, time.Duration, string) {
	docId := sv.r.URL.Query().Get("doc")
	wantsStrings := strings.ToLower(sv.r.URL.Query().Get("strings")) != "false"
	wantsOrg := strings.ToLower(sv.r.URL.Query().Get("org")) == "true"
	dataMode := strings.ToLower(sv.r.URL.Query().Get("dataOnly")) == "true"
	updateTimeout := UPDATE_TIME
	badTimeout := ""
	if waitTime := sv.r.URL.Query().Get("timeout"); waitTime != "" {
		if _, err := fmt.Sscanf(waitTime, "%d", &updateTimeout); err != nil {
			badTimeout = waitTime
		}
	}
	return docId, wantsOrg, wantsStrings, dataMode, updateTimeout, badTimeout
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
	sessionId, ok := urlTail(sv.r, SESSION_CONNECT)
	if !ok {
		return nil, sv.error(ErrCommandFormat, "Bad connect format, should be %sID but was %s", SESSION_CONNECT, sv.r.URL.RequestURI())
	}
	docId, wantsOrg, wantsStrings, dataMode, timeout, badTimeout := sv.getParams()
	if badTimeout != "" {
		return nil, sv.error(ErrCommandFormat, "Bad timeout parameter: %s", badTimeout)
	}
	force := strings.ToLower(sv.r.URL.Query().Get("force")) == "true"
	history := sv.Documents[docId]
	if history == nil && sv.DocumentAliases[docId] != "" {
		history = sv.Documents[sv.DocumentAliases[docId]]
	}
	session := sv.Sessions[sessionId]
	sv.verbose("session id: %s, found session: %b", sessionId, session != nil)
	if session != nil {
		session.lastUsed = time.Now()
		session.wantsOrg = wantsOrg
	}
	if session == nil && docId == "" {
		return nil, sv.error(ErrUnknownSession, "No session %s", sessionId)
	} else if session != nil && sv.FindSession(sv.r) == nil && !force {
		return nil, sv.error(ErrDuplicateConnection, "There is already a connection for %s", sessionId)
	} else if sv.r.Method == http.MethodPost {
		sv.verbose("CREATE SESSION WITH POST")
		if incoming, err := io.ReadAll(sv.r.Body); err != nil {
			return nil, sv.error(ErrCommandFormat, "Could not read document contents")
		} else if session != nil {
			// document may have changed since last connect
			return addChanges(session, string(incoming)), nil
		} else if history != nil {
			// this is a new session and the document already exists
			if len(getEdits(history.GetLatestDocument().String(), string(incoming))) == 0 {
				_, err := sv.addSession(sessionId, docId, wantsOrg, wantsStrings, dataMode, timeout)
				return false, err
			}
			// continue with connection
		} else {
			// this is a new document
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
			_, err = sv.addSession(sessionId, docId, wantsOrg, wantsStrings, dataMode, timeout)
			return true, err
		}
	} else if session == nil {
		sv.verbose("CREATE SESSION WITH GET, NO SESSION")
	} else {
		sv.verbose("CREATE SESSION WITH GET, FOUND SESSION")
	}
	if session == nil {
		if history == nil {
			return nil, sv.error(ErrUnknownDocument, "No document %s", docId)
		}
		var err error
		sv.Session, err = sv.addSessionWithHistory(sessionId, history, wantsOrg, wantsStrings, dataMode, timeout)
		if err != nil {
			return nil, err
		}
	} else {
		sv.Session = session
		sv.verbose("Found session %s, exclusive: %v", session.SessionId, sv.Session.ExclusiveDoc != nil)
	}
	content := ""
	if sv.Session.ExclusiveDoc != nil {
		content = sv.Session.ExclusiveDoc.String()
	} else {
		content = sv.Session.GetLatestDocument().String()
	}
	sv.verbose("CONTENT: %v", content)
	var result any
	if sv.Session.dataMode {
		if session == nil {
			sv.Session.Chunks = org.Parse(content)
		}
		sv.verbose("DATA MODE")
		result = sv.extractData()
	} else if sv.Session.wantsOrg {
		if session == nil {
			sv.Session.Chunks = org.Parse(content)
		}
		sv.verbose("WANTS ORG")
		result = JMap("document", result, "chunks", sv.Session.Chunks)
	} else {
		sv.verbose("DEFAULT")
		result = content
	}
	sv.verbose("RESULT: %v", result)
	return result, nil
}

func (sv *LeisureContext) orgParse() (any, error) {
	if incoming, err := io.ReadAll(sv.r.Body); err != nil {
		return nil, sv.error(ErrCommandFormat, "Could not read document contents")
	} else {
		return org.Parse(string(incoming)), nil
	}
}

func (sv *LeisureContext) stop() {
	os.Exit(0)
}

func (sv *LeisureContext) selectedChunks(edits []doc.Replacement) []org.Chunk {
	ch := make([]org.Chunk, 0, sv.Session.Chunks.Chunks.Measure().Count)
	chSet := make(u.Set[org.OrgId], cap(ch))
	for _, repl := range edits {
		_, right := sv.Session.Chunks.Chunks.Split(func(m org.OrgMeasure) bool {
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

func (sv *LeisureService) FindSession(r *http.Request) *LeisureSession {
	if cookie, err := r.Cookie("session"); err == nil {
		if keyValue := strings.Split(cookie.Value, "="); len(keyValue) == 2 {
			if session := sv.Sessions[keyValue[0]]; session != nil {
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
	} else if session := sv.Sessions[keyValue[0]]; session == nil {
		return sv.error(ErrCommandFormat, "No session for key")
	} else if session.key.Value != cookie.Value {
		return sv.error(ErrCommandFormat, "Bad key for session, expected %s but got %s", session.key.Value, keyValue[1])
	} else if sv.Session == nil {
		return sv.error(ErrCommandFormat, "Internal session error, context does not have session %s\n", session.key.Value)
	}
	sv.Session.lastUsed = time.Now()
	return nil
}

func getstack() []byte {
	r, _ := regexp.Compile("^([^\n]*\n){7}")
	return r.ReplaceAll(debug.Stack(), []byte{})
}

func (sv *LeisureContext) error(errObj any, format string, args ...any) error {
	lerr := AsLeisureError(errObj)
	args = append(append(make([]any, 0, len(args)+1), lerr), args...)
	err := fmt.Errorf("%w:"+format, args...)
	if sv.Session != nil {
		fmt.Fprintf(os.Stderr, "Session error for (%v) %s: %s\n%s", sv.r, sv.Session.SessionId, err.Error(), getstack())
		sv.Logger.Output(2, fmt.Sprintf("Connection %s got error: %s", sv.Session.SessionId, err))
		return err
	}
	fmt.Fprintf(os.Stderr, "Session error (%v) [no session]: %s\n%s", sv.r, err.Error(), debug.Stack())
	debug.PrintStack()
	sv.Logger.Output(2, fmt.Sprintf("got error: %s", err))
	return err
}

// URL: POST /session/edit
// Replace text from replacements in body, commit them, and return the resulting edits
func (sv *LeisureContext) sessionEdit(repls JsonObj) (result any, err error) {
	// set err if there's a panic
	defer func() {
		if errObj := recover(); errObj != nil {
			err = sv.error(errObj, "")
		}
	}()
	if err := sv.checkSession(); err != nil {
		return nil, sv.error(err, "")
	} else if !repls.IsMap() {
		return nil, sv.error(ErrCommandFormat, "expected a replacement array but got %v", repls.V)
	} else if selOffset := repls.GetJson("selectionOffset"); !selOffset.IsNumber() || selOffset.AsInt() < -1 {
		println("BAD SELECTION OFFSET")
		return nil, sv.error(ErrCommandFormat, "expected a selection offset in the replacement but got: %v", repls.V)
	} else if selLength := repls.GetJson("selectionLength"); !selLength.IsNumber() || selLength.AsInt() < -1 {
		println("BAD SELECTION LENGTH")
		return nil, sv.error(ErrCommandFormat, "expected a selection length in the replacement but got: %v", repls.V)
	} else if repls = repls.GetJson("replacements"); !repls.IsArray() && !repls.IsNil() {
		println("BAD REPLACEMENTS")
		return nil, sv.error(ErrCommandFormat, "expected replacement map with a replacement array but got: %v", repls.V)
	} else {
		repl := make([]doc.Replacement, 0, 4)
		l := repls.Len()
		for i := range l {
			el := repls.GetJson(i)
			curRepl, err := sv.replFor(el)
			if err != nil {
				return nil, err
			}
			if curRepl.Length == -1 {
				// a complete replacement destroys earlier edits
				repl = repl[:0]
			}
			repl = append(repl, curRepl)
		}
		if sv.Session.ExclusiveDoc != nil {
			return sv.Session.ExclusiveSessionEdit(repl, selOffset.AsInt(), selLength.AsInt()), nil
		}
		return sv.Session.SessionEdit(repl, selOffset.AsInt(), selLength.AsInt())
	}
}

func chunkNamed(t org.OrgTree, n string) org.Chunk {
	_, r := t.Split(func(m org.OrgMeasure) bool {
		return m.Names.Has(n)
	})
	if r.IsEmpty() {
		return nil
	}
	return r.PeekFirst()
}

func namedChunks(oc *org.OrgChunks) (*org.OrgChunks, org.OrgTree, map[string]org.Chunk, u.Set[string]) {
	t := oc.Chunks
	chunks := map[string]org.Chunk{}
	names := t.Measure().Names
	for name := range names {
		if ch := chunkNamed(t, name); ch != nil {
			chunks[name] = ch
		}
	}
	return oc, t, chunks, names
}

func (s *LeisureSession) SimpleOrgChanges(d1, d2 *doc.Document, changer string, chs *org.ChunkChanges, removed map[org.OrgId]org.Chunk) *org.OrgChunks {
	_, _, chunks0, names0 := namedChunks(s.Chunks)
	result, _, chunks1, names1 := namedChunks(org.Parse(d1.String()))
	add := names1.Complement(names0)
	remove := names0.Complement(names1)
	change := u.NewSet[string]()
	for name := range names0 {
		if names1[name] && chunks0[name].GetText() != chunks1[name].GetText() {
			change.Add(name)
		}
	}
	if d1.GetOps() != d2.GetOps() {
		result2, _, chunks2, names2 := namedChunks(org.Parse(d2.String()))
		add.Merge(change.Complement(names2))
		add.Subtract(names2)
		change.Subtract(names2)
		remove.Subtract(names2)
		chunks1 = chunks2
		result = result2
	}
	popNames := func(chunks map[string]org.Chunk, names u.Set[string], ids u.Set[org.OrgId]) {
		for name := range names {
			ids.Add(chunks[name].AsOrgChunk().Id)
		}
	}
	chs.Added = u.NewSet[org.OrgId]()
	chs.Changed = u.NewSet[org.OrgId]()
	chs.Removed = make([]org.OrgId, 0, len(remove))
	popNames(chunks1, add, chs.Added)
	popNames(chunks1, change, chs.Changed)
	for name := range remove {
		chunk := chunks0[name]
		id := chunk.AsOrgChunk().Id
		chs.Removed = append(chs.Removed, id)
		removed[id] = chunk
	}
	popNames(chunks1, add, chs.Added)
	popNames(chunks1, change, chs.Changed)
	return result
}

// edit session for exclusive editor
// merge together external replacements (like from REDIS) with the editor's replacements
func (s *LeisureSession) ExclusiveSessionEdit(repl []doc.Replacement, selOffset, selLength int) map[string]any {
	s.service.Svc(func() {
		s.HasUpdate = false
	})
	if len(repl) == 0 && s.ExternalBlocks.IsEmpty() {
		return nil
	}
	s.verbose("\n@@@\n@@@ EDIT: %#v\n@@@ OFF: %d\n@@@ LEN: %d\n@@@\n\n", repl, selOffset, selLength)
	d := s.ExclusiveDoc.Copy()
	chs := &org.ChunkChanges{}
	removed := map[org.OrgId]org.Chunk{}
	//removed := u.Set[org.OrgId]{}
	if len(repl) > 0 {
		//fmt.Println("Initial DOCUMENT:")
		//fmt.Println(strings.ReplaceAll(d.String(), "\n", "\n##  "))
		//// apply internal changes first
		//d.Apply("internal", 0, repl)
		//fmt.Println("AFTER INTERNAL CHANGES:")
		////fmt.Println(strings.ReplaceAll(d.String(), "\n", "\n##  "))
		//// apply doc changes to org chunks
		//str := strings.Builder{}
		//org.DumpChunks(&str, "  ", s.Chunks.Chunks)
		//s.orgChanges(repl, &chs, removed)
		////s.Chunks = org.Parse(d.String())
		//if s.Chunks.GetText() != d.String() {
		//	fmt.Fprint(os.Stderr, "@@@\n@@@  ORG TEXT DIFFERS FROM DOC TEXT AFTER INTERNAL CHANGE!!!\n@@@\n")
		//	fmt.Fprint(os.Stderr, "ORIGINAL CHUNKS:\n", str.String())
		//	fmt.Fprint(os.Stderr, "FINAL CHUNKS:\n")
		//	org.DumpChunks(os.Stdout, "  ", s.Chunks.Chunks)
		//}
		d.Apply("internal", 0, repl)
	}
	if selOffset != -1 {
		d.Mark("sel.start", selOffset)
		d.Mark("sel.end", selOffset+selLength)
	}
	d1 := d.Copy()
	s.mergeExternalChanges(d)
	//sadd, schange, sdelete, newOrg := s.SimpleOrgChanges(d)
	newOrg := s.SimpleOrgChanges(d1, d, "internal", chs, removed)
	s.Chunks = newOrg
	fmt.Println("AFTER EXERNAL CHANGES:")
	//fmt.Println(strings.ReplaceAll(d.String(), "\n", "\n##  "))
	org.DumpChunks(os.Stdout, "  ", s.Chunks.Chunks)
	s.ExclusiveDoc = d.Freeze()
	newOff := -1
	newLen := -1
	if selOffset != -1 {
		left, _ := d.SplitOnMarker("sel.start")
		newOff := left.Measure().Width
		left, _ = d.SplitOnMarker("sel.end")
		newEnd := left.Measure().Width
		newLen = newEnd - newOff
	}
	extRepls, _ := d.EditsFor(u.NewSet("external"), nil)
	// need changed chunks here
	s.notifyListeners(chs, removed)
	return s.changes(newOff, newLen, extRepls, false, chs)
}

// func (s *LeisureSession) mergeExternalChanges(d *doc.Document, chs *org.ChunkChanges, removed u.Set[org.OrgId]) {
func (s *LeisureSession) mergeExternalChanges(d *doc.Document) {
	badBlock := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "Error, "+format, args...)
	}
	condensed := make([]map[string]any, 0, 10)
	// condense block commands to only the most recent for each name
	extBlocks := map[string]map[string]any{}
	// note that more recent items occur first in the queue
	for blk := range s.ExternalBlocks.Dequeue() {
		if nameEntry, hasName := blk["name"]; !hasName {
			badBlock("missing block name: %#v", blk)
		} else if name, ok := nameEntry.(string); !ok {
			badBlock("bad block name %v: %#v", blk["type"], blk)
		} else if _, hasType := blk["type"]; !hasType {
			badBlock("missing block type: %#v", blk)
		} else if t, ok := blk["type"].(string); !ok || !MONITOR_BLOCK_TYPES.Has(t) {
			badBlock("bad block type %v: %#v", blk["type"], blk)
		} else if b, has := extBlocks[name]; has && t == b["type"] {
			continue
		} else if t == "delete" {
			// ensure delete block has exactly one values
			bvalue := blk["value"]
			value := ""
			if bvalue == nil {
				value = name
			} else if vstr, isString := bvalue.(string); isString {
				value = vstr
			} else if vstrs, isStrings := bvalue.([]string); isStrings && len(vstrs) == 1 {
				value = vstrs[0]
			} else if varray, isArray := bvalue.([]any); isArray && len(varray) == 1 {
				if vstr, isString = varray[0].(string); isString {
					value = vstr
				} else {
					badBlock("bad block value %v: %#v", varray[0], blk)
					continue
				}
			} else {
				// make separate delete blocks for mulitple values
				for _, name := range u.EnsliceStrings(blk["value"]) {
					copy := make(map[string]any, len(blk))
					maps.Copy(copy, blk)
					copy["value"] = name
					copy["name"] = name
					condensed = append(condensed, copy)
				}
				continue
			}
			// ensure delete block's name is the same as its value
			blk["name"] = value
			condensed = append(condensed, blk)
		} else {
			extBlocks[name] = blk
		}
	}
	for _, blk := range extBlocks {
		condensed = append(condensed, blk)
	}
	extChanges := len(condensed)
	if extChanges == 0 {
		return
	}
	chunks := org.Parse(d.String())
	changes := make([]doc.Replacement, 0, extChanges)
	out := &bytes.Buffer{}
	prefixNl := false
	for _, v := range condensed {
		repl, pfx := s.ComputeInsertRepl(chunks, v, out)
		prefixNl = prefixNl || pfx
		changes = append(changes, repl)
	}
	slices.SortFunc(changes, func(a, b doc.Replacement) int { return b.Offset - a.Offset })
	d.Apply("external", 0, changes)
	////fmt.Println("APPLIED EXTERNAL CHANGES:\n", d.Changes("  "))
	//repls, _ := d.EditsFor(u.NewSet("external"), nil)
	//if s.service.Verbosity > 0 {
	//	fmt.Println("REPLACING:\n REPLS:")
	//	for _, repl := range repls {
	//		t := strings.ReplaceAll(repl.Text, "\n", "\\n")
	//		t = t[:min(len(t), 60)]
	//		fmt.Printf("  %7s [%s]\n", fmt.Sprint(repl.Offset, "-", repl.Offset+repl.Length-1), t)
	//	}
	//	fmt.Print("\n INTO:\n")
	//	org.DisplayChunks("    ", s.Chunks.Chunks)
	//}
	//// apply doc changes to org chunks
	//str := strings.Builder{}
	//org.DumpChunks(&str, "  ", s.Chunks.Chunks)
	//s.orgChanges(repls, chs, removed)
	//if s.Chunks.GetText() != d.String() {
	//	fmt.Fprint(os.Stderr, "@@@\n@@@  ORG TEXT DIFFERS FROM DOC TEXT AFTER EXTERNAL CHANGE!!!\n@@@\n")
	//	fmt.Fprint(os.Stderr, "ORIGINAL CHUNKS:\n", str.String())
	//	fmt.Fprint(os.Stderr, "FINAL CHUNKS:\n")
	//	org.DumpChunks(os.Stdout, "  ", s.Chunks.Chunks)
	//}
}

func (s *LeisureSession) ComputeInsertRepl(chunks *org.OrgChunks, blk map[string]any, out *bytes.Buffer) (repl doc.Replacement, prefixNL bool) {
	prefixNl := false
	name := blk["name"].(string)
	off, ch := chunks.LocateChunkNamed(name)
	if ch.IsEmpty() {
		if blk["type"] == "delete" {
			return doc.Replacement{}, false
		}
		// find a spot in the doc, default to end
		off = chunks.Chunks.Measure().Width
		if !chunks.Chunks.IsEmpty() {
			prefixNl = !strings.HasSuffix(chunks.Chunks.PeekLast().AsOrgChunk().Text, "\n")
		}
		s.verbose("WIDTH: %d, LEN: %d", off, len(chunks.GetText()))
		if blk["type"] != "delete" && blk["tags"] != nil {
		outer:
			for tag := range u.PropStrings(blk["tags"]) {
				// find headline with tag
				if t := chunks.GetChunksTagged(tag); len(t) > 0 {
					for _, hl := range t {
						if nextId := chunks.Next[hl.AsOrgChunk().Id]; nextId == "" {
							continue
						} else if nextHl := chunks.ChunkIds[nextId]; nextHl != nil {
							off, _ = chunks.LocateChunk(nextHl.AsOrgChunk().Id)
							if prevId := chunks.Prev[nextId]; prevId != "" {
								prefixNl = !strings.HasSuffix(chunks.ChunkIds[prevId].GetText(), "\n")
							}
							break outer
						}
					}
				}
			}
		}
	}
	change := doc.Replacement{Offset: off}
	if !ch.IsEmpty() {
		// this is not an insert, it's a replace or a delete
		change.Length = org.Width(ch.Chunk)
	}
	if blk["type"] != "delete" {
		if prefixNl {
			out.WriteRune('\n')
		}
		if err := s.ExternalFmt(out, blk); err != nil {
			fmt.Fprint(os.Stderr, err)
		} else {
			change.Text = out.String()
			fmt.Print("\n\nFORMATTED BLOCK: ", change.Text, "\n\n\n")
		}
		out.Reset()
	}
	return change, prefixNl
}

func (s *LeisureSession) SessionEdit(repl []doc.Replacement, selOffset, selLength int) (result map[string]any, err error) {
	// Using Apply validates the edits, panicking on a repl problem, causing the defer to return an erorr
	blk := s.LatestBlock()
	d := blk.GetDocument(s.History).Freeze()
	d.Apply(s.SessionId, 0, repl)
	d.Simplify()
	repl = append(repl[:0], d.Edits()...)
	// done validating inputs
	s.verbose("edit: %v", repl)
	s.ReplaceAll(repl)
	s.HasUpdate = false
	var trackChunks org.ChunkChanges
	replacements, off, length, err := s.Commit(selOffset, selLength, &trackChunks)
	if err != nil {
		return nil, err
	}
	if s.service.Verbosity > 0 {
		block := s.Latest[s.SessionId]
		hashNums := s.hashNums()
		blocknum := hashNums[block.Hash]
		parents := make([]string, 0, 8)
		for _, v := range block.Parents {
			parents = append(parents, fmt.Sprintf("%d", hashNums[v]))
		}
		repls := make([]string, 0, 8)
		for _, v := range block.Replacements {
			repls = append(repls, fmt.Sprintf("(%d,%d -> '%s')", v.Offset, v.Offset+v.Length-1, v.Text))
		}
		s.verbose("@ BLOCK %d %s (%s): %s", blocknum, block.SessionId, strings.Join(parents, ", "), strings.Join(repls, " "))
	}
	if selOffset > 0 {
		s.verbose("OFFSET: %d -> %d\n", selOffset, off)
	}
	return s.changes(off, length, replacements, len(repl) > 0, &trackChunks), nil
}

func (ss *LeisureSession) changes(selOff, selLen int, replacements []doc.Replacement, update bool, changes *org.ChunkChanges) map[string]any {
	if update {
		for _, s := range ss.service.Sessions {
			if s == ss || s.HasUpdate {
				continue
			}
			s.HasUpdate = true
			if s.Updates != nil {
				// no need to potentially block here
				curSession := s
				go func() { curSession.Updates <- true }()
			}
		}
	}
	result := map[string]any{}
	if !ss.dataMode {
		result["replacements"] = replacements
		result["selectionOffset"] = selOff
		result["selectionLength"] = selLen
	}
	linkCount := len(changes.Linked)
	changeCount := len(changes.Added) + len(changes.Changed) + len(changes.Removed) + linkCount
	if (ss.wantsOrg || ss.dataMode) && (len(replacements) > 0 || changeCount > 0) {
		chunks := ss.Chunks
		chunks.RelinkHierarchy(changes)
		ss.verbose("@@@\nCHUNK CHANGES: %+v\n", changes)
		if changeCount == 0 || (changeCount == linkCount && ss.dataMode) {
			return nil
		} else if ss.dataMode {
			return changes.DataChanges(chunks, ss.wantsOrg)
		} else if changeCount > 0 {
			result["order"] = changes.Order(chunks)
			if !changes.Changed.IsEmpty() {
				result["changed"] = chunkSlice(chunks, changes.Changed)
			}
			if !changes.Added.IsEmpty() {
				result["added"] = chunkSlice(chunks, changes.Added)
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

func (sv *LeisureContext) replFor(repl JsonObj) (curRepl doc.Replacement, err error) {
	if !repl.IsMap() {
		println("NOT MAP")
		err = sv.error(ErrCommandFormat, "expected replacements but got %v", repl)
		return
	}
	offset := repl.GetJson("offset")
	length := repl.GetJson("length")
	block := repl.GetJson("block")
	text := repl.GetJson("text")
	o := 0
	l := 0
	if text.IsNil() {
		text = JsonV("")
	}
	if !(text.IsString() &&
		((offset.IsNil() && block.IsString()) ||
			(offset.IsNumber() && block.IsNil() && length.IsNumber()))) {
		err = sv.error(ErrCommandFormat, "expected replacements but got %v", repl)
		return
	} else if !offset.IsNil() {
		o = offset.AsInt()
		l = length.AsInt()
	} else {
		off, bl := sv.Session.Chunks.LocateChunk(org.OrgId(block.AsString()))
		if bl.IsEmpty() {
			fmt.Printf("COULD NOT LOCATE BLOCK '%s'\n%s", block, org.Dump(sv.Session.Chunks))
			sv.Session.Chunks.Chunks.Dump(os.Stdout, 2)
			err = sv.error(ErrDataMissing, "could not locate block %s", block.String())
			return
		}
		o = off
		l = len(bl.AsOrgChunk().Text)
	}
	if o < 0 || (l < 0 && curRepl.Offset != 0) {
		err = sv.error(ErrCommandFormat, "expected replacements but got %v", repl)
		return
	}
	return doc.Replacement{
		Offset: o,
		Length: l,
		Text:   text.AsString(),
	}, nil
}

func chunkSlice(chunks *org.OrgChunks, ids u.Set[org.OrgId]) []org.ChunkRef {
	result := make([]org.ChunkRef, 0, len(ids))
	for id := range ids {
		result = append(result, chunks.GetChunk(string(id)))
	}
	return result
}

// return whether there is actually new data
// hashes may have changed but new blocks might all be empty
func hasNewData(h *hist.History, parents []Sha) bool {
	latest := h.Heads()
	if !hist.SameHashesNoExclude(latest, parents) {
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

func (ls *LeisureSession) LatestBlock() *hist.OpBlock {
	return ls.History.LatestBlock(ls.SessionId)
}

// URL: GET /session/document -- get the document for the session
func (sv *LeisureContext) sessionDocument() (any, error) {
	if err := sv.checkSession(); err != nil {
		return nil, err
	}
	doc := sv.Session.LatestBlock().GetDocument(sv.Session.History)
	doc.Apply(sv.Session.SessionId, 0, sv.Session.PendingOps)
	//sv.writeSuccess(doc.String())
	return sv.documentResult(doc.String(), false, sv.Session.dataMode, sv.Session.wantsOrg)
}

// URL: GET /session/update -- return whether there is an update
// if there is not yet an update, wait up to UPDATE_TIME for an update before returning
func (sv *LeisureContext) sessionUpdate() {
	ch := make(chan bool)
	Svc(sv.service, func() {
		err := sv.checkSession()
		if err == nil && !sv.Session.HasUpdate {
			timeout := UPDATE_TIME
			if waitTime := sv.r.URL.Query().Get("timeout"); waitTime != "" {
				millis := 0
				_, err = fmt.Sscanf(waitTime, "%d", &millis)
				//fmt.Fprintln(os.Stderr, "PARSED TIMEOUT: ", millis)
				timeout = time.Duration(millis * int(time.Millisecond))
				//fmt.Fprintln(os.Stderr, "ACTUAL TIMEOUT: ", timeout)
			}
			timer := time.After(timeout)
			oldUpdates := sv.Session.Updates
			newUpdates := make(chan bool)
			sv.Session.Updates = newUpdates
			go func() {
				hadUpdate := false
				select {
				case hadUpdate = <-newUpdates:
				case <-timer:
				}
				sv.jsonResponseSvc(func() (any, error) {
					if err != nil {
						return nil, sv.error(err, "")
					}
					if oldUpdates != nil {
						go func() { oldUpdates <- hadUpdate }()
					}
					if sv.Session.Updates == newUpdates {
						sv.Session.Updates = nil
						sv.Session.HasUpdate = false
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
				if sv.Session.Updates != nil {
					sv.Session.Updates <- true
				}
				return true, nil
			})
			ch <- true
		}
	})
	<-ch
}

// URL: GET /session/get/NAME
// get data for NAME
// only allowed for datamode
func (sv *LeisureContext) sessionGet() (result any, err error) {
	// set err if there's a panic
	defer func() {
		if errObj := recover(); errObj != nil {
			err = sv.error(errObj, "")
		}
	}()
	if err := sv.checkSession(); err != nil {
		return nil, sv.error(err, "")
	} else if !sv.Session.dataMode && !sv.Session.wantsOrg {
		return nil, sv.error(ErrCommandFormat, "Not in datamode or orgmode")
	} else if name := path.Base(sv.r.URL.Path); name == "" {
		return nil, sv.error(ErrCommandFormat, "Bad get command, no name")
	} else if data := sv.Session.Chunks.GetChunkNamed(name); data.IsEmpty() {
		return nil, sv.error(ErrDataMissing, "No data named %s", name)
	} else {
		name, dt := extractData(data.Chunk, sv.Session.wantsOrg)
		if name != "" {
			return dt, nil
		}
		return nil, sv.error(ErrDataMismatch, "%s is not a data block", name)
	}
}

// URL: POST /session/set/NAME
func (sv *LeisureContext) sessionSet(arg JsonObj) (result any, err error) {
	force := strings.ToLower(sv.r.URL.Query().Get("force")) == "true"
	// set err if there's a panic
	defer func() {
		if errObj := recover(); errObj != nil {
			err = sv.error(errObj, "")
		}
	}()
	if err := sv.checkSession(); err != nil {
		return nil, sv.error(err, "")
	} else if sv.r.URL.Path == SESSION_SET {
		return sv.sessionSetMultiple(arg)
	} else {
		name := path.Base(sv.r.URL.Path)
		//fmt.Printf("NAME: %s\n", name)
		offset, data := sv.Session.Chunks.LocateChunkNamed(name)
		return sv.addOrSet(name, arg, force, true, offset, data)
	}
}

func (sv *LeisureContext) addOrSet(name string, value JsonObj, force, commit bool, offset int, data org.ChunkRef) (result any, err error) {
	if data.IsEmpty() {
		if !force {
			docstr := sv.Session.LatestBlock().GetDocument(sv.Session.History).String()
			parsed := org.Parse(docstr)
			_, r := parsed.LocateChunkNamed(name)
			println("CHUNKS")
			org.DisplayChunks("  ", parsed.Chunks)
			return nil, sv.error(ErrDataMissing, "No data named %s\nREPARSED CHUNK: %#v\nDOCUMENT:%s", name, r, docstr)
		}
		sv.AddData(name, value, commit)
		return true, nil
	} else if src, ok := data.Chunk.(*org.SourceBlock); ok {
		if src.IsData() {
			return sv.SetData(offset, src.Content, src.End, src, value, commit)
		}
		return nil, sv.error(ErrDataMismatch, "%s is not a data block", name)
	} else if tbl, ok := data.Chunk.(*org.TableBlock); ok {
		return sv.SetData(offset, tbl.TblStart, len(tbl.Text), tbl, value, commit)
	}
	return nil, sv.error(ErrUnknown, "Unknown error")
}

// URL: POST /session/set
func (sv *LeisureContext) sessionSetMultiple(arg JsonObj) (result any, err error) {
	force := strings.ToLower(sv.r.URL.Query().Get("force")) == "true"
	if m, ok := arg.V.(map[string]any); !ok {
		return nil, sv.error(ErrCommandFormat, "Expected an object with name -> data but got %v", arg)
	} else {
		// reverse-sort changes so they can be applied properly
		// because addOrSet with no commit only records the replacement
		// it does not change the document
		changes := make([]DataChange, len(m))
		pos := 0
		for name, value := range m {
			location, data := sv.Session.Chunks.LocateChunkNamed(name)
			if data.IsEmpty() && !force {
				org.DisplayChunks("  ", sv.Session.Chunks.Chunks)
				return nil, sv.error(ErrDataMissing, "No data named '%s'", name)
			}
			changes[pos] = DataChange{name, JsonV(value), data, location}
		}
		slices.SortFunc(changes, func(a, b DataChange) int { return b.location - a.location })
		for _, change := range changes {
			sv.addOrSet(change.name, change.value, true, false, change.location, change.data)
		}
		var trackChanges org.ChunkChanges
		replacements, newSelOff, newSelLen, err := sv.Session.Commit(-1, -1, &trackChanges)
		if err != nil {
			return nil, err
		}
		return sv.Session.changes(newSelOff, newSelLen, replacements, true, &trackChanges), nil
	}
}

func (sv *LeisureContext) AddData(name string, value any, commit bool) (any, error) {
	str := sv.Session.LatestBlock().GetDocument(sv.Session.History).String()
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
	return sv.ReplaceText(-1, -1, len(str), 0, sb.String(), commit)
}

func (sv *LeisureContext) ReplaceText(selOff, selLen, offset, length int, text string, commit bool) (map[string]any, error) {
	sv.Session.Replace(offset, length, text)
	if !commit {
		return nil, nil
	}
	sv.Session.HasUpdate = false
	var trackChanges org.ChunkChanges
	replacements, newSelOff, newSelLen, err := sv.Session.Commit(selOff, selLen, &trackChanges)
	if err != nil {
		return nil, err
	}
	return sv.Session.changes(newSelOff, newSelLen, replacements, true, &trackChanges), nil
}

func (sv *LeisureContext) SetData(offset, start, end int, ch org.DataBlock, value JsonObj, commit bool) (any, error) {
	if newText, err := ch.SetValue(value.V); err != nil {
		return nil, err
	} else {
		sv.verbose("NEW DATA:", newText)
		newEnd := end + len(newText) - len(ch.AsOrgChunk().Text)
		sv.verbose("WANTS ORG:", sv.Session.wantsOrg)
		result, err := sv.ReplaceText(-1, -1, offset+start, end-start, newText[start:newEnd], commit)
		if err == nil && sv.Session.wantsOrg && commit {
			//fmt.Println("RETURNING JSON")
			obj := JMap("replacements", result, "chunk", sv.Session.Chunks.GetChunkAt(offset+start))
			if data, err := json.Marshal(obj); err != nil {
				sv.verboseN(1, "Writing error %s", err)
				if sv.Verbosity > 0 {
					debug.PrintStack()
				}
				//fmt.Println("RETURNING JSON: ", data)
				return data, nil
			} else {
				return nil, err
			}
		}
		return result, err
	}
}

// URL: POST /session/tag/NAME
func (sv *LeisureContext) sessionTag() (result any, err error) {
	// set err if there's a panic
	defer func() {
		if errObj := recover(); errObj != nil {
			err = sv.error(errObj, "")
		}
	}()
	if err := sv.checkSession(); err != nil {
		return nil, sv.error(err, "")
	} else if !(sv.Session.wantsOrg || sv.Session.dataMode) {
		return nil, sv.error(ErrSessionType, "Only org or data sessions can find tags")
	} else if sv.r.URL.Path == SESSION_TAG {
		return nil, sv.error(ErrCommandFormat, "Tag search requires a name")
	}
	name := path.Base(sv.r.URL.Path)
	//fmt.Printf("NAME: %s\n", name)
	return sv.Session.Chunks.GetChunksTagged(name), nil
}

// URL: POST /session/add/NAME
func (sv *LeisureContext) sessionAdd(value JsonObj) (result any, err error) {
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
	} else if _, data := sv.Session.Chunks.LocateChunkNamed(name); !data.IsEmpty() {
		return nil, sv.error(ErrDataMismatch, "Already data named %s", name)
	} else {
		return sv.AddData(name, value, true)
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
	} else if offset, data := sv.Session.Chunks.LocateChunkNamed(name); data.IsEmpty() {
		return nil, sv.error(ErrDataMismatch, "No data named %s", name)
	} else {
		return sv.ReplaceText(-1, -1, offset, len(data.Chunk.AsOrgChunk().Text), "", true)
	}
}

func (sv *LeisureContext) jsonResponseSvc(fn func() (any, error)) {
	sv.SvcSync(func() {
		sv.jsonResponse(fn)
	})
}

func (sv *LeisureContext) jsonSvc(fn func(JsonObj) (any, error)) {
	sv.jsonResponseSvc(func() (any, error) {
		if body, err := io.ReadAll(sv.r.Body); err != nil {
			return nil, sv.error(ErrCommandFormat, "expected a json body")
		} else {
			var msg any
			if len(body) > 0 {
				if err := json.Unmarshal(body, &msg); err != nil {
					return nil, sv.error(ErrCommandFormat, "expected a json body but got %v", string(body))
				}
			}
			return fn(JsonV(msg))
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
	sessionId := "no session"
	if sv.Session != nil {
		sessionId = sv.Session.SessionId
	}
	sv.verboseN(2, "Got %s request[%s]: %s", sv.r.Method, sessionId, sv.r.URL)
	if obj, err := sv.safeCall(fn); err != nil {
		sv.writeRawError(err)
	} else {
		sv.verbose("RETURNING RESULT, %s -> %#v\n", sv.r.URL, obj)
		sv.writeResponse(obj)
	}
}

// SvcSync protects WebService's internals
func (sv *LeisureService) SvcSync(fn func()) {
	// the callers are each in their own goroutine so sync is fine here
	SvcSync(sv.service, func() (bool, error) {
		fn()
		return true, nil
	})
}

func (sv *LeisureService) Svc(fn func()) {
	Svc(sv.service, fn)
}

func (sv *LeisureService) shutdown() {
	close(sv.service)
}

func MemoryStorage(id, content string) hist.DocStorage {
	return hist.NewMemoryStorage(content)
}

func (sv *LeisureService) handleFunc(mux *http.ServeMux, url string, fn func(http.ResponseWriter, *http.Request)) {
	mux.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		if sv.Verbosity > 0 {
			fmt.Fprintln(os.Stderr, "REQUEST: ", r.URL)
		}
		fn(w, r)
	})
}

func (sv *LeisureService) handleJson(mux *http.ServeMux, url string, fn func(*LeisureContext, JsonObj) (any, error)) {
	sv.handleFunc(mux, url, func(w http.ResponseWriter, r *http.Request) {
		ct := &LeisureContext{sv, w, r, sv.FindSession(r)}
		ct.jsonSvc(func(arg JsonObj) (any, error) { return fn(ct, arg) })
	})
}

func (sv *LeisureService) handleJsonResponse(mux *http.ServeMux, url string, fn func(ct *LeisureContext) (any, error)) {
	sv.handleFunc(mux, url, func(w http.ResponseWriter, r *http.Request) {
		ct := &LeisureContext{sv, w, r, sv.FindSession(r)}
		ct.jsonResponse(func() (any, error) { return fn(ct) })
	})
}

func (sv *LeisureService) handleSync(mux *http.ServeMux, url string, fn func(*LeisureContext)) {
	sv.handleFunc(mux, url, func(w http.ResponseWriter, r *http.Request) {
		ct := &LeisureContext{sv, w, r, sv.FindSession(r)}
		ct.SvcSync(func() { fn(ct) })
	})
}

func (sv *LeisureService) handle(mux *http.ServeMux, url string, fn func(*LeisureContext)) {
	sv.handleFunc(mux, url, func(w http.ResponseWriter, r *http.Request) {
		fn(&LeisureContext{sv, w, r, sv.FindSession(r)})
	})
}

func (s *LeisureContext) verbose(format string, args ...any) {
	if s.Session != nil {
		s.Session.verbose(format, args...)
	} else {
		s.LeisureService.verbose(format, args...)
	}
}

func (s *LeisureSession) verbose(format string, args ...any) {
	a := make([]any, 0, len(args)+1)
	a = append(a, s.SessionId)
	a = append(a, args...)
	s.service.verbose("[%s]: "+format, a...)
}

func (sv *LeisureService) verbose(format string, args ...any) {
	sv.verboseN(1, format, args...)
}

func (sv *LeisureService) verboseN(n int, format string, args ...any) {
	if sv.Verbosity >= n {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

func (sv *LeisureService) SetVerbose(n int) {
	sv.Verbosity = n
	org.SetVerbosity(n)
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
	sv.handleJsonResponse(mux, SESSION_DOCUMENT, (*LeisureContext).sessionDocument)
	sv.handle(mux, SESSION_UPDATE, (*LeisureContext).sessionUpdate)
	sv.handleJsonResponse(mux, SESSION_GET, (*LeisureContext).sessionGet)
	sv.handleJson(mux, SESSION_SET, (*LeisureContext).sessionSet)
	sv.handleJsonResponse(mux, SESSION_TAG, (*LeisureContext).sessionTag)
	sv.handleJsonResponse(mux, ORG_PARSE, (*LeisureContext).orgParse)
	sv.handle(mux, STOP, (*LeisureContext).stop)
	RunSvc(sv.service)
	return sv
}
