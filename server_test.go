package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"testing"

	doc "github.com/leisure-tools/document"
	"github.com/leisure-tools/history"
)

type MockResponseWriter struct {
	buf    *bytes.Buffer
	header http.Header
	status int
}

///
/// Mock HTTP
///

func (w *MockResponseWriter) Header() http.Header {
	return w.header
}

func (w *MockResponseWriter) Write(bytes []byte) (int, error) {
	return w.buf.Write(bytes)
}

func (w *MockResponseWriter) WriteHeader(statusCode int) {
	w.status = statusCode
}

func (w *MockResponseWriter) createResponse() *bufio.Reader {
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("HTTP/1.1 %d\n", w.status))
	w.header.Write(buf)
	buf.WriteString("\n")
	buf.Write(w.buf.Bytes())
	return bufio.NewReader(buf)
}

func newMockResponseWriter() *MockResponseWriter {
	return &MockResponseWriter{
		buf:    bytes.NewBuffer(make([]byte, 0, 1024)),
		header: http.Header{},
	}
}

type myT struct {
	*testing.T
}

type errorResult1[V any] struct {
	value V
	err   error
}

func r1[V any](value V, err error) errorResult1[V] {
	return errorResult1[V]{value: value, err: err}
}

func (h errorResult1[V]) check(t fallible) V {
	if h.err != nil {
		die(t, h.err)
	}
	return h.value
}

type fallible interface {
	Error(args ...any)
	FailNow()
}

func (sv *testServer) die(args ...any) {
	die(sv.t, args...)
}

func die(t fallible, args ...any) {
	fmt.Fprintln(os.Stderr, args...)
	t.Error(args...)
	debug.PrintStack()
	t.FailNow()
}

type lazyString func() string

func lazy(f func() string) lazyString {
	return lazyString(f)
}

func (l lazyString) String() string {
	return l()
}

func (j jsonObj) equals(obj jsonObj) bool {
	if j.v == nil || obj.v == nil {
		return j.v == nil && obj.v == nil
	}
	if j.typeof() == obj.typeof() {
		switch j.typeof() {
		case "array":
			if j.len() == obj.len() {
				for i := 0; i < j.len(); i++ {
					if !j.getJson(i).equals(obj.getJson(i)) {
						return false
					}
				}
				return true
			}
		case "object":
			if j.len() == obj.len() {
				for _, key := range j.keys() {
					if !j.getJson(key).equals(obj.getJson(key)) {
						return false
					}
				}
				return true
			}
		case "number":
			if j.value().IsZero() || obj.value().IsZero() {
				return j.value().IsZero() == obj.value().IsZero()
			} else if j.isInt() && obj.isInt() {
				return j.asInt64() == obj.asInt64()
			}
			return j.asFloat64() == obj.asFloat64()
		case "string":
			return j.asString() == obj.asString()
		case "boolean":
			return j.value().Bool() == obj.value().Bool()
		default:
			return j.v == obj.v
		}
	}
	return false
}

func (t myT) jsonV(value any) jsonObj {
	var result any
	t.failNowIfErr(json.Unmarshal([]byte(r1(json.Marshal(value)).check(t)), &result))
	return jsonV(result)
}

func (t myT) testEqual(actual any, expected any, msg any) {
	//if !t.jsonV(actual).equals(t.jsonV(expected)) {
	if !jsonV(actual).equals(jsonV(expected)) {
		die(t, fmt.Sprintf("%s: expected\n <%v> but got\n <%v>", msg, expected, actual))
	}
}

func (t myT) failNowIfNot(cond bool, msg any) {
	if !cond {
		die(t, msg)
	}
}

func (t myT) failNowIfErr(err any) {
	if err != nil {
		die(t, err)
	}
}

type testServer struct {
	t          myT
	mux        *http.ServeMux
	service    *LeisureService
	blockNames map[Sha]string
	history    *history.History
}

type testSession struct {
	myT
	*testServer
	sessionId string
	cookie    *http.Cookie
	*history.History
}

func (sv *testServer) newSession(name, doc, expected string) *testSession {
	s := &testSession{
		myT:        sv.t,
		testServer: sv,
		sessionId:  name,
	}
	cookies, body := sv.processGet(SESSION_CREATE, name, doc)
	s.myT.testEqual(body, expected, "bad response for create")
	s.cookie = cookies[0]
	s.History = sv.service.sessions[name].History
	return s
}

func (s *testSession) edit(edit map[string]any, expected map[string]any, msg string) {
	s.testPost(s.testServer.jsonDecode, SESSION_EDIT, s.cookie, s.sessionId,
		s.jsonEncode(edit),
		expected,
		msg)
}

func (s *testSession) testGet(args ...any) {
	a := append(make([]any, 0, len(args)+1), args[0], s.cookie)
	a = append(a, args[1:])
	s.testServer.testGet(args...)
}

func (sv *testServer) Error(args ...any) {
	sv.t.Error(args...)
}
func (sv *testServer) FailNow() {
	sv.t.FailNow()
}

func emptyBody() *bytes.Buffer {
	return bytes.NewBuffer([]byte{})
}

func (sv *testServer) route(mux *http.ServeMux, method, url string, cookie *http.Cookie, body string) (*http.Request, *MockResponseWriter) {
	buf := emptyBody()
	if body != "" {
		buf = bytes.NewBuffer([]byte(body))
	}
	req := r1(http.NewRequest(method, url, buf)).check(sv)
	if cookie != nil {
		req.AddCookie(cookie)
	}
	resp := newMockResponseWriter()
	mux.ServeHTTP(resp, req)
	return req, resp
}

func urlFor(url []any) string {
	strs := make([]string, 0, 4)
	query := make([]string, 0, 4)
	inQuery := false
	key := ""
	for _, u := range url {
		if u == "?" {
			inQuery = true
			continue
		} else if _, ok := u.(*http.Cookie); ok {
			break
		} else if inQuery && key == "" {
			key = fmt.Sprint(u)
		} else if inQuery {
			query = append(query, fmt.Sprintf("%s=%s", key, u))
			key = ""
		} else {
			strs = append(strs, fmt.Sprint(u))
		}
	}
	result := path.Clean(path.Join(strs...))
	if inQuery {
		result = result + "?" + strings.Join(query, "&")
	}
	return result
}

func cookieFor(url []any) *http.Cookie {
	for _, u := range url {
		if cookie, ok := u.(*http.Cookie); ok {
			return cookie
		}
	}
	return nil
}

func (sv *testServer) get(url ...any) (*http.Request, *MockResponseWriter) {
	req, resp := sv.route(sv.mux, http.MethodGet, urlFor(url), cookieFor(url), "")
	sv.t.testEqual(resp.status, http.StatusOK, lazy(func() string {
		return fmt.Sprintf("Unsuccessful request: %s", resp.buf.String())
	}))
	return req, resp
}

func (sv *testServer) post(url ...any) (*http.Request, *MockResponseWriter) {
	body := url[len(url)-1]
	url = url[:len(url)-1]
	req, resp := sv.route(sv.mux, http.MethodPost, urlFor(url), cookieFor(url), fmt.Sprint(body))
	sv.t.testEqual(resp.status, http.StatusOK, lazy(func() string {
		return fmt.Sprintf("Unsuccessful request: %s", resp.buf.String())
	}))
	return req, resp
}

func (sv *testServer) shutdown() {
	sv.service.shutdown()
}

func (t myT) createServer(name string) *testServer {
	mux := http.NewServeMux()
	ws := Initialize(name, mux, MemoryStorage)
	return &testServer{
		t:          t,
		mux:        mux,
		service:    ws,
		blockNames: map[Sha]string{},
	}
}

func TestCreate(tt *testing.T) {
	t := myT{tt}
	sv := t.createServer("test")
	_, resp := sv.post(DOC_CREATE, UUID, "?", "alias", "fred", "bob content")
	_, resp = sv.get(DOC_LIST)
	t.testEqual(resp.buf.String(), "[[\""+UUID+"\",\"fred\"]]", "Bad document list")
	_, resp = sv.get(SESSION_CREATE, "s1", "fred")
	_, resp = sv.get(SESSION_LIST)
	session := sv.service.sessions["s1"]
	l := session.History.Latest[session.SessionId]
	if l == nil {
		l = session.History.Source
	}
	t.testEqual(l.GetDocument(session.History).String(), "bob content", "Documents are not the same")
	sv.shutdown()
}

const UUID = "0e21af6b-edf7-4d1a-b6e0-439d878424b0"

const doc1 = `line one
line two
line three`

const doc1Edited = `line ONE
line two
line three
line four`

const doc1Merged = `line ONE
line TWO
line three
line four
line five`

func (w *MockResponseWriter) jsonDecode(t myT) jsonObj {
	return t.jsonDecode(w.buf.Bytes())
}

func (t myT) jsonDecode(bytes []byte) jsonObj {
	var obj any
	if err := json.Unmarshal(bytes, &obj); err != nil {
		die(t, err)
	}
	return jsonV(obj)
}

func (t myT) jsonEncode(obj any) string {
	data, err := json.Marshal(obj)
	if err != nil {
		die(t, err)
	}
	return string(data)
}

func replacements(repl []doc.Replacement) []map[string]any {
	result := make([]map[string]any, len(repl))
	for i, r := range repl {
		result[i] = replacement(r)
	}
	return result
}

func replacement(repl doc.Replacement) map[string]any {
	return map[string]any(jmap("offset", repl.Offset, "length", repl.Length, "text", repl.Text))
}

func index(str string, line, col int) int {
	i := 0
	for line > 0 {
		index := strings.IndexByte(str, '\n') + 1
		i += index
		str = str[index:]
		line--
	}
	return i + col
}

func (sv *testServer) processGet(url ...any) ([]*http.Cookie, string) {
	req, resp := sv.get(url...)
	body := resp.buf.String()
	buf := &bytes.Buffer{}
	resp.header.Write(buf)
	resp.WriteHeader(resp.status)
	if hresp, err := http.ReadResponse(resp.createResponse(), req); err != nil {
		sv.die(err)
		return nil, ""
	} else {
		return hresp.Cookies(), body
	}
}

func TestEdits(tt *testing.T) {
	t := myT{tt}
	sv := t.createServer("test")
	sv.post(DOC_CREATE, UUID, "?", "alias", "fred", doc1)
	c, _ := sv.processGet(SESSION_CREATE, "emacs", "fred")
	t.testEqual(len(c), 1, "Bad number of cookies")
	cookie := c[0]
	keyValue := strings.Split(cookie.Value, "=")
	t.testEqual(len(keyValue), 2, fmt.Sprintf("Bad cookie format, expected SESSION_ID=SESSION_KEY but got %s", cookie.Value))
	_, resp := sv.get(SESSION_DOCUMENT, cookie)
	t.testEqual(resp.buf.String(), doc1, "Bad document")
	d1 := doc.NewDocument(doc1)
	d1.Replace("emacs", 0, index(d1.String(), 0, 5), 3, "ONE")
	d1.Replace("emacs", 3, index(d1.String(), 2, 10), 0, "\nline four")
	_, resp = sv.post(SESSION_EDIT, cookie, t.jsonEncode(jmap(
		"replacements", replacements(d1.Edits()),
		"selectionOffset", 0,
		"selectionLength", 0,
	)))
	_, resp = sv.get(SESSION_DOCUMENT, cookie)
	t.testEqual(resp.buf.String(), doc1Edited, "bad document")
	sv.shutdown()
}

func responseString(w *MockResponseWriter) any {
	return w.buf.String()
}

func (sv *testServer) jsonDecode(w *MockResponseWriter) any {
	return w.jsonDecode(sv.t).v
}

func testFetchArgs(count int, args []any, formatErr string) (func(*MockResponseWriter) any, []any, any, any) {
	if len(args) < count {
		panic(formatErr)
	}
	expected := args[len(args)-2]
	errMsg := args[len(args)-1]
	args = args[:len(args)-2]
	transform := responseString
	if f, ok := args[0].(func(*MockResponseWriter) any); ok {
		transform = f
		args = args[1:]
	}
	return transform, args, expected, errMsg
}

// testGet([transform-func], URL, args..., expected, errorMessage)
func (sv *testServer) testGet(args ...any) {
	transform, args, expected, errMsg := testFetchArgs(3, args, "bad use of testGet")
	_, resp := sv.get(args...)
	sv.t.testEqual(transform(resp), expected, errMsg)
}

// testPost([transform-func], URL, args..., data, expected, errorMessage)
func (sv *testServer) testPost(args ...any) {
	transform, args, expected, errMsg := testFetchArgs(4, args, "bad use of testPost")
	_, resp := sv.post(args...)
	sv.t.testEqual(transform(resp), expected, errMsg)
}

func (t myT) repls(args ...any) []doc.Replacement {
	replacements := make([]doc.Replacement, 0, len(args)/3)
	for i := 0; i+2 < len(args); i += 3 {
		off, ok := args[i].(int)
		t.failNowIfNot(ok, "bad cast")
		length, ok := args[i+1].(int)
		t.failNowIfNot(ok, "bad cast")
		text, ok := args[i+2].(string)
		t.failNowIfNot(ok, "bad cast")
		replacements = append(replacements, doc.Replacement{
			Offset: off,
			Length: length,
			Text:   text,
		})
	}
	return replacements
}

func (t myT) edit(selOff, selLen int, repls ...any) map[string]any {
	//fmt.Fprintf(os.Stderr, "Replacements: %v\n", t.jsonEncode(jmap(
	//	"selectionOffset", selOff,
	//	"selectionLength", selLen,
	//	"replacements", t.repls(repls...))))
	return jmap(
		"selectionOffset", selOff,
		"selectionLength", selLen,
		"replacements", t.repls(repls...))
}

func TestTwoPeers(tt *testing.T) {
	t := myT{tt}
	sv := t.createServer("test")
	sv.post(DOC_CREATE, "0000", "?", "alias", "bubba", "hello there")
	cookies, body := sv.processGet(SESSION_CREATE, "emacs", "bubba")
	t.testEqual(body, "hello there", "bad response for create")
	emacs := cookies[0]
	cookies, body = sv.processGet(SESSION_CREATE, "vscode", "bubba")
	t.testEqual(body, "hello there", "bad response for create")
	vscode := cookies[0]
	sv.post(SESSION_EDIT, vscode, t.jsonEncode(
		t.edit(0, 0,
			6, 5, "goodbye")))
	sv.testGet(SESSION_DOCUMENT, vscode, "hello goodbye", "Unexpected document")
	sv.testGet(sv.jsonDecode, SESSION_UPDATE, emacs, true, "Expected update")
	sv.testGet(SESSION_DOCUMENT, emacs, "hello there", "Unexpected document")
	session := sv.service.sessions["emacs"]
	fmt.Printf("EMACS: %v\n", session.SessionId)
	fmt.Printf("EMACS-lastest: %v\n", session.History.Latest[session.SessionId])
	jrepl := jsonV(&doc.Replacement{Offset: 1, Length: 2, Text: "three"})
	expectedRepl := jmap("offset", 1, "length", 2, "text", "three")
	t.testEqual(expectedRepl, jrepl, "Bad comparison")
	sv.testPost(sv.jsonDecode, SESSION_EDIT, emacs,
		t.jsonEncode(t.edit(0, 0)),
		t.edit(0, 0, 6, 5, "goodbye"),
		"Bad replacement")
	sv.testGet(SESSION_DOCUMENT, emacs, "hello goodbye", "Unexpected document")
	sv.shutdown()
}

func TestPeerEdits(tt *testing.T) {
	lines := 2
	word := "hello "
	words := 2
	// these each produce a different error
	//inserts := []int{17, 15, 23}
	inserts := []int{1, 1, 1}
	t := myT{tt}
	sv := t.createServer("test")
	docBuf := strings.Builder{}
	for line := 0; line < lines; line++ {
		for wordNum := 0; wordNum < words; wordNum++ {
			docBuf.WriteString(word)
		}
		docBuf.WriteString("\n")
	}
	sv.post(DOC_CREATE, "0000", "?", "alias", "bubba", docBuf.String())
	sv.history = sv.service.documents["0000"]
	sv.addBlock(sv.history.Source)
	cookies, _ := sv.processGet(SESSION_CREATE, "emacs", "bubba")
	emacs := cookies[0]
	cookies, _ = sv.processGet(SESSION_CREATE, "vscode", "bubba")
	vscode := cookies[0]
	for _, offset := range inserts {
		sv.change("emacs", "vscode", emacs, vscode, offset, 0, "a")
	}
	sv.shutdown()
}

func (sv *testServer) addBlock(blk *history.OpBlock) {
	if sv.blockNames[blk.Hash] != "" {
		return
	}
	n := len(sv.blockNames)
	sb := strings.Builder{}
	for {
		i := n % 52
		c := 'a' + i
		if i >= 26 {
			c = 'A' + (i - 26)
		}
		fmt.Fprintf(&sb, "%c", c)
		n = n / 52
		if n == 0 {
			break
		}
	}
	sv.blockNames[blk.Hash] = sb.String()
}

func (sv *testServer) printBlockOrder() {
	sb := strings.Builder{}
	for _, hash := range sv.history.BlockOrder {
		sb.WriteString(" " + sv.blockNames[hash])
	}
	fmt.Printf("block order:%s\n", sb.String())
}

func (sv *testServer) session(name string) *LeisureSession {
	return sv.service.sessions[name]
}

func (sv *testServer) latest(sessionName string) *history.OpBlock {
	return sv.session(sessionName).latestBlock()
}

func (sv *testServer) block(blk *history.OpBlock) string {
	return sv.blockNames[blk.Hash]
}

func (sv *testServer) printBlock(blk *history.OpBlock) {
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "Block: %s[%s]:", sv.block(blk), blk.SessionId)
	for _, p := range blk.Parents {
		parent := sv.history.GetBlock(p)
		fmt.Fprintf(sb, " %s[%s]", sv.blockNames[p], parent.SessionId)
	}
	fmt.Println(sb.String())
}

func (sv *testServer) change(s1, s2 string, cookie1, cookie2 *http.Cookie, offset, length int, text string) {
	fmt.Printf("replace %d %d %s\n", offset, length, text)
	h := sv.service.sessions[s1].History
	doc1 := sv.latest(s1).GetDocument(h)
	doc2 := sv.latest(s2).GetDocument(h)
	sv.testPost(sv.jsonDecode, SESSION_EDIT, cookie1, sv.t.jsonEncode(
		sv.t.edit(0, 0,
			offset, length, text)),
		sv.t.edit(0, 0),
		"bad edit result",
	)
	sv.addBlock(sv.latest(s1))
	sv.printBlock(sv.latest(s1))
	sv.printBlockOrder()
	newDoc1 := sv.service.sessions[s1].latestBlock().GetDocument(h)
	sv.t.failNowIfNot(doc1.String() != newDoc1.String(), "document is unchanged")
	sv.t.failNowIfNot(len(doc1.String())+len(text) == len(newDoc1.String()), "new document size is wrong")
	sv.testPost(sv.jsonDecode, SESSION_EDIT, cookie2, sv.t.jsonEncode(sv.t.edit(0, 0)),
		sv.t.edit(0, 0, offset, length, text),
		"bad edit result",
	)
	sv.addBlock(sv.latest(s2))
	sv.printBlock(sv.latest(s2))
	sv.printBlockOrder()
	newDoc2 := sv.service.sessions[s2].latestBlock().GetDocument(h)
	sv.t.failNowIfNot(doc2.String() != newDoc2.String(), "document is unchanged")
	sv.t.failNowIfNot(len(doc2.String())+len(text) == len(newDoc2.String()), "new document size is wrong")
	sv.t.failNowIfNot(newDoc1.String() == newDoc2.String(), "documents are not equal")
}

func TestOrgEdit(tt *testing.T) {
	t := myT{tt}
	sv := t.createServer("test")
	doc := `#+begin_src maluba
frood
#+end_src
`
	sv.post(DOC_CREATE, "0000", "?", "alias", "bubba", doc)
	sv.history = sv.service.documents["0000"]
	sv.addBlock(sv.history.Source)
	cookies, _ := sv.processGet(SESSION_CREATE, "emacs", "bubba")
	emacs := cookies[0]
	cookies, _ = sv.processGet(SESSION_CREATE, "vscode", "bubba", "?", "org", "true")
	vscode := cookies[0]
	sv.testPost(sv.jsonDecode, SESSION_EDIT, emacs, sv.t.jsonEncode(
		sv.t.edit(0, 0, 12, 6, "julia")),
		sv.t.edit(0, 0),
		"bad edit result",
	)
	expected := sv.t.edit(0, 0, 12, 6, "julia")
	changedBlock := jmap(
		"content", 18,
		"end", 24,
		"id", "chunk-1",
		"label", 12,
		"labelEnd", 17,
		"text", doc[:12]+"julia"+doc[18:],
		"type", "source",
	)
	expected["order"] = []string{"chunk-1"}
	expected["changed"] = []any{changedBlock}
	sv.testPost(sv.jsonDecode, SESSION_EDIT, vscode,
		t.jsonEncode(t.edit(0, 0)),
		t.jsonDecode([]byte(t.jsonEncode(expected))).v,
		"bad edit result",
	)
	sv.shutdown()
}

func TestTwoSessions(tt *testing.T) {
	t := myT{tt}
	sv := t.createServer("test")
	sv.post(DOC_CREATE, "0000", "?", "alias", "bubba", "hello there")
	emacs := sv.newSession("emacs", "bubba", "hello there")
	browser := sv.newSession("browser", "bubba", "hello there")
	emacs.edit(t.edit(0, 0, 0, 5, "goodbye"), t.edit(0, 0), "bad edit response")
	browser.edit(t.edit(0, 0), t.edit(0, 0, 0, 5, "goodbye"), "bad edit response")
	emacs.edit(t.edit(0, 0, 8, 1, ""), t.edit(0, 0), "bad edit response")
	browser.edit(t.edit(0, 0), t.edit(0, 0, 8, 1, ""), "bad edit response")
	t.testEqual(emacs.GetLatestDocument().String(), "goodbye here", "bad latest document")
	emacs.edit(t.edit(0, 0, 8, 0, "t"), t.edit(0, 0), "bad edit response")
	browser.edit(t.edit(0, 0), t.edit(0, 0, 8, 0, "t"), "bad edit response")
	emacs.edit(t.edit(0, 0, 8, 0, "asdf"), t.edit(0, 0), "bad edit response")
	browser.edit(t.edit(0, 0), t.edit(0, 0, 8, 0, "asdf"), "bad edit response")
	emacs.edit(t.edit(0, 0, 12, 0, "qwer"), t.edit(0, 0), "bad edit response")
	browser.edit(t.edit(0, 0), t.edit(0, 0, 12, 0, "qwer"), "bad edit response")
	emacs.edit(t.edit(0, 0, 16, 0, "zxcv"), t.edit(0, 0), "bad edit response")
	emacs.edit(t.edit(0, 0, 20, 0, "uiop"), t.edit(0, 0), "bad edit response")
	browser.edit(t.edit(0, 0), t.edit(0, 0, 16, 0, "zxcvuiop"), "bad edit response")
}
