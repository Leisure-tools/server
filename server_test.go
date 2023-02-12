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

func (j jsonObj) String() string {
	switch j.typeof() {
	case "string":
		return "\"" + strings.ReplaceAll(j.asString(), "\"", "\\\"") + "\""
	case "array":
		sb := &strings.Builder{}
		sb.WriteString("[")
		first := true
		for i := 0; i < j.len(); i++ {
			if first {
				first = false
			} else {
				sb.WriteString(",")
			}
			sb.WriteString(j.getJson(i).String())
		}
		sb.WriteString("]")
		return sb.String()
	case "object":
		sb := &strings.Builder{}
		sb.WriteString("{")
		first := true
		for _, key := range j.keys() {
			if first {
				first = false
			} else {
				sb.WriteString(",")
			}
			sb.WriteString(jsonV(key).String())
			sb.WriteString(":")
			sb.WriteString(j.getJson(key).String())
		}
		sb.WriteString("}")
		return sb.String()
	default:
		return fmt.Sprint(j.v)
	}
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

func testEqual(t *testing.T, actual any, expected any, msg any) {
	if !jsonV(actual).equals(jsonV(expected)) {
		die(t, fmt.Sprintf("%s: expected <%v> but got <%v>", msg, expected, actual))
	}
}

func failNowIfNot(t *testing.T, cond bool, msg any) {
	if !cond {
		die(t, msg)
	}
}

func failNowIfErr(t *testing.T, err any) {
	if err != nil {
		die(t, err)
	}
}

type testServer struct {
	t   *testing.T
	mux *http.ServeMux
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
	testEqual(sv.t, resp.status, http.StatusOK, lazy(func() string {
		return fmt.Sprintf("Unsuccessful request: %s", resp.buf.String())
	}))
	return req, resp
}

func (sv *testServer) post(url ...any) (*http.Request, *MockResponseWriter) {
	body := url[len(url)-1]
	url = url[:len(url)-1]
	req, resp := sv.route(sv.mux, http.MethodPost, urlFor(url), cookieFor(url), fmt.Sprint(body))
	testEqual(sv.t, resp.status, http.StatusOK, lazy(func() string {
		return fmt.Sprintf("Unsuccessful request: %s", resp.buf.String())
	}))
	return req, resp
}

func createServer(t *testing.T, name string) (*LeisureService, *testServer) {
	mux := http.NewServeMux()
	ws := Initialize(name, mux, MemoryStorage)
	sv := &testServer{
		t:   t,
		mux: mux,
	}
	return ws, sv
}

func TestCreate(t *testing.T) {
	ws, sv := createServer(t, "test")
	_, resp := sv.post(DOC_CREATE, UUID, "?", "alias", "fred", "bob content")
	_, resp = sv.get(DOC_LIST)
	testEqual(t, resp.buf.String(), "[[\""+UUID+"\",\"fred\"]]", "Bad document list")
	_, resp = sv.get(SESSION_CREATE, "s1", "fred")
	_, resp = sv.get(SESSION_LIST)
	session := ws.sessions["s1"]
	l := session.History.Latest[session.Peer]
	if l == nil {
		l = session.History.Source
	}
	testEqual(t, l.GetDocument(session.History).String(), "bob content", "Documents are not the same")
	ws.shutdown()
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

func (w *MockResponseWriter) jsonDecode(t *testing.T) jsonObj {
	var obj any
	if err := json.Unmarshal(w.buf.Bytes(), &obj); err != nil {
		die(t, err)
	}
	return jsonV(obj)
}

func jsonEncode(t *testing.T, obj any) string {
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

func (sv *testServer) cookie(req *http.Request, resp *MockResponseWriter) *http.Cookie {
	return sv.cookies(req, resp)[0]
}

func (sv *testServer) cookies(req *http.Request, resp *MockResponseWriter) []*http.Cookie {
	buf := &bytes.Buffer{}
	resp.header.Write(buf)
	resp.WriteHeader(resp.status)
	if hresp, err := http.ReadResponse(resp.createResponse(), req); err != nil {
		sv.die(err)
		return nil
	} else {
		return hresp.Cookies()
	}
}

func TestEdits(t *testing.T) {
	ws, sv := createServer(t, "test")
	sv.post(DOC_CREATE, UUID, "?", "alias", "fred", doc1)
	c := sv.cookies(sv.get(SESSION_CREATE, "emacs", "fred"))
	testEqual(t, len(c), 1, "Bad number of cookies")
	cookie := c[0]
	keyValue := strings.Split(cookie.Value, "=")
	testEqual(t, len(keyValue), 2, fmt.Sprintf("Bad cookie format, expected SESSION_ID=SESSION_KEY but got %s", cookie.Value))
	_, resp := sv.get(SESSION_GET, cookie)
	testEqual(t, resp.buf.String(), doc1, "Bad document")
	d1 := doc.NewDocument(doc1)
	d1.Replace("emacs", index(d1.String(), 0, 5), 3, "ONE")
	d1.Replace("emacs", index(d1.String(), 2, 10), 0, "\nline four")
	_, resp = sv.post(SESSION_EDIT, cookie, jsonEncode(t, jmap(
		"replacements", replacements(d1.Edits()),
		"selectionOffset", 0,
		"selectionLength", 0,
	)))
	_, resp = sv.get(SESSION_GET, cookie)
	testEqual(t, resp.buf.String(), doc1Edited, "bad document")
	ws.shutdown()
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
	testEqual(sv.t, transform(resp), expected, errMsg)
}

// testPost([transform-func], URL, args..., data, expected, errorMessage)
func (sv *testServer) testPost(args ...any) {
	transform, args, expected, errMsg := testFetchArgs(4, args, "bad use of testPost")
	_, resp := sv.post(args...)
	testEqual(sv.t, transform(resp), expected, errMsg)
}

func TestTwoPeers(t *testing.T) {
	ws, sv := createServer(t, "test")
	sv.post(DOC_CREATE, "0000", "?", "alias", "bubba", "hello there")
	emacs := sv.cookie(sv.get(SESSION_CREATE, "emacs", "bubba"))
	vscode := sv.cookie(sv.get(SESSION_CREATE, "vscode", "bubba"))
	sv.post(SESSION_EDIT, vscode,
		`{
  "selectionOffset":0,
  "selectionLength":0,
  "replacements": [
    {
      "offset": 6,
      "length": 5,
      "text": "goodbye"
    }
  ]
}`)
	sv.testGet(SESSION_GET, vscode, "hello goodbye", "Unexpected document")
	sv.testGet(sv.jsonDecode, SESSION_UPDATE, emacs, true, "Expected update")
	sv.testGet(SESSION_GET, emacs, "hello there", "Unexpected document")
	session := ws.sessions["emacs"]
	fmt.Printf("EMACS: %v\n", session.Peer)
	fmt.Printf("EMACS-lastest: %v\n", session.History.Latest[session.Peer])
	sv.testPost(sv.jsonDecode, SESSION_EDIT, emacs,
		`{
	  "selectionOffset":0,
	  "selectionLength":0,
	  "replacements": []
	}`, jmap(
			"selectionOffset", -1,
			"selectionLength", -1,
			"replacements", []doc.Replacement{
				{
					Offset: 6,
					Length: 5,
					Text:   "goodbye"},
			}),
		"Bad replacement")
	sv.testGet(SESSION_GET, emacs, "hello goodbye", "Unexpected document")
	ws.shutdown()
}
