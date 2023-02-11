package server

import (
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

func die(t fallible, args ...any) {
	t.Error(args...)
	t.FailNow()
}

type lazyString func() string

func lazy(f func() string) lazyString {
	return lazyString(f)
}

func (l lazyString) String() string {
	return l()
}

func testEqual(t *testing.T, actual any, expected any, msg any) {
	failNowIfNot(t, actual == expected, fmt.Sprintf("%s: expected <%v> but got <%v>", msg, expected, actual))
}

func failNowIfNot(t *testing.T, cond bool, msg any) {
	if !cond {
		t.Log(msg)
		fmt.Fprintln(os.Stderr, msg)
		debug.PrintStack()
		t.FailNow()
	}
}

func failNowIfErr(t *testing.T, err any) {
	if err != nil {
		t.Log(err)
		fmt.Fprintln(os.Stderr, err)
		debug.PrintStack()
		t.FailNow()
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

func (sv *testServer) route(mux *http.ServeMux, method, url, body string) *MockResponseWriter {
	buf := emptyBody()
	if body != "" {
		buf = bytes.NewBuffer([]byte(body))
	}
	req := r1(http.NewRequest(method, url, buf)).check(sv)
	//fmt.Printf("Routing request %v\n", req)
	resp := newMockResponseWriter()
	mux.ServeHTTP(resp, req)
	return resp
}

func urlFor(url ...any) string {
	strs := make([]string, 0, 4)
	query := make([]string, 0, 4)
	inQuery := false
	key := ""
	for _, u := range url {
		if u == "?" {
			inQuery = true
			continue
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

func (sv *testServer) get(url ...any) *MockResponseWriter {
	resp := sv.route(sv.mux, http.MethodGet, urlFor(url...), "")
	testEqual(sv.t, resp.status, http.StatusOK, lazy(func() string {
		return fmt.Sprintf("Unsuccessful request: %s", resp.buf.String())
	}))
	return resp
}

func (sv *testServer) post(url ...any) *MockResponseWriter {
	body := url[len(url)-1]
	url = url[:len(url)-1]
	resp := sv.route(sv.mux, http.MethodPost, urlFor(url...), fmt.Sprint(body))
	testEqual(sv.t, resp.status, http.StatusOK, lazy(func() string {
		return fmt.Sprintf("Unsuccessful request: %s", resp.buf.String())
	}))
	return resp
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
	resp := sv.post(DOC_CREATE, UUID, "?", "alias", "fred", "bob content")
	resp = sv.get(DOC_LIST)
	testEqual(t, resp.buf.String(), "[[\""+UUID+"\",\"fred\"]]", "Bad document list")
	resp = sv.get(SESSION_CREATE, "s1", "fred")
	resp = sv.get(SESSION_LIST)
	//fmt.Println("response:", resp.buf)
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
		t.Error(err)
		t.FailNow()
	}
	return jsonObj{obj}
}

func jsonEncode(t *testing.T, obj any) string {
	data, err := json.Marshal(obj)
	if err != nil {
		t.Error(err)
		t.FailNow()
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

func TestEdits(t *testing.T) {
	ws, sv := createServer(t, "test")
	sv.post(DOC_CREATE, UUID, "?", "alias", "fred", doc1)
	sv.get(SESSION_CREATE, "emacs", "fred")
	keyObj := sv.get(SESSION_CONNECT, "emacs").jsonDecode(t)
	testEqual(t, keyObj.isString(), true, "expected string")
	key := keyObj.v
	testEqual(t, sv.get(SESSION_GET, "emacs", "?", "key", key).jsonDecode(t).v, doc1, "Bad document")
	d1 := doc.NewDocument(doc1)
	d1.Replace("emacs", index(d1.String(), 0, 5), 3, "ONE")
	d1.Replace("emacs", index(d1.String(), 2, 10), 0, "\nline four")
	resp := sv.post(SESSION_REPLACE, "emacs", "?", "key", key, jsonEncode(t, jmap(
		"replacements", replacements(d1.Edits()),
		"selectionOffset", 0,
		"selectionLength", 0,
	)))
	fmt.Printf("result: %s\n", resp.buf)
	testEqual(t, sv.get(SESSION_GET, "emacs", "?", "key", key).jsonDecode(t).v, doc1Edited, "")
	ws.shutdown()
}
