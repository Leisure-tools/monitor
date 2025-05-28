package main

// document is {"id":ID,"serial":SERIAL,"blocks":{NAME:BLOCK,...}}

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"os/user"
	"path"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/leisure-tools/monitor"
)

const (
	DEFAULT_UNIX_SOCKET = ".monitor.socket"
)

var cmdctx *kong.Context
var ErrSocketFailure = monitor.NewMonError("socketFailure")
var ErrBadCommand = monitor.NewMonError("badCommand")
var exitCode = 0
var die = func(exitCodes ...int) {
	if len(exitCodes) > 0 {
		exitCode = exitCodes[0]
	}
	os.Exit(exitCode)
}

type CLI struct {
	mon           *monitor.Monitoring
	ctx           *kong.Context
	clientGlobals ClientGlobals
	Verbose       int        `short:v help:Verbose type:counter`
	Serve         ServeCmd   `cmd help:"Start a server" `
	List          ListCmd    `cmd help:"List documents"`
	Add           AddCmd     `cmd help:"Add a new document"`
	Remove        RemoveCmd  `cmd help:"Remove a document"`
	Get           GetCmd     `cmd help:"Get a document or a block within it"`
	Patch         PatchCmd   `cmd help:"Add or replace blocks in a document"`
	Updates       UpdatesCmd `cmd help:"Fetch any updates since the given serial"`
}

type ClientGlobals struct {
	UnixSock string `short:u help:"Unix domain socket to connect to, defaults ~/.monitor.socket"`
	Sock     string `short:h help:"Host to connect to, defaults to localhost:8080 if unix socket does not exist"`
}

type URLOpts struct {
	url string
	Url string `optional help:"Url of host, defaults to localhost:8080"`
}

type ServeCmd struct {
	UnixSocket string `short:u help:"Unix domain socket name, defaults to ~/.monitor.socket, use \"\" to avoid listing"`
	Tcp        string `short:t optional help:"[host:]port to listen on, defaults to localhost:8080"`
	Redis      string `arg help:"REDIS connect string"`
}

type ListCmd struct {
	*ClientGlobals
}

type AddCmd struct {
	*ClientGlobals
	Doc string `arg help:"doument name"`
}

type RemoveCmd struct {
	*ClientGlobals
	Doc string `arg help:"doument name"`
}

type GetCmd struct {
	*ClientGlobals
	Doc   string `arg help:"doument name"`
	Block string `arg optional help:"name of block to get"`
}

type PatchCmd struct {
	*ClientGlobals
	Doc string `arg help:"doument name"`
}

type UpdatesCmd struct {
	*ClientGlobals
	NoData  bool    `help:"request only the serial for the document" `
	Doc     string  `arg help:"doument name"`
	Serial  string  `arg help:"serial to start at" `
	Timeout float64 `optional short:t help:"Timeout in seconds"`
}

type myMux struct {
	*http.ServeMux
}

func (mux *myMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawPath != "" {
		r.URL.Path = r.URL.RawPath
	}
	mux.ServeMux.ServeHTTP(w, r)
}

func (cli *CLI) verbose(format string, args ...any) {
	cli.verboseN(1, format, args...)
}

func (cli *CLI) verboseN(level int, format string, args ...any) {
	if level <= cli.Verbose {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

func abort(format string, args ...any) {
	fmt.Fprintln(os.Stderr, fmt.Errorf(format, args...))
	exitCode = 1
	die()
}

func (cmd *ServeCmd) Run(cli *CLI) error {
	mux := http.NewServeMux()
	var err error
	if cli.mon, err = monitor.New(cmd.Redis, cli.Verbose); err != nil {
		abort("Could not connect to REDIS")
	}
	cli.mon.Verbose = cli.Verbose
	cli.mon.InitMux(mux)
	var listener *net.UnixListener
	die = func(exitCodes ...int) {
		if listener != nil {
			listener.Close()
		}
		if len(exitCodes) > 0 {
			exitCode = exitCodes[0]
		}
		os.Exit(exitCode)
	}
	unix, host, port := GetSocks(cmd.UnixSocket, cmd.Tcp, true)
	cli.verbose("Sockets UNIX: %s, HOST: %s, PORT: %d", unix, host, port)
	if host != "" && unix != "" {
		go http.ListenAndServe(fmt.Sprintf("%s:%d", host, port), mux)
	} else if host != "" {
		http.ListenAndServe(fmt.Sprintf("%s:%d", host, port), mux)
	} else if unix == "" {
		abort("No socket or UNIX socket to listen on")
	}
	if unix != "" {
		cli.verbose("UNIX SOCKET: %s", unix)
		if addr, err := net.ResolveUnixAddr("unix", unix); err != nil {
			abort("%w: could not resolve unix socket %s", ErrSocketFailure, cmd.UnixSocket)
		} else if listener, err = net.ListenUnix("unix", addr); err != nil {
			abort("%w: could not listen on unix socket %s", ErrSocketFailure, cmd.UnixSocket)
		} else {
			listener.SetUnlinkOnClose(true)
			cli.verbose("RUNNING UNIX DOMAIN SERVER: %s", addr)
			abort("%v", http.Serve(listener, &myMux{mux}))
		}
	}
	return nil
}

func GetSocks(unix, tcp string, listen bool) (string, string, int) {
	supplied := unix != ""
	if unix == "" {
		if userObj, err := user.Current(); err != nil {
			abort("Could not determine current user: %s", err)
		} else {
			unix = path.Join(userObj.HomeDir, DEFAULT_UNIX_SOCKET)
		}
	}
	info, err := os.Stat(unix)
	if !(listen && os.IsNotExist(err)) {
		if supplied && os.IsNotExist(err) {
			abort("%s does not exist", unix)
		} else if supplied && err != nil {
			abort("Cannot access %s", unix)
		} else if supplied && info.Mode()|fs.ModeSocket == 0 {
			abort("%s is not a UNIX domaindoes not exist", unix)
		} else if err != nil {
			unix = ""
		}
	}
	host := ""
	port := 8080
	parts := strings.Split(tcp, ":")
	if tcp == "" {
		parts = []string{}
	}
	switch len(parts) {
	case 0:
	case 1:
		if _, err := fmt.Sscanf(parts[len(parts)-1], "%d", &port); err != nil {
			abort("bad socket format, expected [hostname:]portnumber but got '%s'", tcp)
		} else if port != port|0xFFFF {
			abort("bad port value, expected 0-65535 but got %d", port)
		}
		fallthrough
	case 2:
		if parts[0] == "" {
			host = "localhost"
		} else {
			host = parts[0]
		}
	default:
		abort("bad socket format, expected [hostname:]portnumber but got '%s' (%d)", tcp, len(parts))
	}
	if unix == "" && host == "" {
		abort("No UNIX domain or TCP socket address provided")
	}
	return unix, host, port
}

func (cmd *ListCmd) Run(cli *CLI) error {
	obj := monitor.JsonV(parseJson(cli.get(monitor.URL_PREFIX + "list")))
	if !obj.IsArray() {
		abort("bad server response, expected []string but got %#v", obj.V)
	}
	for i := 0; i < obj.Len(); i++ {
		if o := obj.GetJson(i); !o.IsString() {
			abort("bad server response, expected []string but got %#v", o.V)
		}
	}
	if obj.Len() == 0 {
		println("No sessions")
	} else {
		for i := 0; i < obj.Len(); i++ {
			fmt.Println(obj.Get(i))
		}
	}
	return nil
}

func (cmd *AddCmd) Run(cli *CLI) error {
	resp := cli.post(monitor.URL_PREFIX+cmd.Doc, os.Stdin)
	checkError(resp)
	outputResponse(resp)
	return nil
}

func (cmd *RemoveCmd) Run(cli *CLI) error {
	return nil
}

func (cmd *GetCmd) Run(cli *CLI) error {
	url := monitor.URL_PREFIX + cmd.Doc
	if cmd.Block != "" {
		url += "/" + cmd.Block
	}
	resp := cli.get(url)
	checkError(resp)
	outputResponse(resp)
	return nil
}

func (cmd *PatchCmd) Run(cli *CLI) error {
	resp := cli.patch(monitor.URL_PREFIX+cmd.Doc, os.Stdin)
	checkError(resp)
	outputResponse(resp)
	return nil
}

func (cmd *UpdatesCmd) Run(cli *CLI) error {
	ser := cmd.Serial
	if ser == "" {
		ser = "0"
	}
	url := fmt.Sprintf("%s%s/%s/%s", monitor.URL_PREFIX, cmd.Doc, "update", ser)
	if cmd.NoData {
		url = addQuery(url, "nodata", "true")
	}
	if cmd.Timeout > 0 {
		url = addQuery(url, "timeout", cmd.Timeout)
	}
	resp := cli.get(url)
	checkError(resp)
	outputResponse(resp)
	return nil
}

func addQuery(url string, key string, value any) string {
	sep := "&"
	if !strings.Contains(url, "?") {
		sep = "?"
	}
	url = fmt.Sprint(url, sep, key, "=", value)
	return url
}

func (cli *CLI) get(url string, components ...string) *http.Response {
	return cli.request("GET", nil, url, components...)
}

func (cli *CLI) post(url string, body io.Reader) *http.Response {
	return cli.request("POST", body, url)
}

func (cli *CLI) patch(url string, body io.Reader) *http.Response {
	return cli.request("PATCH", body, url)
}

func (cli *CLI) postOrGet(url string) *http.Response {
	if body, err := io.ReadAll(os.Stdin); err != nil {
		abort("%w: error reading input", ErrBadCommand)
		return nil
	} else if len(body) > 0 {
		return cli.post(url, bytes.NewReader(body))
	} else {
		return cli.get(url)
	}
}

func (cli *CLI) request(method string, body io.Reader, urlStr string, moreUrl ...string) *http.Response {
	unix, host, port := GetSocks(cli.clientGlobals.UnixSock, cli.clientGlobals.Sock, false)
	for i, str := range moreUrl {
		moreUrl[i] = url.PathEscape(str)
	}
	uri := "http://unix"
	if unix == "" {
		uri = fmt.Sprintf("http://%s:%d", host, port)
	}
	if path, err := url.JoinPath(urlStr, moreUrl...); err != nil {
		cli.ctx.PrintUsage(true)
		os.Exit(1)
		return nil
	} else if req, err := http.NewRequest(method, uri+path, body); err != nil {
		panic(err)
	} else {
		req.URL = &url.URL{
			Scheme: req.URL.Scheme,
			Host:   req.URL.Host,
			Opaque: path,
		}
		cli.verbose("%s %s", method, req.URL.String())
		if body != nil {
			req.Header.Set("Content-Type", "text/plain")
		}
		client := cli.httpClient(unix, host, port)
		if resp, err := client.Do(req); err != nil {
			panic(err)
		} else {
			return resp
		}
	}
}

func (cli *CLI) httpClient(unix, host string, port int) *http.Client {
	if unix != "" {
		cli.verbose("USING UNIX SOCKET: '%s'", unix)
		return &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", unix)
				},
			},
		}
	} else if host != "" {
		cli.verbose("USING HOST: %s, PORT: %d", host, port)
		return &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("tcp", fmt.Sprint(host, ":", port))
				},
			},
		}
	}
	abort("No socket provided")
	return nil
}

func outputResponse(resp *http.Response) {
	checkError(resp)
	outputWithNL(os.Stdout, resp.Body)
}

func checkError(resp *http.Response) {
	if resp.StatusCode != http.StatusOK {
		fmt.Fprint(os.Stderr, "Error in command ", cmdctx.Selected().Path(), ": ")
		var obj any
		var buf []byte
		var err error
		exitCode = resp.StatusCode
		if buf, err = io.ReadAll(resp.Body); err != nil {
			panic(err)
		} else if err := json.Unmarshal(buf, &obj); err == nil {
			if errObj, ok := obj.(map[string]any); ok && errObj["error"] != "" {
				errObj["args"] = os.Args
				if j, jerr := json.Marshal(errObj); jerr == nil {
					outputWithNL(os.Stderr, bytes.NewBuffer(j))
					die()
				}
			}
		}
		outputWithNL(os.Stdout, bytes.NewBuffer(buf))
		die()
	}
}

func outputWithNL(w io.Writer, r io.Reader) error {
	tr := io.TeeReader(r, w)
	buf := make([]byte, 4096)
	var n int
	var err error
	last := byte(0)
	for err == nil {
		if n, err = tr.Read(buf); err == nil {
			break
		} else if n > 0 {
			last = buf[n-1]
		}
	}
	if last != '\n' {
		fmt.Fprintln(w)
	}
	return err
}

func parseJson(resp *http.Response) any {
	checkError(resp)
	var obj any
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		abort("%v", err)
	}
	if err = json.Unmarshal(buf, &obj); err != nil {
		abort("%v", err)
	}
	return obj
}

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		die(1)
	}()
	cli := CLI{}
	cli.List.ClientGlobals = &cli.clientGlobals
	cli.Add.ClientGlobals = &cli.clientGlobals
	cli.Remove.ClientGlobals = &cli.clientGlobals
	cli.Get.ClientGlobals = &cli.clientGlobals
	cli.Patch.ClientGlobals = &cli.clientGlobals
	cli.Updates.ClientGlobals = &cli.clientGlobals
	cmdctx = kong.Parse(&cli)
	cli.verbose("RUNNING %s %s\n", path.Base(os.Args[0]), strings.Join(os.Args[1:], " "))
	cli.ctx = cmdctx
	cmdctx.Run(&cli)
}
