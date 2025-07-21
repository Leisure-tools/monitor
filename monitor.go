/*
Package monitor implements the Monitor.jl Protocol.

Here is an [architecture overview] and an [animated screencap].

# Capabilities

The Monitor.jl Protocol communicates with simple JSON structures called “blocks” to support
some simple capabilities.

 1. Dynamically monitor Juila data, automatically detect when it changes, and report the new values.

    - You use the connection to make and remove monitors in running programs.

    - Polls for value changes – optionally specify how often to check values.

    - Polling means you do **not** need to alter your code to inform the system when a value changes.

 2. Modify mutable values.

 3. Evaluate code.

 4. Exchange data between programs.

 5. Treat programs like ”database shards”.

 6. Optionally target a subset of subscribers.

 7. Communicate via different transports.

    - Pubsub with REDIS streams.

    - Named pipes.

    - Easy to extend with custom transports.

Monitor.jl can publish to different topics for different purposes.

  - Evaluate code and select data to monitor.
  - Send updates to monitored values.
  - Control programs for rollups or MapReduce.

Connected UIs, like notebooks, can present data blocks in a meaningful way.

  - Blocks can update in-place.
  - Eval blocks can be sent with a button click.
  - Blocks can appear in sections based on tags.
  - Views can create more monitors and change monitored values.

Monitor.jl supports 4 types of blocks with some common properties.

  - type: how a subscriber should interpret the block. Supported types are "monitor", "code", "data", and "delete".
  - name: This block’s name; receivers overwrite the previous values for duplicates.
  - origin: ID of the subscriber that produced this block.
  - topics: optional topic(s) to publish the block to – transports should route these.
  - tags: identifies sets of blocks this block “belongs to”. Can be a string or an array of strings; monitor and data blocks can use this to categorize results and also for cleanup.
  - targets: optional subscriber or list of subscribers that should receive the block (others ignore it).

# Block types

Monitor blocks monitor Julia values.
  - type: "monitor".
  - topic: controls which program(s) install the monitor.
  - targets: controls which program(s) install the monitor.
  - updateTopics: controls which programs receive updates; a UI sending a monitor with this property should automatically subscribe to the topics.
  - updateTargets: controls which programs receive updates.
  - tags: identifyies a set of blocks. Can be a string or an array of strings.
  - root: root value for the variables.
  - quiet: while true, monitor but don't publish updates.
  - value: variables that monitor values; initial values are not placed into Julia but incoming changes are not placed into Julia data.

Code blocks run Julia code
  - type: "code".
  - topic: optional topic to publish the block to, when it’s not the default.
  - targets: optional list of subscribers that should receive the block (others ignore it).
  - tags: identifyies a set of blocks. Can be a string or an array of strings.
  - language: language in which to evaluate code.
  - return: true if the code should return a block to be published.
  - value: code to evaluate.

Data Blocks hold data, can be used for responses.
  - type: "data".
  - topic: optional topic to publish the block to, when it’s not the default.
  - targets: optional list of subscribers that should receive the block (others ignore it).
  - tags: identifyies a set of blocks. Can be a string or an array of strings.
  - code: optional name of the code block that produced this block, if was one.
  - value: value of data.

Delete Blocks remove monitors and data blocks.
  - type: "delete".
  - topic: optional topic to publish the block to, when it’s not the default.
  - targets: optional list of subscribers that should receive the block (others ignore it).
  - value: NAME, [NAME, ...], {"tagged": TAG}, or {"tagged": [TAG, ...]}.

# API: RemoteMonitor methods and their HTTP equivalents

 1. (mon *Monitoring) Add(docId string) (*RemoteMonitor, error)

    - GET /v1/document/{documentId}/add -- add an empty document

 2. (rm *RemoteMonitor) Delete()

    - GET /v1/document/{documentId}/remove -- remove a document

 3. rm.blocks

    - GET /v1/document/{documentId} -- get a document

    - GET /v1/document/{documentId}/{name} -- get a block in a document

 3. (rm *RemoteMonitor) Patch(send bool, blocks ...map[string]any) error

    - PATCH /v1/document/{documentId} -- patch an array of blocks into a document

 4. (rm *RemoteMonitor) deleteBlocks(names ...string) error

    - GET /v1/document/delete/{documentId}/{name} -- send a delete block (and remove from local copy)

    - POST /v1/document/delete/{documentId} -- delete posted blocks (and remove from local copy)

 5. (rm *RemoteMonitor) getUpdate(serial int64) (map[string]any, []string)

    - GET /v1/document/update/{documentId}/{serial} -- retrieve blocks that have changed since serial. Returns {"serial":SERIAL,"blocks":{NAME:BLOCK,...}}

[architecture overview]: https://raw.githubusercontent.com/Leisure-tools/Monitor.jl/main/arch.png
[animated screencap]: https://raw.githubusercontent.com/Leisure-tools/Monitor.jl/main/screencap.gif
*/
package monitor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"maps"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
	u "github.com/leisure-tools/utils"
	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/yaml"
)

type StringQueue = u.ConcurrentQueue[string]

const (
	DEFAULT_TRIM_PERIOD = int64(2 * time.Minute)
	ONE_MINUTE          = int64(time.Minute)
	TOPIC_PREFIX        = "MONITOR-"
	//TOPIC_PREFIX         = " MONITOR "
	URL_PREFIX           = "/monitor/v1/document/"
	LIST                 = "GET " + URL_PREFIX + "list"
	ADD                  = "POST " + URL_PREFIX + "{documentId}"
	ADD_FILE             = "GET " + URL_PREFIX + "{documentId}/add/{file}"
	PATCH                = "PATCH " + URL_PREFIX + "{documentId}"
	GET                  = "GET " + URL_PREFIX + "{documentId}"
	GET_BLOCK            = "GET " + URL_PREFIX + "{documentId}/get/{name}"
	REMOVE               = "GET " + URL_PREFIX + "{documentId}/remove"
	DELETE               = "GET " + URL_PREFIX + "{documentId}/delete/{name}"
	DELETE_POST          = "POST " + URL_PREFIX + "{documentId}/delete"
	UPDATE               = "GET " + URL_PREFIX + "{documentId}/update/{serial}"
	TRACK                = "GET " + URL_PREFIX + "{documentId}/track"
	TRACK_FILE           = "GET " + URL_PREFIX + "{documentId}/track/{file}"
	NOTRACK              = "GET " + URL_PREFIX + "{documentId}/notrack"
	CLEAR                = "GET " + URL_PREFIX + "clear"
	DUMP                 = "GET " + URL_PREFIX + "dump"
	K                    = 1024
	M                    = K * 1024
	G                    = M * 1024
	DEFAULT_REDIS_MILLIS = 200 * time.Millisecond
)

type monError struct {
	Type    string
	Data    map[string]any
	wrapped error
}

type Monitoring struct {
	redis               *redis.Client
	Monitors            map[string]*RemoteMonitor // document id -> remote monitor
	Service             *ChanSvc
	Activity            string // current service activity
	Verbose             int
	Tracker             *fsnotify.Watcher
	TrimPeriod          int64
	Tracking            u.Set[string]
	StreamUpdateChannel chan bool // received when the streams change
}

// a connection for a document
// the default stream name is MONITOR_SESSION_PREFIX + document ID
type RemoteMonitor struct {
	*Monitoring
	PeerId           string
	DocId            string
	Blocks           map[string]map[string]any
	Dead             map[string]int64
	StreamSerials    map[string]int64
	Service          *ChanSvc
	IncomingUpdate   atomic.Int64
	ProcessedUpdate  atomic.Int64
	ProcessingUpdate atomic.Bool
	DefaultStream    string
	AllStreams       u.Set[string]
	StreamIDs        map[string]string
	TopicMap         map[string]string
	Serial           int64 // current update number for the connection
	UpdateTimers     u.Set[*time.Timer]
	Listeners        u.Set[MonitorListener]
	Track            string
	BlockOrder       []string
	Changed          *u.ConcurrentQueue[map[string]any]
}

type RedisConf struct {
	User string
	Host string
	Db   int
	Conf string
}

type MonitorListener interface {
	DataChanged(rm *RemoteMonitor) // called within the RemoteMonitor's service routine
}

func NewMonError(errorType string, values ...any) monError {
	e := monError{Type: errorType}
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

var MON_PAT, _ = regexp.Compile("^(redis://)?((?<user>[^:@]+)?(:(?<pass>[^@]+))?@)?((?<host>[^:/]+)(:(?<port>[0-9]+))?)?(/(?<db>[0-9]+)?)?$")
var monitorNum = 0
var ErrBadArgument = NewMonError("badArgument")
var ErrNoBlockForName = NewMonError("noBlockForName")
var ErrBadBlock = NewMonError("badBlock")
var ErrSendingBlocks = NewMonError("cantSendBlocks")
var ErrNoMonitorConnection = NewMonError("noMonitorConnectionForSession")
var ErrUnknown = NewMonError("unknownError")
var ErrDocumentExists = NewMonError("documentExists")
var ErrDocumentAliasExists = NewMonError("documentAliasExists")
var ErrCommandFormat = NewMonError("badCommandFormat")
var ErrUnknownDocument = NewMonError("unknownDocument")
var ErrUnknownSession = NewMonError("unknownSession")
var ErrDuplicateSession = NewMonError("duplicateSession")
var ErrDuplicateConnection = NewMonError("duplicateConnection")
var ErrExpiredSession = NewMonError("expiredSession")
var ErrInternalError = NewMonError("internalError")
var ErrDataMissing = NewMonError("dataMissing")
var ErrDataMismatch = NewMonError("dataMismatch")
var ErrSessionType = NewMonError("badSessionType")

func (err monError) With(keysAndValues ...any) monError {
	result := monError{
		Type:    err.Type,
		Data:    make(map[string]any, len(keysAndValues)/2),
		wrapped: err.wrapped,
	}
	for i := 0; i < len(keysAndValues); i += 2 {
		result.Data[keysAndValues[i].(string)] = keysAndValues[i+1]
	}
	return result
}

func (err monError) Error() string {
	return err.Type
}

func (err monError) Unwrap() error {
	return err.wrapped
}

func New(conStr string, verbose int, tlsconf *tls.Config) (*Monitoring, error) {
	if m := MON_PAT.FindStringSubmatch(conStr); m == nil {
		return nil, fmt.Errorf("%w: Bad REDIS connection string, %s", ErrBadArgument, conStr)
	} else {
		match := func(name, defaultValue string) string {
			r := m[MON_PAT.SubexpIndex(name)]
			if r == "" {
				r = defaultValue
			}
			return r
		}
		host := match("host", "127.0.0.1")
		port := match("port", "6739")
		user := match("user", "")
		pass := match("pass", "")
		db := match("db", "0")
		var dbNum int
		fmt.Sscanf(db, "%d", &dbNum)
		if tracker, err := fsnotify.NewWatcher(); err != nil {
			return nil, err
		} else {
			fmt.Printf("Connecting to REDIS with %s, user: %s, pass: %s, host: %s, port: %s, tls: %v\n",
				conStr, user, pass, host, port, tlsconf != nil)
			mon := &Monitoring{
				TrimPeriod: DEFAULT_TRIM_PERIOD,
				Monitors:   make(map[string]*RemoteMonitor),
				redis: redis.NewClient(&redis.Options{
					Username:  user,
					Password:  pass,
					Addr:      fmt.Sprint(host, ":", port),
					DB:        dbNum,
					TLSConfig: tlsconf,
				}),
				Verbose:             verbose,
				Tracker:             tracker,
				StreamUpdateChannel: make(chan bool),
			}
			println("CONNECTED")
			mon.Service = NewSvc(mon)
			mon.verbose("Connecting to REDIS: %s", conStr)
			mon.WatchRedis(DEFAULT_REDIS_MILLIS)
			mon.startTrimmer()
			mon.startFileWatcher()
			return mon, nil
		}
	}
}

func (mon *Monitoring) Svc(code func(), wrap ...bool) {
	mon.Service.Svc(code, wrap...)
}

func (mon *Monitoring) startTrimmer() {
	go func() {
		bg := context.Background()
		for {
			minID := fmt.Sprintf("%d-0", (time.Now().UnixNano()-mon.TrimPeriod)/int64(time.Millisecond))
			for cursor := uint64(0); cursor == 0; {
				cmd := mon.redis.ScanType(bg, cursor, "*", 1000, "stream")
				if keys, cur, err := cmd.Result(); err != nil {
					fmt.Fprintf(os.Stderr, "Error getting streams: %v\n", err)
					os.Exit(1)
				} else {
					cursor = cur
					for _, key := range keys {
						if strings.HasPrefix(key, TOPIC_PREFIX) {
							mon.redis.XTrimMinID(bg, key, minID)
						}
					}
				}
			}
			period := min(mon.TrimPeriod, ONE_MINUTE)
			time.Sleep(time.Duration(period))
		}
	}()
}

type MonContext struct {
	mon            *Monitoring
	monitorScratch []*RemoteMonitor
}

func (mon *Monitoring) ctx() *MonContext {
	return &MonContext{mon: mon}
}

func (ctx *MonContext) monitors() []*RemoteMonitor {
	SvcSync(ctx.mon, func() (bool, error) {
		ctx.monitorScratch = ctx.monitorScratch[:0]
		for _, rm := range ctx.mon.Monitors {
			ctx.monitorScratch = append(ctx.monitorScratch, rm)
		}
		return true, nil
	})
	return ctx.monitorScratch
}

func (ctx *MonContext) monitorsTracking(files u.Set[string]) []*RemoteMonitor {
	SvcSync(ctx.mon, func() (bool, error) {
		ctx.monitorScratch = ctx.monitorScratch[:0]
		for _, rm := range ctx.mon.Monitors {
			if files.Has(rm.Track) {
				ctx.monitorScratch = append(ctx.monitorScratch, rm)
			}
		}
		return true, nil
	})
	return ctx.monitorScratch
}

func (mon *Monitoring) monitorTracking(file string) *RemoteMonitor {
	rm, _ := SvcSync(mon, func() (*RemoteMonitor, error) {
		for _, rm := range mon.Monitors {
			if rm.Track == file {
				return rm, nil
			}
		}
		return nil, nil
	})
	return rm
}

func (mon *Monitoring) addFile(file string) error {
	if dir := path.Dir(file); !mon.Tracking.Has(dir) {
		if err := mon.Tracker.Add(dir); err != nil {
			return err
		}
		mon.Tracking.Add(dir)
	}
	return nil
}

func (mon *Monitoring) removeFile(file string) error {
	if dir := path.Dir(file); mon.Tracking.Has(dir) {
		for _, rm := range mon.Monitors {
			if rm.Track != "" && path.Dir(rm.Track) == dir {
				return nil
			}
		}
		if err := mon.Tracker.Remove(dir); err != nil {
			return err
		}
		mon.Tracking.Remove(dir)
	}
	return nil
}

// TODO ** watch directory instead of file
func (mon *Monitoring) startFileWatcher() {
	itemQ := StringQueue{}
	pending := atomic.Bool{}
	enqueue := func(file string) {
		itemQ.Enqueue(file)
		fmt.Println("Enqueuing ", file, " pending: ", pending.Load(), " items: ", !itemQ.IsEmpty())
		go func() {
			ctx := mon.ctx()
			for !itemQ.IsEmpty() && pending.CompareAndSwap(false, true) {
				time.Sleep(125 * time.Millisecond)
				files := u.NewSet[string]()
				// condense duplicates
				for file := range itemQ.Dequeue() {
					println("DEQUEUING FILE " + file)
					files.Add(file)
				}
				fmt.Println("CHECKING FILES: ", files)
				for _, rm := range ctx.monitorsTracking(files) {
					SvcSync(rm, func() (bool, error) {
						println("FILE ACTIVITY, READING FILE")
						if blocks, err := readBlocks(rm.Track); err != nil {
							fmt.Fprintf(os.Stderr, "Error reading file for monitor %s: %s", rm.DocId, rm.Track)
						} else {
							println("PATCHING")
							rm.BasicPatch(true, true, blocks...)
							rm.setBlockOrder(blocks)
						}
						return true, nil
					})
				}
				pending.Store(false)
			}
		}()
	}
	go func() {
		for {
			select {
			case e, ok := <-mon.Tracker.Events:
				if !ok {
					return
				} else if !e.Has(fsnotify.Write) && !e.Has(fsnotify.Rename) {
					continue
				} else if symfilename, err := filepath.EvalSymlinks(e.Name); err != nil {
					fmt.Fprintf(os.Stderr, "Error getting symlink path for file %s: %s\n", symfilename, err)
				} else if absfilename, err := filepath.Abs(symfilename); err != nil {
					fmt.Fprintf(os.Stderr, "Error getting absolute path for file %s: %s\n", absfilename, err)
				} else if e.Has(fsnotify.Rename) {
					fmt.Println("RENAMED: ", e.String())
					if rm := mon.monitorTracking(absfilename); rm != nil {
						SvcSync(rm, func() (bool, error) {
							rm.Track = ""
							return true, nil
						})
					}
				} else if e.Has(fsnotify.Write) {
					println("WRITTEN: ", e.Name)
					enqueue(absfilename)
				} else {
					println("HUH?")
				}
			case err, ok := <-mon.Tracker.Errors:
				if !ok {
					return
				}
				fmt.Fprintln(os.Stderr, err)
			}
		}
	}()
}

func handled(filename string) bool {
	return strings.HasSuffix(filename, ".json") ||
		strings.HasSuffix(filename, ".yml")
}

func readBlocks(filename string) ([]map[string]any, error) {
	var blocks []map[string]any
	var err error = nil
	var content []byte
	if content, err = os.ReadFile(filename); err != nil {
		return nil, err
	} else if len(content) == 0 {
		return nil, nil
	} else if strings.HasSuffix(filename, ".json") {
		err = json.Unmarshal(content, &blocks)
	} else if strings.HasSuffix(filename, ".yml") || strings.HasSuffix(filename, ".yaml") {
		fmt.Println("READING YAML: " + filename)
		err = yaml.Unmarshal(content, &blocks)
	} else if strings.HasSuffix(filename, ".org") {
		err = fmt.Errorf("Org-mode is not yet available")
	} else {
		err = fmt.Errorf("Unknown file format %s", filename)
	}
	if err != nil {
		return nil, err
	}
	return blocks, nil
}

func (rm *RemoteMonitor) Svc(code func(), wrap ...bool) {
	rm.Service.Svc(code, wrap...)
}

func (rm *RemoteMonitor) writeBlocks() error {
	var out []byte
	var err error
	println("WRITING FILE: ", rm.Track)
	blocks := make([]map[string]any, 0, len(rm.Blocks))
	for _, name := range rm.BlockOrder {
		if blk, ok := rm.Blocks[name]; ok {
			blocks = append(blocks, blk)
		}
	}
	if strings.HasSuffix(rm.Track, ".json") {
		out, err = json.Marshal(blocks)
	} else if strings.HasSuffix(rm.Track, ".yml") || strings.HasSuffix(rm.Track, ".yaml") {
		out, err = yaml.Marshal(blocks)
	} else if strings.HasSuffix(rm.Track, ".org") {
		return fmt.Errorf("Org-mode is not yet available")
	} else {
		return fmt.Errorf("File has unknown format: %s", rm.Track)
	}
	if err == nil {
		os.WriteFile(rm.Track, out, 0)
	}
	return err
}

func (mon *Monitoring) verbose(format string, args ...any) {
	mon.verboseN(1, format, args...)
}

func (mon *Monitoring) verboseN(n int, format string, args ...any) {
	if n <= mon.Verbose {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

func abort(format string, args ...any) {
	panic(fmt.Errorf(format, args...))
}

func MonWrap[T any](mon *Monitoring, code func() (T, error)) func() (T, error) {
	return func() (result T, err error) {
		defer func() {
			if e := frecovery(mon.Activity); e != nil {
				err = e
				mon.Shutdown()
			}
		}()
		result, err = code()
		return
	}
}

func RmWrap[T any](rm *RemoteMonitor, code func() (T, error)) func() (T, error) {
	return func() (result T, err error) {
		defer func() {
			if e := frecovery(rm.Activity); e != nil {
				err = e
				rm.Shutdown()
			}
		}()
		result, err = code()
		return
	}
}

func CheckSet(opts ...any) []string {
	var params []string
	for i := range len(opts) - 1 {
		k := opts[i]
		v := opts[i+1]
		if v == "" || v == 0 {
			params = append(params, k.(string))
		}
	}
	return params
}

func ReadRedisConf(conf string) (int, int, string, *tls.Config, error) {
	pass := ""
	port := 0
	tlsPort := 0
	caCert := ""
	cert := ""
	key := ""
	if f, err := os.ReadFile(conf); err != nil {
		return 0, 0, "", nil, err
	} else {
		for line := range strings.SplitSeq(string(f), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "requirepass") {
				pass = strings.Split(line, " ")[1]
			} else if strings.HasPrefix(line, "port") {
				port, err = strconv.Atoi(strings.Split(line, " ")[1])
			} else if strings.HasPrefix(line, "tls-port") {
				tlsPort, err = strconv.Atoi(strings.Split(line, " ")[1])
			} else if strings.HasPrefix(line, "tls-ca-cert-file") {
				caCert = strings.Split(line, " ")[1]
			} else if strings.HasPrefix(line, "tls-cert-file") {
				cert = strings.Split(line, " ")[1]
			} else if strings.HasPrefix(line, "tls-key-file") {
				key = strings.Split(line, " ")[1]
			}
			if err != nil {
				return 0, 0, "", nil, err
			}
		}
	}
	if key != "" || caCert != "" || cert != "" || tlsPort != 0 {
		params := CheckSet("key", key, "caCert", caCert, "cert", cert, "tlsPort", tlsPort)
		if len(params) > 0 {
			return 0, 0, "", nil, fmt.Errorf("Missing TLS parameters: %s", strings.Join(params, ", "))
		} else if port != 0 {
			return 0, 0, "", nil, fmt.Errorf("Both port and TLS paramaters specified")
		}
	}
	if certBin, err := tls.LoadX509KeyPair(cert, key); err != nil {
		return 0, 0, "", nil, fmt.Errorf("Could not read key pair from cert %s, key %s", cert, key)
	} else if caBin, err := os.ReadFile(caCert); err != nil {
		return 0, 0, "", nil, fmt.Errorf("Error reading CA cert file: %s", caCert)
	} else if certPool := x509.NewCertPool(); !certPool.AppendCertsFromPEM(caBin) {
		return port, tlsPort, "", nil, fmt.Errorf("Could not add CA to cert pool")
	} else {
		return port, tlsPort, pass, &tls.Config{
			Certificates:       []tls.Certificate{certBin},
			RootCAs:            certPool,
			InsecureSkipVerify: true,
		}, nil
	}
}

func (cnf *RedisConf) Read() (string, *tls.Config, error) {
	port, tlsPort, pass, tls, err := ReadRedisConf(cnf.Conf)
	host := cnf.Host
	if host == "" {
		host = "localhost"
	}
	if pass != "" {
		pass = ":" + pass
	}
	if cnf.User != "" || pass != "" {
		host = "@" + host
	}
	if port == 0 {
		port = tlsPort
	}
	if port == 0 {
		port = 6739
	}
	conStr := fmt.Sprintf("redis://%s%s%s:%d/%d", cnf.User, pass, host, port, cnf.Db)
	fmt.Println("REDIS CON: ", conStr)
	return conStr, tls, err
}

type streamId struct {
	stream string
	id     string
}

// watch REDIS streams for changes
func (mon *Monitoring) WatchRedis(rate time.Duration) {
	go func() {
		ctx := context.Background()
		mct := mon.ctx()
		first := true
		streamsArg := make([]string, 0, 10)
		count := 0
		for {
			monitors := mct.monitors()
			streamsArg = mon.currentStreamsArg(monitors, streamsArg[:0])
			mon.verboseN(2, "USING STREAMS %v", streamsArg)
			if len(streamsArg) == 0 {
				time.Sleep(rate)
				continue
			}
			if first {
				first = false
				mon.verbose("WATCHING TOPICS: %v", streamsArg)
			}
			cmd := mon.redis.XRead(ctx, &redis.XReadArgs{
				Streams: streamsArg,
				Block:   rate,
			})
			var err error
			var results []redis.XStream
			resultChan := make(chan bool)
			go func() {
				results, err = cmd.Result()
				resultChan <- true
			}()
			select {
			case <-resultChan:
				if err != nil && err != redis.Nil {
					panic(fmt.Sprintf("Couldn't read from REDIS: %#v\n XREAD BLOCK %v STREAMS %#v", err, rate, streamsArg))
				}
			case <-mon.StreamUpdateChannel:
				// streams changed, do another update
				first = true
				continue
			}
			if err == redis.Nil {
				count++
				if count%100 == 0 {
					mon.verbose("%d stream queries", count)
				}
				continue
			} else if err != nil {
				panic(fmt.Sprintf("Couldn't read from REDIS: %#v", err))
			}
			mon.dispatchStreamResults(monitors, results)
		}
	}()
}

func (mon *Monitoring) currentStreamsArg(monitors []*RemoteMonitor, streamsArg []string) []string {
	mon.verboseN(3, "getting streams for %d monitors", len(monitors))
	eachTop := Collect(u.SliceSeq(monitors), func(rm *RemoteMonitor, yield func(s streamId) bool) {
		for stream := range rm.AllStreams {
			mon.verboseN(3, "FOUND STREAM %s", stream)
			if rm.StreamIDs[stream] == "" {
				rm.StreamIDs[stream] = "0"
			}
			if !yield(streamId{stream, rm.StreamIDs[stream]}) {
				break
			}
		}
	})
	ids := make([]string, 0, len(monitors)*2)
	strs := u.NewSet[string]()
	for sid := range eachTop {
		if !strs.Has(sid.stream) {
			strs.Add(sid.stream)
			streamsArg = append(streamsArg, sid.stream)
			ids = append(ids, sid.id)
		}
	}
	streamsArg = append(streamsArg, ids...)
	return streamsArg
}

func (mon *Monitoring) dispatchStreamResults(monitors []*RemoteMonitor, results []redis.XStream) {
	mon.verbose("Got stream results from REDIS: %#v", results)
	chosen := u.NewSet[string]()
	// index monitors by stream ID
	for _, str := range results {
		for _, rm := range monitors {
			rm.Svc(func() {
				startSer := rm.Serial
				if rm.AllStreams.Has(str.Stream) {
					chosen.Add(str.Stream)
					rm.StreamIDs[str.Stream] = str.Messages[len(str.Messages)-1].ID
					for _, msg := range str.Messages {
						var obj any
						if msg.Values["sender"] == rm.PeerId {
							continue
						} else if snd, isString := msg.Values["sender"]; isString {
							mon.verbose("RECEIVED MESSAGE\n  FROM %s\n  THIS: %s", snd, rm.PeerId)
						}
						str := msg.Values["batch"]
						if err := json.Unmarshal([]byte(str.(string)), &obj); err != nil {
							abort("Could not parse JSON for message %#v", str)
						} else if m, ok := obj.([]any); !ok {
							abort("batch is not an array: %#v", obj)
						} else {
							for _, block := range m {
								if dict, ok := block.(map[string]any); !ok {
									abort("batch item is not a map[string]any: %#v", obj)
								} else if err := rm.HandleBlock(dict); err != nil {
									abort("%w", err)
								}
							}
						}
					}
				}
				if !rm.Changed.IsEmpty() {
					rm.notifyChanged()
				}
				// check if any incoming blocks were stored
				if startSer < rm.Serial {
					for timer := range rm.UpdateTimers {
						timer.Reset(time.Microsecond)
					}
					if rm.Track != "" {
						rm.writeBlocks()
					}
				}
				rm.ComputeTopics()
			})
		}
	}
}

func (mon *Monitoring) Shutdown() { mon.Service.Shutdown() }

func (mon *Monitoring) ShutdownService(s *ChanSvc) {
	mon.verbose("SHUTTING DOWN MONITORING SERVICE")
	mon.redis.Close()
	mon.Service = nil
	for _, rm := range mon.Monitors {
		rm.Shutdown()
	}
}

func (mon *Monitoring) getMonitor(r *http.Request) *RemoteMonitor {
	docId := r.PathValue("documentId")
	rm, _ := UnwrappedSvcSync(mon.Service, func() (*RemoteMonitor, error) {
		rm, _ := mon.Monitors[docId]
		return rm, nil
	})
	if rm == nil {
		panic("there is no document named " + docId)
	}
	return rm
}

func (mon *Monitoring) getMonitorAndTrack(r *http.Request) (*RemoteMonitor, string) {
	docId := r.PathValue("documentId")
	var track string
	rm, _ := UnwrappedSvcSync(mon.Service, func() (*RemoteMonitor, error) {
		rm, _ := mon.Monitors[docId]
		track = rm.Track
		return rm, nil
	})
	if rm == nil {
		panic("there is no document named " + docId)
	}
	return rm, track
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	w.Write([]byte(msg))
}

func recovery(w http.ResponseWriter, activity string) {
	if err := recover(); err != nil {
		if activity == err {
			fmt.Fprintf(os.Stderr, "Error %s\n", activity)
		} else {
			fmt.Fprintf(os.Stderr, "Error %s: %s\n", activity, err)
		}
		debug.PrintStack()
		writeError(w, http.StatusBadRequest, "Error "+activity)
	}
}

func frecovery(activity string) error {
	if rerr := recover(); rerr != nil {
		err, ok := rerr.(error)
		if !ok {
			err = fmt.Errorf("%v", rerr)
		}
		fmt.Fprintf(os.Stderr, "Error %s: %v\n", activity, err)
		debug.PrintStack()
		return err
	}
	return nil
}

// clear all streams
func (mon *Monitoring) httpClear(w http.ResponseWriter, _ *http.Request) {
	bg := context.Background()
	first := true
	streams := make([]string, 0, 10)
	for cursor := uint64(0); cursor != 0 || first; {
		first = false
		cmd := mon.redis.ScanType(bg, cursor, "*", 1000, "stream")
		if keys, cur, err := cmd.Result(); err == redis.Nil {
			break
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting streams: %v\n", err)
			os.Exit(1)
		} else {
			cursor = cur
			for _, key := range keys {
				streams = append(streams, key)
			}
		}
	}
	for _, stream := range streams {
		if strings.HasPrefix(stream, TOPIC_PREFIX) {
			mon.redis.XTrimMaxLen(bg, stream, 0)
		}
	}
	w.WriteHeader(http.StatusOK)
}

// pring all stream data
func (mon *Monitoring) httpDump(w http.ResponseWriter, _ *http.Request) {
	result := mon.dump()
	if out, err := json.Marshal(result); err != nil {
		panic("Could not convert stream data to JSON")
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write(out)
	}
}

func (mon *Monitoring) dump() any {
	bg := context.Background()
	result := make([][]any, 0, 10)
	first := true
	for cursor := uint64(0); cursor != 0 || first; {
		first = false
		cmd := mon.redis.ScanType(bg, cursor, "*", 1000, "stream")
		if keys, cur, err := cmd.Result(); err == redis.Nil {
			break
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting streams: %v\n", err)
			os.Exit(1)
		} else {
			cursor = cur
			streams := make([]string, 0, 10)
			for _, key := range keys {
				if strings.HasPrefix(key, TOPIC_PREFIX) {
					streams = append(streams, key)
				} else {
					mon.verbose("SKIPPING STREAM %s", key)
				}
			}
			for i := len(streams); i > 0; i-- {
				streams = append(streams, "0")
			}
			cmd := mon.redis.XRead(bg, &redis.XReadArgs{
				Streams: streams,
				Block:   1 * time.Millisecond,
			})
			if results, err := cmd.Result(); err == redis.Nil {
				continue
			} else if err != nil {
				panic(fmt.Sprintf("Couldn't read from REDIS: %#v", err))
			} else {
				for _, str := range results {
					res := make([]any, 0, len(str.Messages)+1)
					res = append(res, str.Stream)
					for _, msg := range str.Messages {
						var inp any
						var inb any
						if p, okp := msg.Values["sender"]; okp {
							inp = p
						}
						if b, okb := msg.Values["batch"]; okb {
							if err := json.Unmarshal([]byte(b.(string)), &inb); err != nil {
								panic(fmt.Sprintf("Couldn't decode REDIS batch: %s", b.(string)))
							}
						}
						res = append(res, []any{inp, inb})
					}
					result = append(result, res)
				}
			}
		}
	}
	return result
}

func (mon *Monitoring) httpList(w http.ResponseWriter, r *http.Request) {
	docs := make([]string, 0, len(mon.Monitors))
	for _, rm := range mon.ctx().monitors() {
		docs = append(docs, rm.DocId)
	}
	if enc, err := json.Marshal(docs); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Error JSON encoding %#v", docs))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write(enc)
	}
}

func (mon *Monitoring) httpAdd(w http.ResponseWriter, r *http.Request) {
	docId := r.PathValue("documentId")
	_, err := SvcSync(mon, func() (bool, error) {
		_, err := mon.Add(docId)
		return true, err
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "There is already a document named "+docId)
	} else {
		mon.verbose("Add document %s", docId)
		mon.httpPatch(w, r)
	}
}

func (mon *Monitoring) httpAddFile(w http.ResponseWriter, r *http.Request) {
	docId := r.PathValue("documentId")
	rm, err := SvcSync(mon, func() (*RemoteMonitor, error) {
		return mon.Add(docId)
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "There is already a document named "+docId)
	} else if fn := r.PathValue("file"); fn == "" {
		writeError(w, http.StatusBadRequest, "No file provided")
	} else if filename, err := url.PathUnescape(fn); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Could not unescape filename: %s", fn))
	} else if err := rm.trackFile(filename); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Error tracking file %s: %v", filename, err))
	} else {
		mon.verbose("Add document %s", docId)
		mon.httpPatchFile(w, r)
	}
}

func (mon *Monitoring) Add(docId string) (*RemoteMonitor, error) {
	if mon.Monitors[docId] != nil {
		return nil, fmt.Errorf("%w: there is already a document named %s", ErrDataMismatch, docId)
	}
	rm := &RemoteMonitor{
		PeerId:           fmt.Sprint(uuid.New()),
		Monitoring:       mon,
		DocId:            docId,
		Blocks:           make(map[string]map[string]any),
		StreamSerials:    make(map[string]int64),
		IncomingUpdate:   atomic.Int64{},
		ProcessedUpdate:  atomic.Int64{},
		ProcessingUpdate: atomic.Bool{},
		DefaultStream:    TOPIC_PREFIX + docId,
		AllStreams:       u.NewSet(TOPIC_PREFIX + docId),
		TopicMap:         map[string]string{docId: TOPIC_PREFIX + docId},
		StreamIDs:        make(map[string]string),
		UpdateTimers:     make(u.Set[*time.Timer]),
		Listeners:        make(u.Set[MonitorListener]),
		Changed:          &u.ConcurrentQueue[map[string]any]{},
	}
	rm.Service = NewSvc(rm)
	mon.Monitors[docId] = rm
	return rm, nil
}

func (mon *Monitoring) httpRemove(w http.ResponseWriter, r *http.Request) {
	activity := "getting monitor connection"
	defer func() { recovery(w, activity) }()
	mon.getMonitor(r).Shutdown()
}

func (rm *RemoteMonitor) AddListener(l MonitorListener) {
	rm.Listeners.Add(l)
}

func (rm *RemoteMonitor) RemoveListener(l MonitorListener) {
	rm.Listeners.Remove(l)
}

func (rm *RemoteMonitor) notifyChanged() {
	for l := range rm.Listeners {
		l.DataChanged(rm)
	}
}

func (rm *RemoteMonitor) Shutdown() { rm.Service.Shutdown() }

func (rm *RemoteMonitor) ShutdownService(c *ChanSvc) {
	rm.Monitoring.Svc(func() {
		if rm.Track != "" {
			rm.Monitoring.removeFile(rm.Track)
			rm.Track = ""
		}
		delete(rm.Monitoring.Monitors, rm.DocId)
		rm.verbose("SHUTTING DOWN MONITOR")
		rm.verbose("DELETING MONITOR")
		rm.Blocks = nil
		rm.StreamSerials = nil
		rm.Dead = nil
		rm.AllStreams = u.Set[string]{}
		rm.Monitoring = nil
	})
}

func (rm *RemoteMonitor) HandleBlock(block map[string]any) error {
	rm.verbose("Handling block: %#v\n", block)
	switch block["type"] {
	case "data", "code", "monitor":
		return rm.BasicPatch(false, true, block)
	case "delete":
		switch names := block["value"].(type) {
		case string:
			return rm.deleteIncomingBlocks(block, names)
		case []string:
			return rm.deleteIncomingBlocks(block, names...)
		case []any:
			strs := make([]string, len(names))
			for i, str := range names {
				if s, ok := str.(string); ok {
					strs[i] = s
				} else {
					return fmt.Errorf("delete block has bad value: %#v", block["value"])
				}
			}
			block["value"] = strs
			return rm.deleteIncomingBlocks(block, strs...)
		default:
			return fmt.Errorf("delete block has bad value: %#v", block["value"])
		}
	case "":
		return fmt.Errorf("missing block type")
	default:
		return fmt.Errorf("unknown block type: %s", block["type"])
	}
}

func (mon *Monitoring) httpGet(w http.ResponseWriter, r *http.Request) {
	activity := "getting monitor connection"
	defer func() { recovery(w, activity) }()
	rm := mon.getMonitor(r)
	if bytes, err := json.Marshal(rm.Blocks); err != nil {
		errstr := fmt.Sprint(fmt.Errorf("Error encoding blocks %#v: %w", rm.Blocks, err))
		writeError(w, http.StatusBadRequest, errstr)
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write(bytes)
	}
}

func (mon *Monitoring) httpGetBlock(w http.ResponseWriter, r *http.Request) {
	activity := "getting monitor connection"
	defer func() { recovery(w, activity) }()
	rm := mon.getMonitor(r)
	name := r.PathValue("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "Empty block name")
	} else {
		SvcSync(rm, func() (bool, error) {
			if block, ok := rm.Blocks[name]; !ok {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("No block named %s", name))
			} else if bytes, err := json.Marshal(block); err != nil {
				errstr := fmt.Sprint(fmt.Errorf("Error encoding block %s = %#v: %w", name, block, err))
				writeError(w, http.StatusBadRequest, errstr)
			} else {
				w.WriteHeader(http.StatusOK)
				w.Write(bytes)
			}
			return true, nil
		})
	}
}

func (mon *Monitoring) httpPatch(w http.ResponseWriter, r *http.Request) {
	activity := "getting monitor connection"
	defer func() { recovery(w, activity) }()
	rm := mon.getMonitor(r)
	var blocks []map[string]any
	if content, err := io.ReadAll(r.Body); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Error reading document content, expected array of blocks with names but got error: %v", err))
	} else if len(content) == 0 {
		w.WriteHeader(http.StatusOK)
	} else if err := json.Unmarshal(content, &blocks); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Error reading document content, expected array of blocks with names but got %s", content))
	} else if err := rm.Patch(true, blocks...); err != nil {
		writeError(w, http.StatusBadRequest, "Error sending blocks")
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (mon *Monitoring) httpPatchFile(w http.ResponseWriter, r *http.Request) {
	activity := "getting monitor connection"
	defer func() { recovery(w, activity) }()
	rm := mon.getMonitor(r)
	if fn := r.PathValue("file"); fn == "" {
		writeError(w, http.StatusBadRequest, "No file provided")
	} else if filename, err := url.PathUnescape(fn); err != nil {
		writeError(w, http.StatusBadRequest, "Error unescaping filename: "+fn)
	} else if blocks, err := readBlocks(filename); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Error reading file %s: %v", filename, err))
	} else if err := rm.Patch(true, blocks...); err != nil {
		writeError(w, http.StatusBadRequest, "Error sending blocks")
	} else {
		rm.setBlockOrder(blocks)
		w.WriteHeader(http.StatusOK)
	}
}

func CopyBlock(block map[string]any) map[string]any {
	cpy := make(map[string]any, len(block))
	maps.Copy(cpy, block)
	return cpy
}

func (rm *RemoteMonitor) setBlockOrder(blocks []map[string]any) {
	blockOrder := make([]string, 0, len(blocks))
	for _, blk := range blocks {
		name, blockName := blk["name"]
		nameStr, isString := name.(string)
		if _, patchName := rm.Blocks[nameStr]; blockName && isString && patchName {
			blockOrder = append(blockOrder, nameStr)
		}
	}
	rm.BlockOrder = blockOrder
}

func (rm *RemoteMonitor) Patch(send bool, blocks ...map[string]any) error {
	_, err := WrappedSvcSync(rm.Service, func() (bool, error) {
		return true, rm.BasicPatch(send, true, blocks...)
	})
	return err
}

// if any blocks in the list have changed, record and publish them
func (rm *RemoteMonitor) BasicPatch(send, change bool, blocks ...map[string]any) error {
	rm.verbose("Patch (send = %v) %s with %#v", send, rm.DocId, blocks)
	count := 0
	for i, block := range blocks {
		if name, ok := block["name"].(string); !ok {
			return fmt.Errorf("%w Block %d has a bad name: %#v", ErrBadBlock, i, block)
		} else {
			if rm.Blocks[name] != nil {
				if old, e1 := json.Marshal(rm.Blocks[name]); e1 != nil {
					return e1
				} else if new, e2 := json.Marshal(block); e2 != nil {
					return e2
				} else if string(old) == string(new) {
					continue
				}
			}
			if count < i {
				blocks[count] = block
			}
			count++
			fmt.Println("CHANGED:", block)
			rm.StreamSerials[name] = rm.Serial
			rm.verbose("@@@\n@@@ RECEIVED BLOCK %#v\n@@@", block)
			rm.Blocks[name] = block
			if change {
				rm.Changed.Enqueue(block)
				rm.notifyChanged()
			}
		}
	}
	if count > 0 {
		rm.Serial++
		rm.ComputeTopics()
		if send {
			fmt.Println("SENDING:", blocks[:count])
			rm.Send(rm.Service.invocation, blocks[:count]...)
		}
	}
	return nil
}

func (rm *RemoteMonitor) ComputeTopics() {
	old := rm.AllStreams
	rm.AllStreams = make(u.Set[string])
	rm.addTopic(rm.DefaultStream)
	// a fingertree could maintain this automatically
	for _, block := range rm.Blocks {
		for topic := range u.PropStrings(block["topics"]) {
			rm.addTopic(topic)
		}
		for topic := range u.PropStrings(block["updatetopics"]) {
			rm.addTopic(topic)
		}
	}
	if len(rm.AllStreams) != len(old) || !rm.AllStreams.Contains(old) {
		rm.verbose("CHANGING WATCHED TOPICS: %v", rm.AllStreams)
		// redo the current update if streams changed
		go func() { rm.StreamUpdateChannel <- true }()
	}
}

func (rm *RemoteMonitor) addTopic(topic string) {
	stream := topic
	if topic != rm.DefaultStream {
		stream = rm.DefaultStream + "-" + topic
	}
	rm.TopicMap[topic] = stream
	rm.AllStreams.Add(stream)
}

func (mon *Monitoring) httpDelete(w http.ResponseWriter, r *http.Request) {
	activity := "getting monitor connection"
	defer func() { recovery(w, activity) }()
	if rm := mon.getMonitor(r); rm == nil {
		return
	} else {
		activity = "sending deletes"
		err := rm.DeleteBlocks(true, r.PathValue("name"))
		if err != nil {
			panic(activity)
		}
	}
}

func (mon *Monitoring) httpDeletePost(w http.ResponseWriter, r *http.Request) {
	activity := "getting monitor connection"
	defer func() { recovery(w, activity) }()
	rm := mon.getMonitor(r)
	if rm == nil {
		return
	}
	activity = "reading document content, expected array of names"
	content, err := io.ReadAll(r.Body)
	if err != nil {
		panic(activity)
	}
	var obj JsonObj
	json.Unmarshal(content, &obj)
	if !obj.IsArray() {
		panic(activity)
	}
	names := make([]string, 0, obj.Len())
	for i := obj.Len() - 1; i >= 0; i-- {
		if s := obj.GetJson(i); !s.IsString() {
			panic(activity)
		} else {
			names = append(names, s.AsString())
		}
	}
	activity = "sending deletes"
	err = rm.DeleteBlocks(true, names...)
	if err != nil {
		panic(activity)
	}
}

func (rm *RemoteMonitor) DeleteBlocks(send bool, names ...string) error {
	_, err := WrappedSvcSync(rm.Service, func() (bool, error) {
		_, err := rm.basicDeleteBlocks(send, names...)
		return true, err
	})
	return err
}

func (rm *RemoteMonitor) deleteIncomingBlocks(block map[string]any, names ...string) error {
	names, err := rm.basicDeleteBlocks(false, names...)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return nil
	}
	if bnames, isArray := block["value"].([]string); isArray && len(names) != len(bnames) {
		newBlock := make(map[string]any, len(block))
		maps.Copy(newBlock, block)
		newBlock["value"] = names
		block = newBlock
	}
	rm.Changed.Enqueue(block)
	rm.notifyChanged()
	return nil
}

func (rm *RemoteMonitor) basicDeleteBlocks(send bool, names ...string) ([]string, error) {
	pos := 0
	i := rm.Service.invocation
	deadCount := len(rm.Dead)
	for i, name := range names {
		if rm.Blocks[name] != nil {
			rm.Dead[name] = rm.Serial
			delete(rm.Blocks, name)
			pos++
		} else {
			if pos < i {
				names[pos] = names[i]
			}
		}
	}
	if deadCount < len(rm.Dead) {
		rm.Serial++
	}
	rm.ComputeTopics()
	names = names[0:pos]
	if len(names) == 0 {
		return names, nil
	}
	if send {
		rm.Send(i, JMap("type", "delete", "value", names))
	}
	return names, nil
}

func (mon *Monitoring) httpUpdate(w http.ResponseWriter, r *http.Request) {
	activity := "getting monitor connection"
	defer func() { recovery(w, activity) }()
	rm := mon.getMonitor(r)
	if rm == nil {
		return
	}
	activity = "getting serial value"
	nodata := strings.ToLower(r.URL.Query().Get("nodata")) == "true"
	serial := int64(0)
	if serStr := r.PathValue("serial"); serStr != "" {
		if _, err := fmt.Sscanf(serStr, "%d", &serial); err != nil {
			panic(activity + " " + serStr)
		}
	}
	activity = "getting timeout value"
	timeout := float64(0)
	if timeoutStr := r.URL.Query().Get("timeout"); timeoutStr != "" {
		if _, err := fmt.Sscanf(timeoutStr, "%f", &timeout); err != nil {
			panic(activity + " " + timeoutStr)
		}
	}
	activity = "getting updates"
	rmSerial, result, deleted := rm.GetUpdates(serial, timeout, nodata)
	w.WriteHeader(http.StatusOK)
	if nodata {
		fmt.Fprint(w, rmSerial)
	} else if result != nil {
		activity = "encoding update blocks"
		encoded, err := json.Marshal(JMap("serial", rmSerial, "blocks", result, "deleted", deleted))
		if err != nil {
			panic(activity)
		}
		w.Write(encoded)
	} else {
		w.Write([]byte("null"))
	}
}

func (mon *Monitoring) httpTrack(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("TRACK: %#v\n", r)
	os.Exit(1)
	activity := "attempting to track file"
	defer func() { recovery(w, activity) }()
	rm, track := mon.getMonitorAndTrack(r)
	if fn := r.PathValue("file"); fn == "" {
		var current any = nil
		if track != "" {
			current = track
		}
		if encoded, err := json.Marshal(current); err != nil {
			panic("could not marshal document tracking filename")
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write(encoded)
		}
	} else if filename, err := url.PathUnescape(fn); err != nil {
		panic("Could not unescape filename: " + fn)
	} else if !handled(filename) {
		panic("File is in an unhandled format (not *.json, *.yml, or *.yaml): " + filename)
	} else {
		SvcSync(rm, func() (bool, error) {
			if err := rm.trackFile(filename); err != nil {
				panic(fmt.Sprint(err))
			} else {
				w.WriteHeader(http.StatusOK)
			}
			return true, nil
		})
	}
}

func (rm *RemoteMonitor) trackFile(filename string) error {
	if symfilename, err := filepath.EvalSymlinks(filename); err != nil {
		return err
	} else if absfilename, err := filepath.Abs(symfilename); err != nil {
		return err
	} else if rm.Track != "" {
		return fmt.Errorf("Already tracking %s", rm.Track)
	} else if err := rm.Monitoring.addFile(absfilename); err != nil {
		return err
	} else {
		rm.Track = absfilename
		return nil
	}
}

func (mon *Monitoring) httpNoTrack(w http.ResponseWriter, r *http.Request) {
	activity := "attempting to track file"
	defer func() { recovery(w, activity) }()
	if _, track := mon.getMonitorAndTrack(r); track == "" {
		panic("Not tracking a file for document: " + r.PathValue("documentId"))
	} else {
		track = ""
		if err := mon.Tracker.Remove(track); err != nil {
			panic(err)
		}
		w.WriteHeader(http.StatusOK)
	}
}

func (rm *RemoteMonitor) GetUpdates(serial int64, timeout float64, nodata bool) (int64, map[string]any, []string) {
	var result map[string]any
	var dead []string
	var timer *time.Timer
	if timeout > 0 {
		// if a timer is required, add it to rm.UpdateTimers so it can be killed from outside
		UnwrappedSvcSync(rm.Service, func() (bool, error) {
			if rm.Serial <= serial {
				timer = time.NewTimer(time.Duration(timeout*1000) * time.Millisecond)
				rm.UpdateTimers.Add(timer)
			}
			return true, nil
		})
		if timer != nil {
			// wait for timeout or close
			<-timer.C
			UnwrappedSvcSync(rm.Service, func() (bool, error) {
				delete(rm.UpdateTimers, timer)
				return true, nil
			})
		}
	}
	WrappedSvcSync(rm.Service, func() (bool, error) {
		if !nodata {
			result, dead = rm.gatherUpdates(serial)
		}
		serial = rm.Serial
		return true, nil
	})
	return serial, result, dead
}

func (rm *RemoteMonitor) gatherUpdates(serial int64) (map[string]any, []string) {
	var result map[string]any
	var dead []string
	for name, ser := range rm.StreamSerials {
		if ser <= serial {
			continue
		}
		if result == nil {
			result = JMap(name, rm.Blocks[name])
		} else {
			result[name] = rm.Blocks[name]
		}
	}
	for name, ser := range rm.Dead {
		if ser > serial {
			if dead == nil {
				dead = make([]string, 0)
			}
			dead = append(dead, name)
		}
	}
	return result, dead
}

func (rm *RemoteMonitor) Send(i uint64, blocks ...map[string]any) {
	rm.Service.Verify(i)
	if len(blocks) == 0 {
		return
	}
	defaultTopics := []string{rm.DefaultStream}
	topics := []string{rm.DefaultStream}
	start := 0
	ctx := context.Background()
	sendOnTopic := func(topic string, blocks any) {
		blocksStr, err := json.Marshal(blocks)
		if err != nil {
			abort("Error sending to topic %s, could not get JSON for blocks: %#s\n", topic, blocks)
		}
		rm.verbose("SENDING TO '%s' %#v\n  FROM: %s", topic, blocks, rm.PeerId)
		_, err = rm.redis.XAdd(ctx, &redis.XAddArgs{
			Stream:     topic,
			NoMkStream: false,
			ID:         "*",
			Values:     []any{"batch", blocksStr, "sender", rm.PeerId},
		}).Result()
		if err != redis.Nil && err != nil {
			abort("Error sending blocks to '%s': %v", topic, err)
		}
	}
	sendBlocks := func(i int) {
		if i > start {
			var b any = blocks[start:i]
			start = i
			for _, tp := range topics {
				sendOnTopic(tp, b)
			}
		}
	}
	for i, block := range blocks {
		t := u.EnsliceStrings(block["topics"])
		if len(t) == 0 || (len(t) == 1 && t[0] == "") {
			t = defaultTopics
		} else {
			if _, isStrings := block["topics"].([]string); isStrings {
				newT := make([]string, 0, len(t))
				copy(newT, t)
				t = newT
			}
			for i, topic := range t {
				t[i] = rm.DefaultStream + "-" + topic
			}
		}
		rm.verbose("SENDING ON TOPICS: %#v", t)
		if !rm.sameTopics(t, topics) {
			sendBlocks(i)
			topics = t
		}
	}
	sendBlocks(len(blocks))
}

func (rm *RemoteMonitor) sameTopics(a, b []string) bool {
	if a == nil {
		return b == nil || (len(b) == 1 && b[0] == rm.DefaultStream)
	} else if b == nil {
		return len(a) == 1 && a[0] == rm.DefaultStream
	} else if len(a) != len(b) {
		return false
	}
	items := u.NewSet(a...)
	for _, sB := range b {
		if !items.Has(sB) {
			return false
		}
	}
	return true
}

func (mon *Monitoring) handle(mux *http.ServeMux, pat string, f func(w http.ResponseWriter, r *http.Request)) {
	mux.HandleFunc(pat, func(w http.ResponseWriter, r *http.Request) {
		mon.verbose("Request: %s %v", r.Method, r.URL)
		f(w, r)
	})
}

func (mon *Monitoring) InitMux(mux *http.ServeMux) {
	mon.handle(mux, LIST, mon.httpList)
	mon.handle(mux, ADD, mon.httpAdd)
	mon.handle(mux, ADD_FILE, mon.httpAddFile)
	mon.handle(mux, REMOVE, mon.httpRemove)
	mon.handle(mux, GET, mon.httpGet)
	mon.handle(mux, GET_BLOCK, mon.httpGetBlock)
	mon.handle(mux, PATCH, mon.httpPatch)
	mon.handle(mux, DELETE, mon.httpDelete)
	mon.handle(mux, DELETE_POST, mon.httpDelete)
	mon.handle(mux, UPDATE, mon.httpUpdate)
	mon.handle(mux, TRACK, mon.httpTrack)
	mon.handle(mux, TRACK_FILE, mon.httpTrack)
	mon.handle(mux, NOTRACK, mon.httpNoTrack)
	mon.handle(mux, CLEAR, mon.httpClear)
	mon.handle(mux, DUMP, mon.httpDump)
}
