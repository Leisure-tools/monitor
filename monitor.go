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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	TOPIC_PREFIX = "MONITOR-"
	//TOPIC_PREFIX         = " MONITOR "
	URL_PREFIX           = "/monitor/v1/document"
	LIST                 = "GET " + URL_PREFIX + "/list"
	ADD                  = "POST " + URL_PREFIX + "/{documentId}"
	PATCH                = "PATCH " + URL_PREFIX + "/{documentId}"
	GET                  = "GET " + URL_PREFIX + "/{documentId}"
	GET_BLOCK            = "GET " + URL_PREFIX + "/{documentId}/{name}"
	REMOVE               = "GET " + URL_PREFIX + "/{documentId}/remove"
	DELETE               = "GET " + URL_PREFIX + "/{documentId}/delete/{name}"
	DELETE_POST          = "POST " + URL_PREFIX + "/{documentId}/delete"
	UPDATE               = "GET " + URL_PREFIX + "/{documentId}/update/{serial}"
	TRACK                = "GET " + URL_PREFIX + "/{documentId}/track"
	TRACK_FILE           = "GET " + URL_PREFIX + "/{documentId}/track/{file}"
	NOTRACK              = "GET " + URL_PREFIX + "/{documentId}/notrack"
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
	redis    *redis.Client
	Monitors map[string]*RemoteMonitor // document id -> remote monitor
	Service  *ChanSvc
	Activity string // current service activity
	Verbose  int
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
	DefaultTopic     string
	AllTopics        Set[string]
	TopicIDs         map[string]string
	Serial           int64 // current update number for the connection
	UpdateTimers     Set[*time.Timer]
	Listeners        Set[MonitorListener]
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

var MON_PAT, _ = regexp.Compile("(redis://)?((?<user>[^:]+)(:(?<pass>[^:]+))?@)?((?<host>[^:]+):)?(?<port>[0-9]+)?(/(?<db>[0-9+]))?$")
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

func New(conStr string, verbose int) (*Monitoring, error) {
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
		db := match("db", "0")
		var dbNum int
		fmt.Sscanf(db, "%d", &dbNum)
		mon := &Monitoring{
			Monitors: make(map[string]*RemoteMonitor),
			redis: redis.NewClient(&redis.Options{
				Username: match("user", ""),
				Password: match("pass", ""),
				Addr:     fmt.Sprint(host, ":", port),
				DB:       dbNum,
			}),
			Verbose: verbose,
		}
		mon.Service = NewSvc(mon)
		mon.verbose("Connecting to REDIS: %s", conStr)
		mon.Watch(DEFAULT_REDIS_MILLIS)
		return mon, nil
	}
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

// watch REDIS streams for changes
func (mon *Monitoring) Watch(rate time.Duration) {
	ctx := context.Background()
	go func() {
		var streamsArg []string
		for true {
			WrappedSvcSync(mon.Service, func() (bool, error) {
				topics := 0
				for _, rm := range mon.Monitors {
					topics += len(rm.AllTopics)
				}
				pos := 0
				streamsArg = make([]string, topics*2)
				for _, rm := range mon.Monitors {
					for topic := range rm.AllTopics {
						streamsArg[pos] = topic
						if id := rm.TopicIDs[topic]; id != "" {
							streamsArg[pos+topics] = id
						} else {
							rm.TopicIDs[topic] = "0"
							streamsArg[pos+topics] = "0"
						}
						pos++
					}
				}
				return true, nil
			})
			if len(streamsArg) == 0 {
				time.Sleep(rate)
				continue
			}
			cmd := mon.redis.XRead(ctx, &redis.XReadArgs{
				Streams: streamsArg,
				Block:   rate,
			})
			results, err := cmd.Result()
			if err == redis.Nil {
				continue
			} else if err != nil {
				panic(fmt.Sprintf("Couldn't read from REDIS: %#v", err))
			}
			mon.verbose("Got stream results from REDIS:: %#v", results)
			// index monitors by stream ID
			for _, str := range results {
				for _, rm := range mon.Monitors {
					WrappedSvcSync(rm.Service, func() (bool, error) {
						startSer := rm.Serial
						if rm.AllTopics[str.Stream] {
							rm.TopicIDs[str.Stream] = str.Messages[len(str.Messages)-1].ID
							for _, msg := range str.Messages {
								var obj any
								if msg.Values["sender"] == rm.PeerId {
									continue
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
										} else {
											if err := rm.HandleBlock(dict); err != nil {
												abort("%w", err)
											}
										}
									}
								}
							}
						}
						if startSer < rm.Serial {
							for timer := range rm.UpdateTimers {
								timer.Reset(time.Microsecond)
							}
						}
						return true, nil
					})
				}
			}
		}
	}()
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
		return mon.Monitors[docId], nil
	})
	if rm == nil {
		panic("there is no document named " + docId)
	}
	return rm
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

func (mon *Monitoring) httpList(w http.ResponseWriter, r *http.Request) {
	docs := make([]string, 0, len(mon.Monitors))
	for name := range mon.Monitors {
		docs = append(docs, name)
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
	if _, err := mon.Add(docId); err != nil {
		writeError(w, http.StatusBadRequest, "There is already a document named "+docId)
	} else {
		mon.verbose("Add document %s", docId)
		mon.httpPatch(w, r)
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
		DefaultTopic:     TOPIC_PREFIX + docId,
		AllTopics:        Set[string]{TOPIC_PREFIX + docId: true},
		TopicIDs:         make(map[string]string),
		UpdateTimers:     make(Set[*time.Timer]),
		Listeners:        make(Set[MonitorListener]),
	}
	rm.Service = NewSvc(mon)
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

func (rm *RemoteMonitor) Shutdown() { rm.Service.Shutdown() }

func (rm *RemoteMonitor) ShutdownService(s *ChanSvc) {
	rm.verbose("SHUTTING DOWN MONITOR")
	rm.verbose("DELETING MONITOR")
	delete(rm.Monitoring.Monitors, rm.DocId)
	rm.Blocks = nil
	rm.StreamSerials = nil
	rm.Dead = nil
	rm.AllTopics = nil
	rm.Monitoring = nil
}

func (rm *RemoteMonitor) HandleBlock(block map[string]any) error {
	rm.verbose("Handling block: %#v\n", block)
	switch block["type"] {
	case "data", "code", "monitor":
		return rm.BasicPatch(false, true, block)
	case "delete":
		switch names := block["value"].(type) {
		case string:
			return rm.basicDeleteBlocks(false, names)
		case []string:
			return rm.basicDeleteBlocks(false, names...)
		case []any:
			strs := make([]string, len(names))
			for i, str := range names {
				if s, ok := str.(string); ok {
					strs[i] = s
				} else {
					return fmt.Errorf("delete block has bad value: %#v", block["value"])
				}
			}
			return rm.basicDeleteBlocks(false, strs...)
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
		block := rm.Blocks[name]
		if bytes, err := json.Marshal(rm.Blocks[name]); err != nil {
			errstr := fmt.Sprint(fmt.Errorf("Error encoding block %s = %#v: %w", name, block, err))
			writeError(w, http.StatusBadRequest, errstr)
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write(bytes)
		}
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

func CopyBlock(block map[string]any) map[string]any {
	cpy := make(map[string]any, len(block))
	for k, v := range block {
		cpy[k] = v
	}
	return cpy
}

func (rm *RemoteMonitor) Patch(send bool, blocks ...map[string]any) error {
	_, err := WrappedSvcSync(rm.Service, func() (bool, error) {
		err := rm.BasicPatch(send, true, blocks...)
		return true, err
	})
	return err
}

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
			rm.Blocks[name] = block
		}
	}
	if count > 0 {
		rm.Serial++
		rm.AllTopics = make(Set[string])
		rm.AllTopics.Add(rm.DefaultTopic)
		// a fingertree could maintain this automatically
		for _, block := range rm.Blocks {
			addTopics(rm.AllTopics, block)
		}
		if send {
			fmt.Println("SENDING:", blocks[:count])
			rm.Send(rm.Service.invocation, blocks[:count]...)
		}
		if change {
			for l := range rm.Listeners {
				l.DataChanged(rm)
			}
		}
	}
	return nil
}

func (mon *Monitoring) httpDelete(w http.ResponseWriter, r *http.Request) {
	activity := "getting monitor connection"
	defer func() { recovery(w, activity) }()
	rm := mon.getMonitor(r)
	if rm == nil {
		return
	}
	activity = "sending deletes"
	err := rm.DeleteBlocks(true, r.PathValue("name"))
	if err != nil {
		panic(activity)
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
		err := rm.basicDeleteBlocks(send, names...)
		return true, err
	})
	return err
}

func (rm *RemoteMonitor) basicDeleteBlocks(send bool, names ...string) error {
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
	topics, err := computeTopics(rm.Blocks)
	if err != nil {
		return err
	}
	rm.AllTopics = topics
	names = names[0:pos]
	if len(names) == 0 {
		return nil
	}
	if send {
		rm.Send(i, JMap("type", "delete", "value", names))
	}
	return nil
}

func computeTopics(blocks map[string]map[string]any) (Set[string], error) {
	allTopics := make(Set[string])
	for _, block := range blocks {
		if err := addTopics(allTopics, block); err != nil {
			return nil, err
		}
	}
	return allTopics, nil
}

func addTopics(topics Set[string], blocks ...map[string]any) error {
	for i, block := range blocks {
		if blockTopics, ok := block["topics"].([]string); ok {
			for _, topic := range blockTopics {
				topics[topic] = true
			}
		} else if blockTopic, ok := block["topics"].(string); ok {
			topics[blockTopic] = true
		} else if block["topics"] != nil {
			return fmt.Errorf("%w Block %d has a bad topic: %#v", ErrBadBlock, i, block)
		}
	}
	return nil
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
	result, deleted := rm.GetUpdates(serial, timeout, nodata)
	w.WriteHeader(http.StatusOK)
	if nodata {
		w.Write([]byte(fmt.Sprint(rm.Serial)))
	} else if result != nil {
		activity = "encoding update blocks"
		encoded, err := json.Marshal(JMap("serial", rm.Serial, "blocks", result, "deleted", deleted))
		if err != nil {
			panic(activity)
		}
		w.Write(encoded)
	} else {
		w.Write([]byte("null"))
	}
}

func (rm *RemoteMonitor) GetUpdates(serial int64, timeout float64, nodata bool) (map[string]any, []string) {
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
	if nodata {
		return nil, nil
	}
	WrappedSvcSync(rm.Service, func() (bool, error) {
		result, dead = rm.gatherUpdates(serial)
		return true, nil
	})
	return result, dead
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
	var topics []string = []string{rm.DefaultTopic}
	start := 0
	ctx := context.Background()
	sendOnTopic := func(topic string, blocks any) {
		blocksStr, err := json.Marshal(blocks)
		if err != nil {
			abort("Error sending to topic %s, could not get JSON for blocks: %#s\n", topic, blocks)
		}
		rm.verbose("SENDING TO '%s' %#v", topic, blocks)
		_, err = rm.redis.XAdd(ctx, &redis.XAddArgs{
			Stream:     topic,
			NoMkStream: false,
			ID:         "*",
			Values:     []any{"batch", blocksStr, "sender", rm.PeerId},
		}).Result()
		if err != nil {
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
		t := enslice(block["topics"])
		if !rm.sameTopics(t, topics) {
			sendBlocks(i)
			topics = t
			if topics == nil {
				topics = []string{rm.DefaultTopic}
			}
		}
	}
	sendBlocks(len(blocks))
}

func enslice(item any) []string {
	if item == nil {
		return nil
	} else if s, ok := item.(string); ok {
		return []string{s}
	} else if sA, ok := item.([]string); ok {
		return sA
	} else if a, ok := item.([]any); ok {
		t := make([]string, 0, len(a))
		for _, element := range a {
			if elStr, ok := element.(string); ok {
				t = append(t, elStr)
			} else {
				fmt.Fprintf(os.Stderr, "bad string value: %s\n", elStr)
			}
		}
		return t
	}
	fmt.Fprintf(os.Stderr, "bad string array value: %s\n", item)
	return nil
}

func (rm *RemoteMonitor) sameTopics(a, b []string) bool {
	if a == nil {
		return b == nil || (len(b) == 1 && b[0] == rm.DefaultTopic)
	} else if b == nil {
		return len(a) == 1 && a[0] == rm.DefaultTopic
	} else if len(a) != len(b) {
		return false
	}
	items := NewSet(a...)
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
	mon.handle(mux, REMOVE, mon.httpRemove)
	mon.handle(mux, GET, mon.httpGet)
	mon.handle(mux, GET_BLOCK, mon.httpGetBlock)
	mon.handle(mux, PATCH, mon.httpPatch)
	mon.handle(mux, DELETE, mon.httpDelete)
	mon.handle(mux, DELETE_POST, mon.httpDelete)
	mon.handle(mux, UPDATE, mon.httpUpdate)
}
