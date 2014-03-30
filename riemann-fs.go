package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/amir/raidman"
	"github.com/dvirsky/go-pylog/logging"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"os"
	"os/signal"
	"strings"
)

var riemann_fields []string = []string{"Service", "Host", "Metric", "Description", "State", "Time", "Tags", "Ttl"}

func stringSetToDirEntries(stringSet map[string]bool) []fuse.DirEntry {
	files := make([]fuse.DirEntry, len(stringSet), len(stringSet)+1)
	i := 0
	for host := range stringSet {
		files[i] = fuse.DirEntry{Name: host, Mode: fuse.S_IFDIR}
		i++
	}
	return files

}

func eventToDirEntries(event raidman.Event) []fuse.DirEntry {
	files := make([]fuse.DirEntry, 9+len(event.Attributes), 9+len(event.Attributes))
	offset := 0
	for i, field := range riemann_fields {
		files[i] = fuse.DirEntry{Name: field, Mode: fuse.S_IFREG}
		offset++
	}
	for field := range event.Attributes {
		files[offset] = fuse.DirEntry{Name: field, Mode: fuse.S_IFREG}
		offset++
	}
	files[offset] = fuse.DirEntry{Name: ".json", Mode: fuse.S_IFREG}
	return files
}

func InArray(ary []string, value string) bool {
	for _, v := range ary {
		if v == value {
			return true
		}
	}
	return false
}

type RiemannFS struct {
	RiemannClient *raidman.Client
	pathfs.FileSystem
}

// return a "set" of unique field values from query result
func (fs *RiemannFS) fieldForEventFromQuery(query string, lambda func(raidman.Event) string) map[string]bool {
	fields := map[string]bool{}
	for _, event := range fs.queryRiemann(query) {
		fields[lambda(event)] = true
	}
	return fields
}

func (fs *RiemannFS) queryRiemann(query string) []raidman.Event {
	events, err := fs.RiemannClient.Query(query)
	if err != nil {
		logging.Errorf("Error while querying riemann: %s\n", err.Error())
		return nil
	}
	return events
}

func (fs *RiemannFS) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {
	logging.Debug("OpenDir(%s, %v)\n", name, *context)
	names := strings.Split(name, string(os.PathSeparator))
	if name == "" {
		files := stringSetToDirEntries(fs.fieldForEventFromQuery("true", func(e raidman.Event) string { return e.Host }))
		files = append(files, fuse.DirEntry{Name: ".query", Mode: fuse.S_IFDIR})
		return files, fuse.OK
	} else if names[0] == ".query" {
		switch len(names) {
		case 1:
			return nil, fuse.OK // empty directory
		case 2, 3:
			return stringSetToDirEntries(
				fs.fieldForEventFromQuery(
					names[1],
					func(e raidman.Event) string {
						if len(names) == 2 {
							return e.Host
						} else {
							return e.Service
						}
					},
				),
			), fuse.OK
		case 4:
			return eventToDirEntries(fs.getEventsForPath(names)[0]), fuse.OK
		}
	} else {
		switch len(names) {
		case 1:
			return stringSetToDirEntries(
					fs.fieldForEventFromQuery(
						fmt.Sprintf("host = \"%s\"", name),
						func(e raidman.Event) string { return e.Service },
					),
				),
				fuse.OK
		case 2:
			events := fs.getEventsForPath(names)
			if events == nil || len(events) == 0 {
				return nil, fuse.OK
			}
			if len(events) > 1 {
				panic("Got more then 1 event for (host, service) query, this should never happen")
			}
			return eventToDirEntries(events[0]), fuse.OK
		}
	}
	return nil, fuse.OK
}

func (fs *RiemannFS) nodeForPath(path string) (nodeType uint32, aryPath []string) {
	aryPath = strings.Split(path, string(os.PathSeparator))
	if aryPath[0] == ".query" {
		if len(aryPath) <= 4 {
			nodeType = fuse.S_IFDIR
		} else if len(aryPath) == 5 {
			nodeType = fuse.S_IFREG
		}
	} else if len(aryPath) <= 2 {
		nodeType = fuse.S_IFDIR
	} else if len(aryPath) == 3 {
		nodeType = fuse.S_IFREG
	}
	return
}

// TODO: Actually check the Riemann index
func (fs *RiemannFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	logging.Debug("GetAttr(%s, %v)\n", name, *context)
	node_type, aryPath := fs.nodeForPath(name)
	logging.Debug("Node type: %d, aryPath: %v\n", node_type, aryPath)
	switch node_type {
	case fuse.S_IFDIR:
		return &fuse.Attr{Mode: node_type | 0755}, fuse.OK
	case fuse.S_IFREG:
		if events := fs.getEventsForPath(aryPath); len(events) == 1 {
			value, _ := valueForAttribute(events[0], aryPath[len(aryPath)-1])
			return &fuse.Attr{Mode: node_type | 0644, Size: uint64(len(value))}, fuse.OK
		}
	}
	return nil, fuse.ENOENT
}

func (fs *RiemannFS) Open(path string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	node_type, aryPath := fs.nodeForPath(path)
	switch node_type {
	case fuse.S_IFREG:
		// first, check flags
		if flags&fuse.O_ANYWRITE != 0 {
			return nil, fuse.EPERM
		}
		events := fs.getEventsForPath(aryPath)
		if len(events) > 1 {
			panic("Got more then 1 event for query, this should never happen")
		} else if len(events) == 0 {
			return nil, fuse.ENOENT
		}
		value, err := valueForAttribute(events[0], aryPath[len(aryPath)-1])
		if err != nil {
			return nil, fuse.ENOENT
		}
		return nodefs.NewReadOnlyFile(nodefs.NewDataFile(value)), fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *RiemannFS) queryForPath(aryPath []string) string {
	if aryPath[0] == ".query" {
		switch len(aryPath) {
		case 2:
			return aryPath[1]
		case 3:
			return fmt.Sprintf("(%s) and host = \"%s\"", aryPath[1], aryPath[2])
		case 4, 5:
			return fmt.Sprintf("(%s) and host = \"%s\" and service = \"%s\"", aryPath[1], aryPath[2], aryPath[3])
		}
	} else if len(aryPath) >= 2 {
		return fmt.Sprintf("host = \"%s\" and service = \"%s\"", aryPath[0], aryPath[1])
	}
	return ""
}

func (fs *RiemannFS) getEventsForPath(aryPath []string) []raidman.Event {
	if query := fs.queryForPath(aryPath); query != "" {
		return fs.queryRiemann(query)
	}
	return nil
}

func valueForAttribute(event raidman.Event, attribute string) (value []byte, err error) {
	if attribute == ".json" {
		value, err = json.Marshal(event)
	} else {
		if _value, ok := event.Attributes[attribute]; ok {
			return []byte(_value), nil
		}
		if !InArray(riemann_fields, attribute) {
			return nil, errors.New("Field not found")
		}
		var sValue string
		switch attribute {
		case "Tags":
			sValue = strings.Join(event.Tags, ", ")
		case "Time":
			sValue = fmt.Sprintf("%d", event.Time)
		case "Ttl":
			sValue = fmt.Sprintf("%f", event.Ttl)
		case "Metric":
			sValue = fmt.Sprintf("%f", event.Metric)
		case "Host":
			sValue = event.Host
		case "Service":
			sValue = event.Service
		case "Description":
			sValue = event.Description
		case "State":
			sValue = event.State
		}
		value = []byte(sValue)
	}
	return
}

func main() {
	var riemann_host = flag.String("host", "localhost", "Riemann host to connect to")
	var riemann_port = flag.Int("port", 5555, "Riemann TCP port")
	var mount_point = flag.String("mount-point", "", "A directory to mount on")
	var debug_mode = flag.Bool("debug", false, "Activate debug mode")

	flag.Parse()

	if *mount_point == "" {
		fmt.Println("Mount point must be specified")
		os.Exit(1)
	}
	fmt.Printf("Mounting RiemannFS on %s\n", *mount_point)
	if *debug_mode {
		logging.SetLevel(logging.ALL)
	} else {
		logging.SetLevel(logging.ALL &^ (logging.INFO | logging.DEBUG))
	}

	riemannClient, err := raidman.Dial("tcp", fmt.Sprintf("%s:%d", *riemann_host, *riemann_port))
	if err != nil {
		logging.Panic("Failed to connect to riemann, %s", err.Error())
	}
	fs := &RiemannFS{FileSystem: pathfs.NewDefaultFileSystem(), RiemannClient: riemannClient}
	roFS := pathfs.NewReadonlyFileSystem(fs)
	nfs := pathfs.NewPathNodeFs(roFS, nil)
	mount, _, err := nodefs.MountRoot(*mount_point, nfs.Root(), nil)
	if err == nil {
		// trap ctrl+c and umount
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		go func() {
			for _ = range c {
				mount.Unmount()
			}
		}()
		mount.Serve()
	} else {
		logging.Panic("something wrong happened while mounting\n")
	}
}
