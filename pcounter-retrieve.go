package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	pcounter "github.com/synerex/proto_pcounter"
	api "github.com/synerex/synerex_api"
	pb "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

// datastore provider provides Datastore Service.

type DataStore interface {
	store(str string)
}

var (
	nodesrv   = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local     = flag.String("local", "", "Local Synerex Server")
	sendfile  = flag.String("sendfile", "", "Sending file name") // only one file
	startDate = flag.String("startDate", "02-07", "Specify Start Date")
	endDate   = flag.String("endDate", "12-31", "Specify End Date")
	startTime = flag.String("startTime", "00:00", "Specify Start Time")
	endTime   = flag.String("endTime", "24:00", "Specify End Time")
	dir       = flag.String("dir", "", "Directory of data storage") // for all file
	all       = flag.Bool("all", false, "Send all file in Dir")     // for all file
	speed     = flag.Float64("speed", 1.0, "Speed of sending packets")
	multi     = flag.Int("multi", 1, "Specify sending multiply messages")
	mu        sync.Mutex
	version   = "0.01"
	baseDir   = "store"
	dataDir   string
	ds        DataStore
)

func init() {
	var err error
	dataDir, err = os.Getwd()
	if err != nil {
		fmt.Printf("Can't obtain current wd")
	}
	dataDir = filepath.ToSlash(dataDir) + "/" + baseDir
	ds = &FileSystemDataStore{
		storeDir: dataDir,
	}
}

type FileSystemDataStore struct {
	storeDir  string
	storeFile *os.File
	todayStr  string
}

// open file with today info
func (fs *FileSystemDataStore) store(str string) {
	const layout = "2006-01-02"
	day := time.Now()
	todayStr := day.Format(layout) + ".csv"
	if fs.todayStr != "" && fs.todayStr != todayStr {
		fs.storeFile.Close()
		fs.storeFile = nil
	}
	if fs.storeFile == nil {
		_, er := os.Stat(fs.storeDir)
		if er != nil { // create dir
			er = os.MkdirAll(fs.storeDir, 0777)
			if er != nil {
				fmt.Printf("Can't make dir '%s'.", fs.storeDir)
				return
			}
		}
		fs.todayStr = todayStr
		file, err := os.OpenFile(filepath.FromSlash(fs.storeDir+"/"+todayStr), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("Can't open file '%s'", todayStr)
			return
		}
		fs.storeFile = file
	}
	fs.storeFile.WriteString(str + "\n")
}

func supplyPCounterCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {

	pc := &pcounter.PCounter{}

	err := proto.Unmarshal(sp.Cdata.Entity, pc)
	if err == nil { // get Pcounter
		ts0 := ptypes.TimestampString(pc.Ts)
		ld := fmt.Sprintf("%s,%s,%s,%s", ts0, pc.Hostname, pc.Mac, pc.Ip, pc.IpVpn)
		ds.store(ld)
		for _, ev := range pc.Data {
			ts := ptypes.TimestampString(ev.Ts)
			line := fmt.Sprintf("%s,%s,%d,%s,%s,", ts, pc.DeviceId, ev.Seq, ev.Typ, ev.Id)
			switch ev.Typ {
			case "counter":
				line = line + fmt.Sprintf("%s,%d", ev.Dir, ev.Height)
			case "fillLevel":
				line = line + fmt.Sprintf("%d", ev.FillLevel)
			case "dwellTime":
				tsex := ptypes.TimestampString(ev.TsExit)
				line = line + fmt.Sprintf("%f,%f,%s,%d,%d", ev.DwellTime, ev.ExpDwellTime, tsex, ev.ObjectId, ev.Height)
			}
			ds.store(line)
		}
	}
}

func subscribePCounterSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	client.SubscribeSupply(ctx, supplyPCounterCallback)
	log.Fatal("Error on subscribe")
}

const dateFmt = "2006-01-02T15:04:05.999Z"

func atoUint(s string) uint32 {
	r, err := strconv.Atoi(s)
	if err != nil {
		log.Print("err", err)
	}
	return uint32(r)
}

func getHourMin(dt string) (hour int, min int) {
	st := strings.Split(dt, ":")
	hour, _ = strconv.Atoi(st[0])
	min, _ = strconv.Atoi(st[1])
	return hour, min
}

func getMonthDate(dt string) (month int, date int) {
	st := strings.Split(dt, "-")
	month, _ = strconv.Atoi(st[0])
	date, _ = strconv.Atoi(st[1])
	return month, date
}

// sending People Counter File.
func sendingPCounterFile(client *sxutil.SXServiceClient) {
	// file
	fp, err := os.Open(*sendfile)
	if err != nil {
		panic(err)
	}
	defer fp.Close()

	scanner := bufio.NewScanner(fp) // csv reader

	last := time.Now()
	var pc *pcounter.PCounter = nil
	evts := make([]*pcounter.PEvent, 0, 1)
	pcs := make([]*pcounter.PCounter, 0, 1)
	mcount := 0      // count multiple packets
	started := false // start flag
	stHour, stMin := getHourMin(*startTime)
	edHour, edMin := getHourMin(*endTime)

	for scanner.Scan() { // read one line.
		dt := scanner.Text()
		token := strings.Split(dt, ",")

		switch token[3] {
		case "alive":
		case "statusList":

		case "counter":
			//			fmt.Println(token[0], token[1], token[2], token[3], token[4], token[5], token[6])
			tm, _ := time.Parse(dateFmt, token[0]) // RFC3339Nano
			// check timestamp of data
			if !started {
				if (tm.Hour() > stHour || (tm.Hour() == stHour && tm.Minute() >= stMin)) &&
					(tm.Hour() < edHour || (tm.Hour() == edHour && tm.Minute() <= edMin)) {
					started = true
					log.Printf("Start output! %v", tm)
				} else {
					continue // skip all data
				}
			} else {
				if tm.Hour() > edHour || (tm.Hour() == edHour && tm.Minute() > edMin) {
					started = false
					log.Printf("Stop  output! %v", tm)
					continue
				}
			}

			tp, _ := ptypes.TimestampProto(tm)
			evt := &pcounter.PEvent{
				Typ:    token[3],
				Ts:     tp,
				Seq:    atoUint(token[2]),
				Id:     token[4],
				Dir:    token[5],
				Height: atoUint(token[6]),
			}
			evts = append(evts, evt)
		case "fillLevel":

		case "dwellTime":

		default: // this might come first // IP address
			//			log.Printf("%s:%s",token[3], dt)
			if !started {
				continue
			}
			if pc != nil {
				//				sendPacket(pc)
				if len(evts) > 0 {
					if *multi == 1 { // sending each packets
						pc.Data = evts
						out, _ := proto.Marshal(pc)
						cont := pb.Content{Entity: out}
						smo := sxutil.SupplyOpts{
							Name:  "PCounter",
							Cdata: &cont,
						}
						_, nerr := client.NotifySupply(&smo)
						if nerr != nil {
							log.Printf("Send Fail!\n", nerr)
						} else {
							//						log.Printf("Sent OK! %#v\n", pc)
						}
						if *speed < 0 { // sleep for each packet
							time.Sleep(time.Duration(-*speed) * time.Millisecond)
						}

					} else { // sending multiple packets
						mcount++
						pc.Data = evts
						pcs = append(pcs, pc)
						if mcount > *multi { // now sending!
							pcss := &pcounter.PCounters{
								Pcs: pcs,
							}
							out, _ := proto.Marshal(pcss)
							cont := pb.Content{Entity: out}
							smo := sxutil.SupplyOpts{
								Name:  "PCounterMulti",
								Cdata: &cont,
							}
							_, nerr := client.NotifySupply(&smo)
							if nerr != nil {
								log.Printf("Send Fail!\n", nerr)
							} else {
								log.Printf("Sent OK! %d bytes: %s\n", len(out), ptypes.TimestampString(pc.Ts))
							}
							if *speed < 0 {
								time.Sleep(time.Duration(-*speed) * time.Millisecond)
							}
							pcs = make([]*pcounter.PCounter, 0, 1)
							mcount = 0
						}
					}
				}

			}
			evts = make([]*pcounter.PEvent, 0, 1)
			pc = &pcounter.PCounter{}
			tm, er := time.Parse(dateFmt, token[0])
			if er != nil {
				log.Printf("Time parse error! %v  %v", tm, er)
			}
			dur := tm.Sub(last)
			//			log.Printf("Sleep %v %v %v",dur, tm, last)
			if dur.Nanoseconds() > 0 {
				if *speed > 0 {
					time.Sleep(time.Duration(float64(dur.Nanoseconds()) / *speed))
				}
				last = tm
			}
			if dur.Nanoseconds() < 0 {
				last = tm
			}

			tp, _ := ptypes.TimestampProto(tm)
			pc.Ts = tp
			pc.Hostname = token[1]
			pc.DeviceId = token[2]
			pc.Mac = token[2]
			pc.Ip = token[3]
			pc.IpVpn = token[4]
		}
	}

	if pc != nil {
		if len(evts) > 0 {
			if *multi == 1 { // sending each packets
				pc.Data = evts
				out, _ := proto.Marshal(pc)
				cont := pb.Content{Entity: out}
				smo := sxutil.SupplyOpts{
					Name:  "PCounter",
					Cdata: &cont,
				}
				_, nerr := client.NotifySupply(&smo)
				if nerr != nil {
					log.Printf("Send Fail!\n", nerr)
				} else {
					//							log.Printf("Sent OK! %#v\n", pc)
				}
			} else { // sending multiple packets
				mcount++
				pc.Data = evts
				pcs = append(pcs, pc)
				pcss := &pcounter.PCounters{
					Pcs: pcs,
				}
				out, _ := proto.Marshal(pcss)
				cont := pb.Content{Entity: out}
				smo := sxutil.SupplyOpts{
					Name:  "PCounterMulti",
					Cdata: &cont,
				}
				_, nerr := client.NotifySupply(&smo)
				if nerr != nil {
					log.Printf("Send Fail!\n", nerr)
				} else {
					log.Printf("Sent Last OK! %d bytes: %s\n", len(out), ptypes.TimestampString(pc.Ts))
				}
			}
		}
	}

	if mcount > 0 {
		pc.Data = evts
		pcs = append(pcs, pc)
		pcss := &pcounter.PCounters{
			Pcs: pcs,
		}
		out, _ := proto.Marshal(pcss)
		cont := pb.Content{Entity: out}
		smo := sxutil.SupplyOpts{
			Name:  "PCounterMulti",
			Cdata: &cont,
		}
		_, nerr := client.NotifySupply(&smo)
		if nerr != nil {
			log.Printf("Send Fail!\n", nerr)
		} else {
			log.Printf("Sent Last OK! %d bytes: %s\n", len(out), ptypes.TimestampString(pc.Ts))
		}
	}
}

func sendAllPCounterFile(client *sxutil.SXServiceClient) {
	// check all files in dir.
	stMonth, stDate := getMonthDate(*startDate)
	edMonth, edDate := getMonthDate(*endDate)

	if *dir == "" {
		log.Printf("Please specify directory")
		data := "data"
		dir = &data
	}
	files, err := ioutil.ReadDir(*dir)

	if err != nil {
		log.Printf("Can't open diretory %v", err)
		os.Exit(1)
	}
	// should be sorted.
	var ss = make(sort.StringSlice, 0, len(files))

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".csv") { // check is CSV file
			//
			fn := file.Name()
			var year, month, date int
			ct, err := fmt.Sscanf(fn, "%4d-%02d-%02d.csv", &year, &month, &date)
			if (month > stMonth || (month == stMonth && date >= stDate)) &&
				(month < edMonth || (month == edMonth && date <= edDate)) {
				ss = append(ss, file.Name())
			} else {
				log.Printf("file: %d %v %s: %04d-%02d-%02d", ct, err, fn, year, month, date)
			}
		}
	}

	ss.Sort()

	for _, fname := range ss {
		dfile := path.Join(*dir, fname)
		// check start date.

		log.Printf("Sending %s", dfile)
		sendfile = &dfile
		sendingPCounterFile(client)
	}

}

//dataServer(pc_client)

func main() {
	log.Printf("PCounterRetrieve(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.PEOPLE_COUNTER_SVC}

	srv, rerr := sxutil.RegisterNode(*nodesrv, "PCouterRetrieve", channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		srv = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", srv)

	//	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(srv)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	} else {
		log.Print("Connecting SynerexServer")
	}

	pc_client := sxutil.NewSXServiceClient(client, pbase.PEOPLE_COUNTER_SVC, "{Client:PcountRetrieve}")

	//	wg.Add(1)
	//    log.Print("Subscribe Supply")
	//    go subscribePCounterSupply(pc_client)

	if *all { // send all file
		sendAllPCounterFile(pc_client)
	} else if *dir != "" {
	} else if *sendfile != "" {
		//		for { // infinite loop..
		sendingPCounterFile(pc_client)
		//		}
	}

	//	wg.Wait()

}
