package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/hashicorp/serf/serf"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

// Constants:
const (
	DefaultAppName = `serf`
	defaultAddr    = "0.0.0.0"
	defaultPort    = 7946
	defaultAPIPort = `8080`
)

var (
	apiAddr       string
	apiPort       string
	bindAddr      string
	bindPort      int
	advertiseAddr string
	advertisePort int
	peerList      []string
	amLeader      bool
	pf            *pflag.FlagSet
)

func init() {
	pf = pflag.NewFlagSet(DefaultAppName, pflag.ExitOnError)
	pf.StringVar(&bindAddr, "bind", defaultAddr, "Set bind address")
	pf.IntVar(&bindPort, "port", defaultPort, "Set bind port")
	pf.StringVar(&advertiseAddr, "addr", "", "Set advertise address")
	pf.IntVar(&advertisePort, "aport", defaultPort, "Set sdvertise port")
	pf.StringVar(&apiPort, "api", defaultAPIPort, "Set API port")
	pf.StringSliceVar(&peerList, `peers`, []string{}, "Comma separated list of known peers")
}

func main() {
	pf.Parse(os.Args[1:])
	if pf.Changed(`port`) && !pf.Changed(`aport`) {
		advertisePort = bindPort
	}
	apiAddr = bindAddr + `:` + apiPort
	cluster, err := setupCluster(bindAddr, advertiseAddr, bindPort, advertisePort, peerList...)
	if err != nil {
		log.Fatal(err)
	}
	defer cluster.Leave()

	theOneAndOnlyNumber := InitTheNumber(42)
	launchHTTPAPI(theOneAndOnlyNumber)

	ctx := context.Background()
	if name, err := os.Hostname(); err == nil {
		ctx = context.WithValue(ctx, "name", name)
	}

	//debugDataPrinterTicker := time.Tick(time.Second * 5)
	numberBroadcastTicker := time.Tick(time.Second * 2)
	leaderBroadcastTicker := time.Tick(time.Second * 5)
	leaderWorkTicker := time.Tick(time.Second * 10)
	for {
		select {
		//case <-debugDataPrinterTicker:
		//log.Printf("%v Members\n%+v\n\n", cluster.NumNodes(), cluster.Members())
		//log.Printf("%v Members\n", cluster.NumNodes())
		//curVal, curGen, curMeta := theOneAndOnlyNumber.getValue()
		//log.Printf("State: Val: %v Gen: %v Leader: %v\n", curVal, curGen, curMeta)
		case <-numberBroadcastTicker:
			members := getOtherMembers(cluster)
			ctx, _ := context.WithTimeout(ctx, time.Second*2)
			go notifyOthers(ctx, members, theOneAndOnlyNumber)
		case <-leaderBroadcastTicker:
			var highestVal int
			var highestNode string
			for _, m := range cluster.Members() {
				var val int
				switch m.Status.String() {
				case `alive`:
					for _, oct := range strings.Split(m.Addr.String(), `.`) {
						o, _ := strconv.Atoi(oct)
						val += o
					}
				default:
				}
				if val > highestVal {
					highestVal = val
					highestNode = m.Name
				}
				//fmt.Printf("Peer: %v Status: %v, PeerVal: %v\n", m.Name, m.Status, val)
			}
			curVal, _, _ := theOneAndOnlyNumber.getValue()
			if curVal != highestVal {
				if highestNode == cluster.LocalMember().Name {
					theOneAndOnlyNumber.setValue(highestVal, highestNode)
					amLeader = true
				} else {
					amLeader = false
				}
			}
		case <-leaderWorkTicker:
			_, _, leader := theOneAndOnlyNumber.getValue()
			if leader == cluster.LocalMember().Name {
				fmt.Println("I AM LEADER. AM I:", amLeader)
			} else {
				fmt.Println("I AM NOT THE LEADER. AM I:", amLeader)
			}
		}
	}

}

func getOtherMembers(cluster *serf.Serf) []serf.Member {
	members := cluster.Members()
	for i := 0; i < len(members); {
		if members[i].Name == cluster.LocalMember().Name || members[i].Status != serf.StatusAlive {
			if i < len(members)-1 {
				members = append(members[:i], members[i+1:]...)
			} else {
				members = members[:i]
			}
		} else {
			i++
		}
	}
	return members
}

func notifyOthers(ctx context.Context, otherMembers []serf.Member, db *oneAndOnlyNumber) {
	g, ctx := errgroup.WithContext(ctx)
	if len(otherMembers) <= MembersToNotify {
		for _, member := range otherMembers {
			curMember := member
			g.Go(func() error {
				return notifyMember(ctx, curMember.Addr.String(), apiPort, db)
			})
		}
	} else {
		randIndex := rand.Int() % len(otherMembers)
		for i := 0; i < MembersToNotify; i++ {
			g.Go(func() error {
				return notifyMember(
					ctx,
					otherMembers[(randIndex+i)%len(otherMembers)].Addr.String(),
					apiPort,
					db)
			})
		}
	}

	err := g.Wait()
	if err != nil {
		log.Printf("Error when notifying other members: %v", err)
	}
}

func notifyMember(ctx context.Context, addr, port string, db *oneAndOnlyNumber) error {
	val, gen, _ := db.getValue()
	URL := fmt.Sprintf("http://%v:%v/notify/%v/%v?notifier=%v", addr, port, val, gen, ctx.Value("name"))
	req, err := http.NewRequest("POST", URL, nil)
	if err != nil {
		//return errors.Wrap(err, "Couldn't create request")
		return fmt.Errorf("unable to create POST request: %v", err)
	}
	req = req.WithContext(ctx)

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("unable to complete request: %v", err)
	}
	return nil
}

func launchHTTPAPI(db *oneAndOnlyNumber) {
	go func() {
		m := mux.NewRouter()
		m.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
			val, _, _ := db.getValue()
			fmt.Fprintf(w, "%v", val)
		})

		m.HandleFunc("/set/{newVal}/{metadata}", func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			newVal, err := strconv.Atoi(vars["newVal"])
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "%v", err)
				return
			}

			db.setValue(newVal, vars["metadata"])

			fmt.Fprintf(w, "%v", newVal)
		})

		m.HandleFunc("/notify/{curVal}/{curGeneration}", func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			curVal, err := strconv.Atoi(vars["curVal"])
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "%v", err)
				return
			}
			curGeneration, err := strconv.Atoi(vars["curGeneration"])
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "%v", err)
				return
			}

			notifier := r.URL.Query().Get("notifier")

			if changed := db.notifyValue(curVal, curGeneration, notifier); changed {
				log.Printf(
					"NewVal: %v Gen: %v Notifier: %v",
					curVal,
					curGeneration,
					notifier)
			}
			w.WriteHeader(http.StatusOK)
		})
		fmt.Println(`Started API on`, apiAddr)
		log.Fatal(http.ListenAndServe(apiAddr, m))
	}()
}

func setupCluster(bindAddr, advertiseAddr string, bindPort, advertisePort int, peers ...string) (*serf.Serf, error) {
	conf := serf.DefaultConfig()
	conf.Init()
	conf.MemberlistConfig.BindAddr = bindAddr
	conf.MemberlistConfig.BindPort = bindPort
	conf.MemberlistConfig.AdvertiseAddr = advertiseAddr
	conf.MemberlistConfig.AdvertisePort = advertisePort

	cluster, err := serf.Create(conf)
	if err != nil {
		return nil, fmt.Errorf("unable to create cluster: %v", err)
	}

	_, err = cluster.Join(peers, true)
	if err != nil {
		log.Printf("Couldn't join cluster, starting own: %v\n", err)
	}

	return cluster, nil
}
