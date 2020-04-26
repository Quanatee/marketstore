package main

import (
	//"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4polygon"
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4tiingo"
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/backfill"
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/livefill"
	//"github.com/alpacahq/marketstore/contrib/polygon/handlers"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	//"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
	"gopkg.in/yaml.v2"
)

type QuanateeFetcher struct {
	config FetcherConfig
}

type FetcherConfig struct {
    PolygonApiKey  string   `yaml:"polygon_api_key"`
    TiingoApiKey   string   `yaml:"tiingo_api_key"`
	TwelveApiKey   string   `yaml:"twelve_api_key"`
	MarketType     string   `yaml:"market_type"`
	QueryStart     string   `yaml:"query_start"`
	Symbols        []string `yaml:"symbols"`
}

// NewBgWorker returns a new instances of QuanateeFetcher. See FetcherConfig
// for more details about configuring QuanateeFetcher.
func NewBgWorker(conf map[string]interface{}) (w bgworker.BgWorker, err error) {
	data, _ := yaml.Marshal(conf)
	config := FetcherConfig{}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return
    }
    
	backfill.BackfillM = &sync.Map{}
	
	return &QuanateeFetcher{
		config: config,
	}, nil
}

// Run the QuanateeFetcher. It starts the streaming API as well as the
// asynchronous backfilling routine.
func (qf *QuanateeFetcher) Run() {

	//log.Info("Polygon Key: %s", qf.config.PolygonApiKey)
	//log.Info("Market Type: %s", qf.config.MarketType)
	api4polygon.SetAPIKey(qf.config.PolygonApiKey)
	api4tiingo.SetAPIKey(qf.config.TiingoApiKey)

	from := time.Now().Add(time.Minute)
	from = time.Date(from.Year(), from.Month(), from.Day(), from.Hour(), from.Minute(), 0, 0, time.UTC)
	to := from.Add(time.Minute)
	to = to.Add(1*time.Millisecond)
	// from = from.Add(-1*time.Second)
	
	firstLoop := true

	for {
		
		for {
			if time.Now().Unix() >= to.Unix() {
				break
			} else {
				time.Sleep(to.Sub(time.Now()))
				time.Sleep(1*time.Millisecond)
			}
		}
		
		for _, symbol := range qf.config.Symbols {
			var (
				err  error
				tbk  = io.NewTimeBucketKey(fmt.Sprintf("%s/1Min/OHLCV", symbol))
			)
			if err = livefill.Bars(symbol, qf.config.MarketType, from, to); err != nil {
				log.Error("[polygon] bars livefill failure for key: [%v] (%v)", tbk.String(), err)
			}
			
			if firstLoop == true {
				log.Info("")
				//backfill.BackfillM.LoadOrStore(symbol, from.Unix())
				//go qf.workBackfillBars()
			}

		}
				
		from = from.Add(time.Minute)
		to = to.Add(time.Minute)
		
		if firstLoop == true {
			firstLoop = false
		}
	}

	select {}
}

func (qf *QuanateeFetcher) workBackfillBars() {

	ticker := time.NewTicker(5 * time.Second)

	for range ticker.C {
		
		wg := sync.WaitGroup{}
		count := 0

		// range over symbols that need backfilling, and
		// backfill them from the last written record
		backfill.BackfillM.Range(func(key, value interface{}) bool {
			symbol := key.(string)
			// make sure epoch value isn't nil (i.e. hasn't
			// been backfilled already)
			if value != nil {
				go func() {
					wg.Add(1)
					defer wg.Done()

					// backfill the symbol in parallel
					stop := qf.backfillBars(symbol, value.(int64))
					if stop == true {
						log.Info("%s backfill is complete", symbol)
						backfill.BackfillM.Store(key, nil)
					} else {
						backfill.BackfillM.LoadOrStore(key, nil)
					}
				}()
			}

			// limit 10 goroutines per CPU core
			if count >= runtime.NumCPU()*10 {
				return false
			}

			return true
		})
		wg.Wait()
	}
}

// Backfill bars from start
func (qf *QuanateeFetcher) backfillBars(symbol string, endEpoch int64) bool {

	var (
		start time.Time
		end   time.Time
		from time.Time
		err  error
		tbk  = io.NewTimeBucketKey(fmt.Sprintf("%s/1Min/OHLCV", symbol))
	)
	
	for _, layout := range []string{
		"2006-01-02 03:04:05",
		"2006-01-02T03:04:05",
		"2006-01-02 03:04",
		"2006-01-02T03:04",
		"2006-01-02",
	} {
		start, err = time.Parse(layout, qf.config.QueryStart)
		if err == nil {
			break
		}
	}

	end = time.Unix(endEpoch, 0)

	// query the latest entry prior to the streamed record	
	instance := executor.ThisInstance
	cDir := instance.CatalogDir
	q := planner.NewQuery(cDir)
	q.AddTargetKey(tbk)
	q.SetRowLimit(io.LAST, 1)
	q.SetEnd(endEpoch - int64(time.Minute.Seconds()))

	parsed, err := q.Parse()
	if err != nil {
		log.Error("[polygon] query parse failure (%v)", err)
		return true
	}

	scanner, err := executor.NewReader(parsed)
	if err != nil {
		log.Error("[polygon] new scanner failure (%v)", err)
		return true
	}

	csm, err := scanner.Read()
	if err != nil {
		log.Error("[polygon] scanner read failure (%v)", err)
		return true
	}

	epoch := csm[*tbk].GetEpoch()
	stop := false

	// has gap to fill
	if len(epoch) != 0 {
		from = time.Unix(epoch[len(epoch)-1], 0)
	} else {
		from = start
	}

	to := from.AddDate(0, 0, 7)
	if to.Unix() >= end.Unix() {
		to = end
		stop = true
	}
	
	// log.Info("%s backfill from %v to %v, stop:%v", symbol, from, to, stop)
	
	// request & write the missing bars
	if err = backfill.Bars(symbol, qf.config.MarketType, from, to); err != nil {
		log.Error("[polygon] bars backfill failure for key: [%v] (%v)", tbk.String(), err)
	}
	
	return stop
}

func main() {}
