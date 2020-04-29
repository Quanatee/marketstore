package main

import (
	//"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"
	
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4polygon"
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4tiingo"
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4twelve"
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/filler"
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
    PolygonApiKey   string   `yaml:"polygon_api_key"`
    TiingoApiKey    string   `yaml:"tiingo_api_key"`
	TwelveApiKey    string   `yaml:"twelve_api_key"`
	QueryStart      string   `yaml:"query_start"`
	CryptoSymbols	[]string `yaml:"crypto_symbols"`
	ForexSymbols 	[]string `yaml:"forex_symbols"`
	EquitySymbols   []string `yaml:"equity_symbols"`
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
    
	filler.BackfillFrom = &sync.Map{}
	filler.BackfillMarket = &sync.Map{}
	
	return &QuanateeFetcher{
		config: config,
	}, nil
}

// Run the QuanateeFetcher. It starts the streaming API as well as the
// asynchronous backfilling routine.
func (qf *QuanateeFetcher) Run() {
	/*
	log.Info("Polygon Key: %s", qf.config.PolygonApiKey)
	log.Info("Tiingo Key: %s", qf.config.TiingoApiKey)
	log.Info("Twelve Key: %s", qf.config.TwelveApiKey)
	*/
	api4polygon.SetAPIKey(qf.config.PolygonApiKey)
	api4tiingo.SetAPIKey(qf.config.TiingoApiKey)
	api4twelve.SetAPIKey(qf.config.TwelveApiKey)

	from := time.Now().Add(time.Minute)
	from = time.Date(from.Year(), from.Month(), from.Day(), from.Hour(), from.Minute(), 0, 0, time.UTC)
	to := from.Add(time.Minute)
	to = to.Add(1*time.Second)
	
	firstLoop := true

	for {
		
		for {
			if time.Now().Unix() >= to.Unix() {
				break
			} else {
				time.Sleep(to.Sub(time.Now()))
			}
		}
		// Loop Crypto Symbols
		for _, symbol := range qf.config.CryptoSymbols {
			if filler.IsMarketOpen("crypto", from) == true {
				// Market is open
				go func() {
					var err error
					if err = filler.Bars(symbol, "crypto", from, to); err != nil {
						log.Error("bars livefill failure for key: [%v] (%v)", symbol, err)
					}
				}()
			} else if firstLoop == true {
				// Market is closed but we just started pentagon
				go func() {
					var err error
					if err = filler.Bars(symbol, "crypto", from.AddDate(0, 0, -3), to); err != nil {
						log.Error("bars livefill failure for key: [%v] (%v)", symbol, err)
					}
				}()
			}
			if firstLoop == true {
				filler.BackfillFrom.LoadOrStore(symbol, from)
				filler.BackfillMarket.LoadOrStore(symbol, "crypto")
			}
		}
		// Loop Forex Symbols
		for _, symbol := range qf.config.ForexSymbols {
			if filler.IsMarketOpen("forex", from) == true {
				// Market is open
				go func() {
					var err error
					if err = filler.Bars(symbol, "forex", from, to); err != nil {
						log.Error("bars livefill failure for key: [%v] (%v)", symbol, err)
					}
				}()
			} else if firstLoop == true {
				// Market is closed but we just started pentagon
				go func() {
					var err error
					if err = filler.Bars(symbol, "forex", from.AddDate(0, 0, -3), to); err != nil {
						log.Error("bars livefill failure for key: [%v] (%v)", symbol, err)
					}
				}()
			}
			if firstLoop == true {
				filler.BackfillFrom.LoadOrStore(symbol, from)
				filler.BackfillMarket.LoadOrStore(symbol, "forex")
			}
		}
		// Loop Equity Symbols
		for _, symbol := range qf.config.EquitySymbols {
			if filler.IsMarketOpen("equity", from) == true {
				// Market is open
				go func() {
					var err error
					if err = filler.Bars(symbol, "equity", from, to); err != nil {
						log.Error("bars livefill failure for key: [%v] (%v)", symbol, err)
					}
				}()
			} else if firstLoop == true {
				// Market is closed but we just started pentagon
				go func() {
					var err error
					if err = filler.Bars(symbol, "equity", from.AddDate(0, 0, -3), to); err != nil {
						log.Error("bars livefill failure for key: [%v] (%v)", symbol, err)
					}
				}()
			}
			if firstLoop == true {
				filler.BackfillFrom.LoadOrStore(symbol, from)
				filler.BackfillMarket.LoadOrStore(symbol, "equity")
			}
		}
		// Start backfill and disable first loop
		if firstLoop == true {
			go qf.workBackfillBars()
			firstLoop = false
		}
		// Update from and to dates
		from = from.Add(time.Minute)
		to = to.Add(time.Minute)
	}

}

func (qf *QuanateeFetcher) workBackfillBars() {

	ticker := time.NewTicker(90 * time.Second)

	for range ticker.C {
		
		wg := sync.WaitGroup{}
		count := 0

		// range over symbols that need backfilling, and
		// backfill them from the last written record
		filler.BackfillFrom.Range(func(key, value interface{}) bool {
			symbol := key.(string)
			marketType, _ := filler.BackfillMarket.Load(key)
			// make sure epoch value isn't nil (i.e. hasn't
			// been backfilled already)
			if value != nil {
				go func() {
					wg.Add(1)
					defer wg.Done()

					// backfill the symbol in parallel
					stop := qf.backfillBars(symbol, marketType.(string), value.(time.Time))
					if stop == true {
						log.Info("%s backfill completed. Last input: %v", symbol, value.(time.Time))
						filler.BackfillFrom.Store(key, nil)
					} else {
						filler.BackfillFrom.LoadOrStore(key, nil)
					}
				}()
			}

			// limit 12 goroutines per CPU core
			if count >= runtime.NumCPU()*12 {
				return false
			}

			return true
		})
		wg.Wait()

	}
}

// Backfill bars from start
func (qf *QuanateeFetcher) backfillBars(symbol, marketType string, end time.Time) bool {

	var (
		start time.Time
		end   time.Time
		from time.Time
		err  error
		tbk  = io.NewTimeBucketKey(fmt.Sprintf("%s/1Min/Price", symbol))
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

	// query the latest entry prior to the streamed record	
	instance := executor.ThisInstance
	cDir := instance.CatalogDir
	q := planner.NewQuery(cDir)
	q.AddTargetKey(tbk)
	q.SetRowLimit(io.LAST, 1)
	q.SetEnd(endEpoch - int64(time.Minute.Seconds()))

	parsed, err := q.Parse()
	if err != nil {
		log.Error("query parse failure (%v)", err)
		return true
	}

	scanner, err := executor.NewReader(parsed)
	if err != nil {
		log.Error("new scanner failure (%v)", err)
		return true
	}

	csm, err := scanner.Read()
	if err != nil {
		log.Error("scanner read failure (%v)", err)
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

	to := from.AddDate(0, 0, 14)
	if to.Unix() >= end.Unix() {
		to = end
		stop = true
	}
	// log.Info("%s backfill from %v to %v, stop:%v", symbol, from, to, stop)
	
	// request & write the missing bars
	if err = filler.Bars(symbol, marketType, from, to); err != nil {
		log.Error("bars backfill failure for key: [%v] (%v)", tbk.String(), err)
	}
	
	return stop
}

func main() {}
