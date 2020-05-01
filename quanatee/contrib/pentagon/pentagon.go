package main

import (
	//"encoding/json"
	"fmt"
	//"runtime"
	"math/rand"
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
	QueryStart time.Time
	TimeStarted time.Time
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
	
	startDate, _ := time.Parse("2006-01-02", config.QueryStart)
	
	return &QuanateeFetcher{
		config: config,
		QueryStart: startDate,
		TimeStarted: time.Now(),
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
	var wg sync.WaitGroup
	for {
		
		for {
			if time.Now().Unix() >= to.Unix() {
				break
			} else {
				time.Sleep(to.Sub(time.Now()))
				time.Sleep(1*time.Second)
			}
		}
		
		wg.Add(1)
		go qf.liveCrypto(&wg, from, to, firstLoop)
		wg.Add(1)
		go qf.liveForex(&wg, from, to, firstLoop)
		wg.Add(1)
		go qf.liveEquity(&wg, from, to, firstLoop)
		wg.Wait()

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

func (qf *QuanateeFetcher) liveCrypto(wg *sync.WaitGroup, from, to time.Time, firstLoop bool) {
	defer wg.Done()
	var wg2 sync.WaitGroup
	count := 0
	// Loop Crypto Symbols
	for _, symbol := range qf.config.CryptoSymbols {
		count++
		if count % 3 == 0 {
			time.Sleep(1*time.Second)
		}
		if filler.IsMarketOpen("crypto", from) == true {
			// Market is open
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "crypto", from, to)
		} else if firstLoop == true {
			// Market is closed but we just started pentagon
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "crypto", from.Add(-5000*time.Minute), to)
		}
		if firstLoop == true {
			filler.BackfillFrom.LoadOrStore(symbol, from)
			filler.BackfillMarket.LoadOrStore(symbol, "crypto")
		}
	}
	defer wg2.Wait()
}

func (qf *QuanateeFetcher) liveForex(wg *sync.WaitGroup, from, to time.Time, firstLoop bool) {
	defer wg.Done()
	var wg2 sync.WaitGroup
	count := 0
	// Loop Forex Symbols
	for _, symbol := range qf.config.ForexSymbols {
		count++
		if count % 7 == 0 {
			time.Sleep(1*time.Second)
		}
		if filler.IsMarketOpen("forex", from) == true {
			// Market is open
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "forex", from, to)
		} else if firstLoop == true {
			// Market is closed but we just started pentagon
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "forex", from.Add(-5000*time.Minute), to)
		}
		if firstLoop == true {
			filler.BackfillFrom.LoadOrStore(symbol, from)
			filler.BackfillMarket.LoadOrStore(symbol, "forex")
		}
	}
	defer wg2.Wait()
}
func (qf *QuanateeFetcher) liveEquity(wg *sync.WaitGroup, from, to time.Time, firstLoop bool) {
	defer wg.Done()
	var wg2 sync.WaitGroup
	count := 0
	checkSplit := rand.Intn(99)

	// Loop Equity Symbols
	for _, symbol := range qf.config.EquitySymbols {

		// Initalize splits data from polygon, or randomly check if there are new splits
		if firstLoop == true || checkSplit == 0 {
			api4polygon.GetSplits(symbol)
			// Check if symbol has splits
			splits := GetPreviousSplits
			for _, split := range splits.SplitData {
				issueDate, _ := time.Parse("2006-01-02", split.Issue)
				// Check if splits is after plugin was started and in the future
				if issueDate.Before(qf.TimeStarted) && issueDate.After(time.Now()) {
					// Bookmark the future split event
					api4polygon.SetUpcomingSplits[symbol] = issueDate
				}
			}
		}
		
		// Slow down requests
		count++
		if count % 13 == 0 {
			time.Sleep(1*time.Second)
		}
		if filler.IsMarketOpen("equity", from) == true {
			// Market is open
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "equity", from, to)
		} else if firstLoop == true {
			// Market is closed but we just started pentagon
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "equity", from.Add(-20000*time.Minute), to)
		}
		if firstLoop == true {
			filler.BackfillFrom.LoadOrStore(symbol, from)
			filler.BackfillMarket.LoadOrStore(symbol, "equity")
		}
		// If time has passed the issue date of future split event, trigger backfill
		issueDate := api4polygon.GetUpcomingSplits(symbol)
		if time.Now().After(issueDate) {
			// Delete bookmark of future split event
			api4polygon.DeleteUpcomingSplits(symbol)
			// Delete entire tbk
			tbk  := io.NewTimeBucketKey(fmt.Sprintf("%s/1Min/Price", symbol))
			err := executor.ThisInstance.CatalogDir.RemoveTimeBucket(tbk)
			if err != nil {
				log.Error("removal of catalog entry failed: %s", err.Error())
			}
			// Start new "firstLoop" request
			filler.Bars(&wg2, symbol, "equity", from.Add(-20000*time.Minute), to)
			// Retrigger Backfill
			filler.BackfillFrom.Store(symbol, from)
			filler.BackfillMarket.Store(symbol, "equity")
		}
	}
	defer wg2.Wait()
}

func (qf *QuanateeFetcher) workBackfillBars() {

	for {
		
		// Sleep to the next 30th second of the next minute
		next := time.Now().Add(time.Minute)
		next = time.Date(next.Year(), next.Month(), next.Day(), next.Hour(), next.Minute(), 30, 0, time.UTC)
		time.Sleep(next.Sub(time.Now()))

		wg := sync.WaitGroup{}
		count := 0
		// range over symbols that need backfilling, and
		// backfill them from the last written record
		filler.BackfillFrom.Range(func(key, value interface{}) bool {
			// Delay for 1 second every 3 requests
			count++
			if count % 3 == 0 {
				time.Sleep(time.Second)
			}
			symbol := key.(string)
			marketType, _ := filler.BackfillMarket.Load(key)
			// make sure epoch value isn't nil (i.e. hasn't
			// been backfilled already)
			if value != nil {
				go func() {
					wg.Add(1)
					defer wg.Done()
					force := false
					// backfill the symbol
					stop := qf.backfillBars(symbol, marketType.(string), value.(time.Time))
					if stop == true {
						log.Info("%s backfill stopped. Last input: %v", symbol, value.(time.Time))
						filler.BackfillFrom.Store(key, nil)
					}
				}()
			}
			return true
		})
		wg.Wait()
		
	}
}

// Backfill bars from start
func (qf *QuanateeFetcher) backfillBars(symbol, marketType string, end time.Time) bool {

	var (
		from time.Time
		tbk  = io.NewTimeBucketKey(fmt.Sprintf("%s/1Min/Price", symbol))
	)
	
	// query the latest entry prior to the streamed record	
	instance := executor.ThisInstance
	cDir := instance.CatalogDir
	q := planner.NewQuery(cDir)
	q.AddTargetKey(tbk)
	q.SetRowLimit(io.LAST, 1)
	
	switch marketType {
	case "equity":
		q.SetEnd(end.Add(-20000*time.Minute).Unix() - int64(time.Minute.Seconds()))
    default:
		q.SetEnd(end.Add(-5000*time.Minute).Unix() - int64(time.Minute.Seconds()))
	}
	parsed, err := q.Parse()
	if err != nil {
		log.Error("%s query parse failure (%v), symbol data not available.", err)
		return true
	}

	scanner, err := executor.NewReader(parsed)
	if err != nil {
		log.Error("%s new scanner failure (%v)", err)
		return true
	}

	csm, err := scanner.Read()
	if err != nil {
		log.Error("%s scanner read failure (%v)", err)
		return true
	}

	epoch := csm[*tbk].GetEpoch()
	stop := false

	// has gap to fill
	if len(epoch) != 0 {
		from = time.Unix(epoch[len(epoch)-1], 0)
	} else {
		from = qf.QueryStart
	}
		
	to := from
	// Keep requests under 5000 rows (Twelvedata limit). Equity gets more due to operating hours
	switch marketType {
	case "equity":
		to = to.Add(20000*time.Minute)
	default:
		to = to.Add(5000*time.Minute)
	}
	if to.Unix() >= end.Unix() {
		to = end
		stop = true
	}
	// log.Info("%s backfill from %v to %v, stop:%v", symbol, from, to, stop)
	
	// request & write the missing bars
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go filler.Bars(&wg2, symbol, marketType, from, to)
	wg2.Wait()
	return stop
}

func main() {}
