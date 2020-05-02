package main

import (
	//"encoding/json"
	"fmt"
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
	api4polygon.SplitEvents = &sync.Map{}
	api4polygon.UpcomingSplitEvents = &sync.Map{}
	api4tiingo.SplitEvents = &sync.Map{}
	api4tiingo.UpcomingSplitEvents = &sync.Map{}

	startDate, _ := time.Parse("2006-01-02 03:04", config.QueryStart)
	
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

	for _, symbol := range qf.config.EquitySymbols {
		api4polygon.UpdateSplits(symbol, qf.TimeStarted)
		api4tiingo.UpdateSplits(symbol, qf.TimeStarted)
	}

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
			go qf.checkStockSplits()
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
	wg2.Wait()
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
	wg2.Wait()
}
func (qf *QuanateeFetcher) liveEquity(wg *sync.WaitGroup, from, to time.Time, firstLoop bool) {
	defer wg.Done()
	var wg2 sync.WaitGroup
	count := 0
	
	// Loop Equity Symbols
	for _, symbol := range qf.config.EquitySymbols {

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
	}
	wg2.Wait()
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

func (qf *QuanateeFetcher) checkStockSplits() {
	
	for {

		// Run at 2:00 NY time
		next := time.Now().AddDate(0, 0, 1)
		next = time.Date(next.Year(), next.Month(), next.Day(), 7, 0, 0, 0, time.UTC)
		time.Sleep(next.Sub(time.Now()))
		
		log.Info("Checking for stock splits happening today...")
		wg := sync.WaitGroup{}
		
		for _, symbol := range qf.config.EquitySymbols {
			
			rebackfill_pg := api4polygon.UpdateSplits(symbol, qf.TimeStarted)
			rebackfill_ti := api4tiingo.UpdateSplits(symbol, qf.TimeStarted)
			
			nil_if_not_backfilling, _ := filler.BackfillMarket.Load(symbol)

			if ( rebackfill_pg == true || rebackfill_ti == true ) && nil_if_not_backfilling == nil {
				
				log.Info("%s has a stock split event today, removing history and restarting backfill...", symbol)
				// Delete entire tbk
				tbk  := io.NewTimeBucketKey(fmt.Sprintf("%s/1Min/Price", symbol))
				err := executor.ThisInstance.CatalogDir.RemoveTimeBucket(tbk)
				if err != nil {
					log.Error("removal of catalog entry failed: %s", err.Error())
				}
				// Start new "firstLoop" request
				from := time.Now().Add(time.Minute)
				from = time.Date(from.Year(), from.Month(), from.Day(), from.Hour(), from.Minute(), 0, 0, time.UTC)
				to := from.Add(time.Minute)
				to = to.Add(1*time.Second)
				go func() {
					wg.Add(1)
					filler.Bars(&wg, symbol, "equity", from.Add(-20000*time.Minute), to)
					// Retrigger Backfill
					filler.BackfillFrom.Store(symbol, from)
					filler.BackfillMarket.Store(symbol, "equity")
				}()
			}
		}

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
		end = end.Add(-20000*time.Minute).Add(-1*time.Minute)
		q.SetEnd(end.Unix())
    default:
		end = end.Add(-5000*time.Minute).Add(-1*time.Minute)
		q.SetEnd(end.Unix())
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
