package main

import (
	//"encoding/json"
	"fmt"
	"sync"
	"time"
	
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api"
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
	FuturesSymbols  []string `yaml:"futures_symbols"`
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
	api.InitalizeSharedMaps()
	
	startDate, _ := time.Parse("2006-01-02 03:04", config.QueryStart)
	
	return &QuanateeFetcher{
		config: config,
		QueryStart: startDate,
		TimeStarted: time.Now(),
	}, nil
}

const (
	
	crypto_limit = 7
	forex_limit  = 7
	equity_limit = 21
	futures_limit  = 7
)

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

	log.Info("Scanning for previous stock split events and fetching historical daily volume:")
	for _, symbol := range qf.config.CryptoSymbols {
		api4polygon.UpdateDailyVolumes(symbol, "crypto", qf.QueryStart)
		//api4tiingo.UpdateDailyVolumes(symbol, qf.QueryStart)
	}
	for _, symbol := range qf.config.ForexSymbols {
		api4polygon.UpdateDailyVolumes(symbol, "forex", qf.QueryStart)
		//api4tiingo.UpdateDailyVolumes(symbol, qf.QueryStart)
	}
	for _, symbol := range qf.config.EquitySymbols {
		api4polygon.UpdateDailyVolumes(symbol, "equity", qf.QueryStart)
		//api4tiingo.UpdateDailyVolumes(symbol, qf.QueryStart)
		api4polygon.UpdateSplitEvents(symbol, qf.TimeStarted)
	}
	for _, symbol := range qf.config.FuturesSymbols {
		api4polygon.UpdateDailyVolumes(symbol, "futures", qf.QueryStart)
		//api4tiingo.UpdateDailyVolumes(symbol, qf.QueryStart)
		api4polygon.UpdateSplitEvents(symbol, qf.TimeStarted)
	}
	log.Info("Scan complete.")
	
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
				log.Info("Sleeping for %v s", to.Sub(time.Now()))
				time.Sleep(to.Sub(time.Now()))
			}
		}
		
		log.Info("Waking up to process from %v to %v", from, to)
		
		wg.Add(1)
		go qf.liveCrypto(&wg, from, to, firstLoop)
		wg.Add(1)
		go qf.liveForex(&wg, from, to, firstLoop)
		wg.Add(1)
		go qf.liveEquity(&wg, from, to, firstLoop)
		wg.Add(1)
		go qf.liveFutures(&wg, from, to, firstLoop)
		wg.Wait()
		log.Info("Livefill cycle completed.")

		// Start backfill and disable first loop
		if firstLoop == true {
			go qf.workBackfillBars()
			go qf.DailyChecker()
			firstLoop = false
		}
		// Update from and to dates
		from = from.Add(time.Minute)
		to = from.Add(time.Minute)
		to = to.Add(1*time.Second)

	}

}

func (qf *QuanateeFetcher) liveCrypto(wg *sync.WaitGroup, from, to time.Time, firstLoop bool) {
	defer wg.Done()
	var wg2 sync.WaitGroup
	// Loop Crypto Symbols
	for _, symbol := range qf.config.CryptoSymbols {
		if firstLoop == true {
			// Market is closed but we just started pentagon
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "crypto", from.AddDate(0, 0, -crypto_limit), to)
			filler.BackfillFrom.LoadOrStore(symbol, qf.QueryStart)
			filler.BackfillMarket.LoadOrStore(symbol, "crypto")
		} else if filler.IsMarketOpen("crypto", from) == true {
			// Market is open
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "crypto", from, to)
		}
	}
	wg2.Wait()
	log.Debug("Livefill crypto completed.")
}

func (qf *QuanateeFetcher) liveForex(wg *sync.WaitGroup, from, to time.Time, firstLoop bool) {
	defer wg.Done()
	var wg2 sync.WaitGroup
	// Loop Forex Symbols
	for _, symbol := range qf.config.ForexSymbols {
		if firstLoop == true {
			// Market is closed but we just started pentagon
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "forex", from.AddDate(0, 0, -forex_limit), to)
			filler.BackfillFrom.LoadOrStore(symbol, qf.QueryStart)
			filler.BackfillMarket.LoadOrStore(symbol, "forex")
		} else if filler.IsMarketOpen("forex", from) == true {
			// Market is open
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "forex", from, to)
		}
	}
	wg2.Wait()
	log.Debug("Livefill forex completed.")
}

func (qf *QuanateeFetcher) liveEquity(wg *sync.WaitGroup, from, to time.Time, firstLoop bool) {
	defer wg.Done()
	var wg2 sync.WaitGroup
	// Loop Equity Symbols
	for _, symbol := range qf.config.EquitySymbols {
		if firstLoop == true {
			// Market is closed but we just started pentagon
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "equity", from.AddDate(0, 0, -equity_limit), to)
			filler.BackfillFrom.LoadOrStore(symbol, qf.QueryStart)
			filler.BackfillMarket.LoadOrStore(symbol, "equity")
		} else if filler.IsMarketOpen("equity", from) == true {
			// Market is open
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "equity", from, to)
		}
	}
	wg2.Wait()
	log.Debug("Livefill equity completed.")
}

func (qf *QuanateeFetcher) liveFutures(wg *sync.WaitGroup, from, to time.Time, firstLoop bool) {
	defer wg.Done()
	var wg2 sync.WaitGroup
	// Loop Futures Symbols
	for _, symbol := range qf.config.FuturesSymbols {
		if firstLoop == true {
			// Market is closed but we just started pentagon
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "futures", from.AddDate(0, 0, -futures_limit), to)
			filler.BackfillFrom.LoadOrStore(symbol, qf.QueryStart)
			filler.BackfillMarket.LoadOrStore(symbol, "futures")
		} else if filler.IsMarketOpen("futures", from) == true {
			// Market is open
			wg2.Add(1)
			go filler.Bars(&wg2, symbol, "futures", from, to)
		}
	}
	wg2.Wait()
	log.Debug("Livefill futures completed.")
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
					to := qf.backfillBars(symbol, marketType.(string), value.(time.Time))
					if to.Unix() >= qf.TimeStarted.Unix() {
						log.Info("%s backfill stopped. Last input: %v", symbol, value.(time.Time))
						filler.BackfillFrom.Store(key, nil)
						// Remove historical daily volume, keep only daily volume after time start just in case livefeed volume fails
						symbolPolygonDailyVolume_, _ := api.PolygonDailyVolumes.Load(symbol)
						if symbolPolygonDailyVolume_ != nil {
							symbolDailyVolume := symbolPolygonDailyVolume_.(map[time.Time]float32)
							var newDailyVolume map[time.Time]float32
							for date, volume := range symbolDailyVolume {
								if date.After(qf.TimeStarted) {
									newDailyVolume[date] = volume
								}
							}
							api.PolygonDailyVolumes.Store(symbol, symbolDailyVolume)
						}
						symbolTiingoDailyVolume_, _ := api.TiingoDailyVolumes.Load(symbol)
						if symbolTiingoDailyVolume_ != nil {
							symbolDailyVolume := symbolTiingoDailyVolume_.(map[time.Time]float32)
							var newDailyVolume map[time.Time]float32
							for date, volume := range symbolDailyVolume {
								if date.After(qf.TimeStarted) {
									newDailyVolume[date] = volume
								}
							}
							api.TiingoDailyVolumes.Store(symbol, symbolDailyVolume)
						}
					} else {
						// Set to as the next from
						filler.BackfillFrom.Store(key, to)
					}
				}()
			}
			return true
		})
		wg.Wait()
		
	}
}

func (qf *QuanateeFetcher) DailyChecker() {
	
	for {

		// Run at 2:00 NY time
		next := time.Now().AddDate(0, 0, 1)
		next = time.Date(next.Year(), next.Month(), next.Day(), 7, 0, 0, 0, time.UTC)
		time.Sleep(next.Sub(time.Now()))

		wg := sync.WaitGroup{}
		log.Info("Updating recent daily volumes...")
		for _, symbol := range qf.config.CryptoSymbols {
			api4polygon.UpdateDailyVolumes(symbol, "crypto", qf.TimeStarted)
			api4tiingo.UpdateDailyVolumes(symbol, qf.TimeStarted)
		}
		for _, symbol := range qf.config.ForexSymbols {
			api4polygon.UpdateDailyVolumes(symbol, "forex", qf.TimeStarted)
			api4tiingo.UpdateDailyVolumes(symbol, qf.TimeStarted)
		}
		for _, symbol := range qf.config.EquitySymbols {
			api4polygon.UpdateDailyVolumes(symbol, "equity", qf.TimeStarted)
			api4tiingo.UpdateDailyVolumes(symbol, qf.TimeStarted)
			api4polygon.UpdateSplitEvents(symbol, qf.TimeStarted)
		}
		for _, symbol := range qf.config.FuturesSymbols {
			api4polygon.UpdateDailyVolumes(symbol, "futures", qf.TimeStarted)
			api4tiingo.UpdateDailyVolumes(symbol, qf.TimeStarted)
			api4polygon.UpdateSplitEvents(symbol, qf.TimeStarted)
		}
		log.Info("Checking for split events...")
		for _, symbol := range qf.config.EquitySymbols {
			
			rebackfill_pg := api4polygon.UpdateSplitEvents(symbol, qf.TimeStarted)
			nil_if_not_backfilling, _ := filler.BackfillMarket.Load(symbol)

			if rebackfill_pg == true && nil_if_not_backfilling == nil {
				
				log.Info("%s has a equity split event today, removing history and restarting backfill...", symbol)
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
					filler.Bars(&wg, symbol, "equity", from.AddDate(0, 0, -equity_limit), to)
					// Retrigger Backfill
					filler.BackfillFrom.Store(symbol, from)
					filler.BackfillMarket.Store(symbol, "equity")
				}()
			}
		}
		for _, symbol := range qf.config.FuturesSymbols {
			
			rebackfill_pg := api4polygon.UpdateSplitEvents(symbol, qf.TimeStarted)
			nil_if_not_backfilling, _ := filler.BackfillMarket.Load(symbol)

			if rebackfill_pg == true && nil_if_not_backfilling == nil {
				
				log.Info("%s has a futures split event today, removing history and restarting backfill...", symbol)
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
					filler.Bars(&wg, symbol, "futures", from.AddDate(0, 0, -futures_limit), to)
					// Retrigger Backfill
					filler.BackfillFrom.Store(symbol, from)
					filler.BackfillMarket.Store(symbol, "futures")
				}()
			}
		}

		wg.Wait()
	}

}

// Backfill bars from start
func (qf *QuanateeFetcher) backfillBars(symbol, marketType string, from time.Time) (time.Time) {

	var (
		tbk  = io.NewTimeBucketKey(fmt.Sprintf("%s/1Min/Price", symbol))
	)
	
	// query the latest entry prior to the streamed record	
	instance := executor.ThisInstance
	cDir := instance.CatalogDir
	q := planner.NewQuery(cDir)
	q.AddTargetKey(tbk)
	q.SetRowLimit(io.LAST, 1)
	
	end := qf.TimeStarted
	q.SetEnd(end.Unix())
	
	if from.IsZero() {
		// Dynamically find missing values
		parsed, err := q.Parse()
		if err != nil {
			log.Error("%s query parse failure (%v), symbol data not available.", err)
			from = qf.QueryStart
		}
		scanner, err := executor.NewReader(parsed)
		if err != nil {
			log.Error("%s new scanner failure (%v)", err)
			from = qf.QueryStart
		}
		csm, err := scanner.Read()
		if err != nil {
			log.Error("%s scanner read failure (%v)", err)
			from = qf.QueryStart
		}
		epoch := csm[*tbk].GetEpoch()
		if len(epoch) != 0 {
			from = time.Unix(epoch[len(epoch)-1], 0)
		} else {
			from = qf.QueryStart
		}
	}
	
	to := from
	// Keep requests under 5000 rows (Twelvedata limit). Equity gets more due to operating hours
	switch marketType {
	case "crypto":
		to = to.AddDate(0, 0, crypto_limit)
	case "forex":
		to = to.AddDate(0, 0, forex_limit)
	case "equity":
		to = to.AddDate(0, 0, equity_limit)
	case "futures":
		to = to.AddDate(0, 0, futures_limit)
	default:
		to = to.AddDate(0, 0, equity_limit)
	}
	
	// log.Info("%s backfill from %v to %v, stop:%v", symbol, from, to, stop)
	
	// request & write the missing bars
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go filler.Bars(&wg2, symbol, marketType, from, to)
	wg2.Wait()
	return to
}

func main() {}