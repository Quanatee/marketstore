package main

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4polygon"
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/backfill"
	//"github.com/alpacahq/marketstore/contrib/polygon/handlers"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	//"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
)

type QuanateeFetcher struct {
	config FetcherConfig
}

type FetcherConfig struct {
	Symbols        []string `yaml:"symbols"`
    PolygonApiKey  string   `yaml:"polygon_api_key"`
    TiingoApiKey   string   `yaml:"tiingo_api_key"`
    twelveApiKey   string   `yaml:"polygon_api_key"`
	QueryStart     string   `yaml:"query_start"`
}

// NewBgWorker returns a new instances of QuanateeFetcher. See FetcherConfig
// for more details about configuring QuanateeFetcher.
func NewBgWorker(conf map[string]interface{}) (w bgworker.BgWorker, err error) {
	data, _ := json.Marshal(conf)
	config := FetcherConfig{}
	err = json.Unmarshal(data, &config)
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

	api4polygon.SetAPIKey(qf.config.PolygonApiKey)

	select {}
}

func (qf *QuanateeFetcher) workBackfillBars() {
	ticker := time.NewTicker(30 * time.Second)

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
					qf.backfillBars(symbol, *value.(*int64))
					backfill.BackfillM.Store(key, nil)
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
func (qf *QuanateeFetcher) backfillBars(symbol string, endEpoch int64) {
	var (
		from time.Time
		err  error
		tbk  = io.NewTimeBucketKey(fmt.Sprintf("%s/1Min/OHLCV", symbol))
	)

	// query the latest entry prior to the streamed record
	if qf.config.QueryStart == "" {
		instance := executor.ThisInstance
		cDir := instance.CatalogDir
		q := planner.NewQuery(cDir)
		q.AddTargetKey(tbk)
		q.SetRowLimit(io.LAST, 1)
		q.SetEnd(endEpoch - int64(time.Minute.Seconds()))

		parsed, err := q.Parse()
		if err != nil {
			log.Error("[polygon] query parse failure (%v)", err)
			return
		}

		scanner, err := executor.NewReader(parsed)
		if err != nil {
			log.Error("[polygon] new scanner failure (%v)", err)
			return
		}

		csm, err := scanner.Read()
		if err != nil {
			log.Error("[polygon] scanner read failure (%v)", err)
			return
		}

		epoch := csm[*tbk].GetEpoch()

		// no gap to fill
		if len(epoch) == 0 {
			return
		}

		from = time.Unix(epoch[len(epoch)-1], 0)

	} else {
		for _, layout := range []string{
			"2006-01-02 03:04:05",
			"2006-01-02T03:04:05",
			"2006-01-02 03:04",
			"2006-01-02T03:04",
			"2006-01-02",
		} {
			from, err = time.Parse(layout, qf.config.QueryStart)
			if err == nil {
				break
			}
		}
	}

	// request & write the missing bars
	if err = backfill.Bars(symbol, from, time.Time{}); err != nil {
		log.Error("[polygon] bars backfill failure for key: [%v] (%v)", tbk.String(), err)
	}
}

func main() {}
