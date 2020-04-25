package backfill

import (
	"fmt"
	//"math"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4polygon"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	//"github.com/alpacahq/marketstore/utils/log"
)

var (
	// NY timezone
	NY, _     = time.LoadLocation("America/New_York")
	ErrRetry  = fmt.Errorf("retry error")
	BackfillM *sync.Map
	MarketType string
	PolygonPrefix = map[string]string{
		"forex": "C:",
		"crypto": "X:",
		"stocks": "",
	}
)

func SetMarketType(marketType string) {
	MarketType = marketType
}

func Bars(symbol string, from, to time.Time) (err error) {
	if from.IsZero() {
		from = time.Date(2017, 1, 1, 0, 0, 0, 0, NY)
	}

	if to.IsZero() {
		to = time.Now()
	}
	
	ohlcv, err := api4polygon.GetPastAggregates(PolygonPrefix[MarketType]+symbol, "1", "minute", from, to)
	if err != nil {
		return err
	}

	if len(ohlcv.Epoch) == 0 {
		return
	}
	
	log.Info("Backfill: %s %v %v", symbol, from, to)
	
	tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/OHLCV")
	csm := io.NewColumnSeriesMap()
	
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", ohlcv.Epoch)
	cs.AddColumn("Open", ohlcv.Open)
	cs.AddColumn("High", ohlcv.High)
	cs.AddColumn("Low", ohlcv.Low)
	cs.AddColumn("Close", ohlcv.Close)
	cs.AddColumn("Volume", ohlcv.Volume)
	csm.AddColumnSeries(*tbk, cs)

	return executor.WriteCSM(csm, false)
}
