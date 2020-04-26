package backfill

import (
	"fmt"
	//"math"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4polygon"
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4tiingo"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
)

var (
	ErrRetry  = fmt.Errorf("retry error")
	BackfillM *sync.Map
)

func Bars(symbol, marketType string, from, to time.Time) (err error) {

	if from.IsZero() {
		from = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	if to.IsZero() {
		to = time.Now()
	}
	
	ohlcv, err := api4polygon.GetAggregates(symbol, marketType, "1", "minute", from, to)
	if err != nil {
		log.Error("[polygon] bars livefill failure for: [%v] (%v)", tbk.String(), err)
		// return err
	}
	
	ohlcv2, err2 := api4tiingo.GetAggregates(symbol, marketType, "1", "min", from, to)
	if err2 != nil {
		log.Error("[tiingo] bars livefill failure for: [%v] (%v)", tbk.String(), err2)
		// return err2
	}
	
	log.Info("backfill.Bars(%s) ohlcv1(%v) ohlcv2(%v)", symbol, len(ohlcv.Epoch), len(ohlcv2.Epoch))

	if len(ohlcv.Epoch) == 0 && len(ohlcv2.Epoch) == 0 {
		return
	}
	
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
