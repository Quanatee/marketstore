package livefill

import (
	"fmt"
	//"math"
	//"sync"
	"time"

	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4polygon"
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4tiingo"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
)

var (
	ErrRetry  = fmt.Errorf("retry error")
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
	
	if len(ohlcv.Epoch) == 0 && len(ohlcv2.Epoch) == 0 {
		return
	}

	log.Info("livefill.Bars(%s) from %v to %v", symbol, from.Unix(), to.Unix())
	log.Info("livefill.Bars(%s) ohlcv1(%v) ohlcv2(%v), ohlcv(%v) ohlcv2[0](%v)", symbol, len(ohlcv.Epoch), len(ohlcv2.Epoch), ohlcv.Epoch[0], ohlcv2.Epoch[0])
	log.Info("livefill.Bars(%s) ohlcv1(%v) ohlcv2(%v), ohlcv(%v) ohlcv2[0](%v)", symbol, len(ohlcv.Epoch), len(ohlcv2.Epoch), ohlcv.Close[0], ohlcv2.Close[0])
	
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
