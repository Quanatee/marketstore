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
)

func Bars(symbol string, from, to time.Time) (err error) {
	if from.IsZero() {
		from = time.Date(2014, 1, 1, 0, 0, 0, 0, NY)
	}

	if to.IsZero() {
		to = time.Now()
	}

	resp, err := api4polygon.GetHistoricAggregates(symbol, "minute", from, to, nil)
	if err != nil {
		return err
	}

	if len(resp.Ticks) == 0 {
		return
	}

	tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/OHLCV")
	csm := io.NewColumnSeriesMap()

	epoch := make([]int64, len(resp.Ticks))
	open := make([]float32, len(resp.Ticks))
	high := make([]float32, len(resp.Ticks))
	low := make([]float32, len(resp.Ticks))
	close := make([]float32, len(resp.Ticks))
	volume := make([]int32, len(resp.Ticks))

	for i, bar := range resp.Ticks {
		epoch[i] = bar.EpochMilliseconds / 1000
		open[i] = float32(bar.Open)
		high[i] = float32(bar.High)
		low[i] = float32(bar.Low)
		close[i] = float32(bar.Close)
		volume[i] = int32(bar.Volume)
	}

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", close)
	cs.AddColumn("Volume", volume)
	csm.AddColumnSeries(*tbk, cs)

	return executor.WriteCSM(csm, false)
}
