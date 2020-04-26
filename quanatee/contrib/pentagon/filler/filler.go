package filler

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

type OHLCV_map struct {
	Open      map[int64]float32
	High      map[int64]float32
	Low       map[int64]float32
	Close     map[int64]float32
	HLC       map[int64]float32
	Volume    map[int64]float32
}

func Bars(symbol, marketType string, from, to time.Time) (err error) {
	if from.IsZero() {
		from = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	if to.IsZero() {
		to = time.Now()
	}
	
	// ohlcvs := make([]OHLCV_map, 0)
	var ohlcvs []OHLCV_map

	ohlcv_polygon, err := api4polygon.GetAggregates(symbol, marketType, "1", "minute", from, to)
	if err != nil {
		log.Error("[polygon] bars filler failure for: [%s] (%v)", symbol, err)
		// return err
	} else {
		if len(ohlcv_polygon.HLC) > 0 {
			reconstruct := OHLCV_map{
				Open: ohlcv_polygon.Open,
				High: ohlcv_polygon.High,
				Low: ohlcv_polygon.Low,
				Close: ohlcv_polygon.Close,
				HLC: ohlcv_polygon.HLC,
				Volume: ohlcv_polygon.Volume,
			}
			ohlcvs = append(ohlcvs, reconstruct)
		}
	}
	ohlcv_tiingo, err := api4tiingo.GetAggregates(symbol, marketType, "1", "min", from, to)
	if err != nil {
		log.Error("[tiingo] bars filler failure for: [%s] (%v)", symbol, err)
		// return err
	} else {
		if len(ohlcv_tiingo.HLC) > 0 {
			reconstruct := OHLCV_map{
				Open: ohlcv_polygon.Open,
				High: ohlcv_polygon.High,
				Low: ohlcv_polygon.Low,
				Close: ohlcv_polygon.Close,
				HLC: ohlcv_polygon.HLC,
				Volume: ohlcv_polygon.Volume,
			}
			ohlcvs = append(ohlcvs, reconstruct)
		}
	}

	// Get the Epoch slice of the largest OHLCV set
	Epochs := make([]int64, 0)

    for _, ohlcv_ := range ohlcvs {
		if len(ohlcv_.HLC) > len(Epochs) {
			// Epochs = make([]string, 0, len(len(ohlcv_.HLC)))
			for key := range ohlcv_.HLC {
				Epochs = append(Epochs, key)
			}
		}
	}

	// If length is 0, no data was returned
	if len(Epochs) == 0 {
		return
	}
	var Opens, Highs, Lows, Closes, HLCs, Volumes []float32
	
	for _, Epoch := range Epochs {
		var open, high, low, close, hlc, volume float32
		for _, ohlcv_ := range ohlcvs {
			if _, ok := ohlcv_.HLC[Epoch]; ok {
				open += float32(ohlcv_.Open[Epoch])
				high += float32(ohlcv_.High[Epoch])
				low += float32(ohlcv_.Low[Epoch])
				close += float32(ohlcv_.Close[Epoch])
				hlc += float32(ohlcv_.HLC[Epoch])
				volume += float32(ohlcv_.Volume[Epoch])
			}
		}
		Opens = append(Opens, open / float32(len(ohlcvs)))
		Highs = append(Highs, high / float32(len(ohlcvs)))
		Lows = append(Lows, low / float32(len(ohlcvs)))
		Closes = append(Closes, close / float32(len(ohlcvs)))
		HLCs = append(HLCs, hlc / float32(len(ohlcvs)))
		Volumes = append(Volumes, volume)
	}
	
	if len(Epochs) <= 3 {
		log.Info("filler.Bars(%s) livefill from %v to %v with %v sources | Epochs(%v) HLCs(%v)", symbol, from.Unix(), to.Unix(), len(ohlcvs), Epochs, HLCs)
	}

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", Epochs)
	cs.AddColumn("Open", Opens)
	cs.AddColumn("High", Highs)
	cs.AddColumn("Low", Lows)
	cs.AddColumn("Close", Closes)
	cs.AddColumn("HLC", HLCs)
	cs.AddColumn("Volume", Volumes)

	tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/Price")
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)

	return executor.WriteCSM(csm, false)
}