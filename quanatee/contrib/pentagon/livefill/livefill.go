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

type OHLCV_map struct {
	Open      map[int64]float32
	High      map[int64]float32
	Low       map[int64]float32
	HLC     map[int64]float32
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
	
	var ohlcvs []OHLCV
	
	ohlcv_polygon, err := api4polygon.GetAggregates(symbol, marketType, "1", "minute", from, to)
	if err != nil {
		log.Error("[polygon] bars livefill failure for: [%v] (%v)", tbk.String(), err)
		// return err
	} else {
		if len(ohlcv_polygon.HLC) > 0 {
			ohlcvs = ohlcvs.append(ohlcvs, ohlcv_polygon)
		}
	}
	ohlcv_tiingo, err := api4tiingo.GetAggregates(symbol, marketType, "1", "min", from, to)
	if err != nil {
		log.Error("[tiingo] bars livefill failure for: [%v] (%v)", tbk.String(), err)
		// return err
	} else {
		if len(ohlcv_tiingo.HLC) > 0 {
			ohlcvs = ohlcvs.append(ohlcvs, ohlcv_tiingo)
		}
	}

	// Get the Epoch slice of the largest OHLCV set
	var Epochs []int64
	
    for index, ohlcv_ := range ohlcvs {
		if len(ohlcv_.HLC) > len(Epoch) {
			Epoch = make([]string, 0, len(len(ohlcv_.HLC)))
			for key := range ohlcv_.HLC {
				Epoch = append(Epoch, key)
			}
		}
	}

	// If length is 0, no data was returned
	if len(Epoch) == 0 {
		return
	}

	Open := make([]float32, length)
	High := make([]float32, length)
	Low := make([]float32, length)
	Close := make([]float32, length)
	HLC := make([]float32, length)
	Volume := make([]float32, length)
	
	for _, Epoch := range Epochs {
		var open, high, low, close, hlc, volume float32
		for _, ohlcv_ := range ohlcvs {
			if ohlcv_.HLC[Epoch] {
				open += ohlcv_.Open[Epoch]
				high += ohlcv_.High[Epoch]
				low += ohlcv_.Low[Epoch]
				close += ohlcv_.Close[Epoch]
				hlc += ohlcv_.HLC[Epoch]
				volume += ohlcv_.Volume[Epoch]
			}
		}
		Open = append(Open, open / len(ohlcvs))
		High = append(High, high / len(ohlcvs))
		Low = append(Low, low / len(ohlcvs))
		Close = append(Close, close / len(ohlcvs))
		HLC = append(HLC, hlc / len(ohlcvs))
		Volume = append(Volume, volume)
	}
	
	log.Info("livefill.Bars(%s) from %v to %v", symbol, from.Unix(), to.Unix())
	log.Info("livefill.Bars(%s) HLC(%v)", symbol, len(HLC))
	
	tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/OHLCV")
	csm := io.NewColumnSeriesMap()
	
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", Epoch)
	cs.AddColumn("Open", Open)
	cs.AddColumn("High", High)
	cs.AddColumn("Low", Low)
	cs.AddColumn("HLC", HLC)
	cs.AddColumn("Volume", Volume)
	csm.AddColumnSeries(*tbk, cs)

	return executor.WriteCSM(csm, false)
}