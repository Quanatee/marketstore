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
		log.Error("[polygon] bars livefill failure for: [%s] (%v)", symbol, err)
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
		log.Error("[tiingo] bars livefill failure for: [%s] (%v)", symbol, err)
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

	Open := make([]float32, len(Epochs))
	High := make([]float32, len(Epochs))
	Low := make([]float32, len(Epochs))
	Close := make([]float32, len(Epochs))
	HLC := make([]float32, len(Epochs))
	Volume := make([]float32, len(Epochs))
	
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
		Open = append(Open, open / float32(len(ohlcvs)))
		High = append(High, high / float32(len(ohlcvs)))
		Low = append(Low, low / float32(len(ohlcvs)))
		Close = append(Close, close / float32(len(ohlcvs)))
		HLC = append(HLC, hlc / float32(len(ohlcvs)))
		Volume = append(Volume, volume)
	}
	
	log.Info("livefill.Bars(%s) from %v to %v", symbol, from.Unix(), to.Unix())
	log.Info("livefill.Bars(%s) Epochs(%v)", symbol, Epochs)
	log.Info("livefill.Bars(%s) HLC(%v)", symbol, HLC)
	
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", Epochs)
	cs.AddColumn("Open", Open)
	cs.AddColumn("High", High)
	cs.AddColumn("Low", Low)
	cs.AddColumn("HLC", HLC)
	cs.AddColumn("Volume", Volume)

	tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/Price")
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)

	return executor.WriteCSM(csm, false)
}