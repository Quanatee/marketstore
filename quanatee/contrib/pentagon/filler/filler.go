package filler

import (
	"fmt"
	//"math"
	"math/rand"
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

type OHLCV struct {
	Open      map[int64]float32
	High      map[int64]float32
	Low       map[int64]float32
	Close     map[int64]float32
	Volume    map[int64]float32
	HLC       map[int64]float32
	Spread    map[int64]float32
	VWAP      map[int64]float32
}

func Bars(symbol, marketType string, from, to time.Time) (err error) {
	if from.IsZero() {
		from = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	if to.IsZero() {
		to = time.Now()
	}
	
	// ohlcvs := make([]OHLCV, 0)
	var ohlcvs []OHLCV
	
	time.Now()
	
	// Check current task livefill
	if (to.Add(time.Minute)).After(time.Now()) {

	} else {
		// Current task is ackfill
	}
	
	ohlcv, _ := GetDataFromProvider("polygon", symbol, marketType, from, to)
	
	if (to.Add(time.Minute)).After(time.Now()) {
		// Current task livefill
		if len(ohlcv.HLC) > 0 {
			// Randomly run alt providers at 34% chance per alt provider
			if rand.Intn(3) == 0 {
				ohlcv, _ := GetDataFromProvider("tiingo", symbol, marketType, from, to)
				if len(ohlcv.HLC) > 0 {
					ohlcvs = append(ohlcvs, ohlcv)
				}
			}
			// if rand.Intn(3) == 0 {
			// 	ohlcv, _ := GetDataFromProvider("twelve", symbol, marketType, from, to)
			// 	if len(ohlcv.HLC) > 0 {
			// 		ohlcvs = append(ohlcvs, ohlcv)
			// 	}
			// }
		} else {
			// Run all alt providers since main provider  failed
			ohlcv, _ := GetDataFromProvider("tiingo", symbol, marketType, from, to)
			if len(ohlcv.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv)
			}
			// ohlcv, _ := GetDataFromProvider("twelve", symbol, marketType, from, to)
			// if len(ohlcv.HLC) > 0 {
			// 	ohlcvs = append(ohlcvs, ohlcv)
			// }
		}
	} else {
		// Current task is backfill
		// Run all alt providers
		ohlcv, _ := GetDataFromProvider("tiingo", symbol, marketType, from, to)
		if len(ohlcv.HLC) > 0 {
			ohlcvs = append(ohlcvs, ohlcv)
		}
		// ohlcv, _ := GetDataFromProvider("twelve", symbol, marketType, from, to)
		// if len(ohlcv.HLC) > 0 {
		// 	ohlcvs = append(ohlcvs, ohlcv)
		// }
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
	var Opens, Highs, Lows, Closes, Volumes, HLCs, Spreads, Vwaps []float32
	
	for _, Epoch := range Epochs {
		var open, high, low, close, volume, hlc, spread, vwap float32
		for _, ohlcv_ := range ohlcvs {
			if _, ok := ohlcv_.HLC[Epoch]; ok {
				open += float32(ohlcv_.Open[Epoch])
				high += float32(ohlcv_.High[Epoch])
				low += float32(ohlcv_.Low[Epoch])
				close += float32(ohlcv_.Close[Epoch])
				volume += float32(ohlcv_.Volume[Epoch])
				hlc += float32(ohlcv_.HLC[Epoch])
				spread += float32(ohlcv_.Spread[Epoch])
				vwap += float32(ohlcv_.VWAP[Epoch])
			}
		}
		Opens = append(Opens, open / float32(len(ohlcvs)))
		Highs = append(Highs, high / float32(len(ohlcvs)))
		Lows = append(Lows, low / float32(len(ohlcvs)))
		Closes = append(Closes, close / float32(len(ohlcvs)))
		Volumes = append(Volumes, volume)
		HLCs = append(HLCs, hlc / float32(len(ohlcvs)))
		Spreads = append(Spreads, spread / float32(len(ohlcvs)))
		Vwaps = append(Vwaps, vwap / float32(len(ohlcvs)))
	}
	
	if len(Epochs) <= 3 {
		log.Info("filler.Bars(%s) livefill from %v to %v with %v sources | Epochs(%v) HLCs(%v)", symbol, from.Unix(), to.Unix(), len(ohlcvs), Epochs, HLCs)
	} else {
		log.Info("filler.Bars(%s) backfill from %v to %v with %v sources | Length(%v)", symbol, from.Unix(), to.Unix(), len(ohlcvs), len(Epochs))
	}

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", Epochs)
	cs.AddColumn("Open", Opens)
	cs.AddColumn("High", Highs)
	cs.AddColumn("Low", Lows)
	cs.AddColumn("Close", Closes)
	cs.AddColumn("Volume", Volumes)
	cs.AddColumn("HLC", HLCs)
	cs.AddColumn("Spread", Spreads)
	cs.AddColumn("VWAP", Vwaps)

	tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/Price")
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)

	return executor.WriteCSM(csm, false)
}


func GetDataFromProvider(
	provider, symbol, marketType string,
	from, to time.Time) (OHLCV, error) {
	
	switch provider {
	case "polygon":
		ohlcv, err := api4polygon.GetAggregates(symbol, marketType, "1", "minute", from, to)
		if err != nil {
			log.Error("[polygon] bars filler failure for: [%s] (%v)", symbol, err)
		} else {
			if len(ohlcv.HLC) > 0 {
				reconstruct := OHLCV{
					Open: ohlcv.Open,
					High: ohlcv.High,
					Low: ohlcv.Low,
					Close: ohlcv.Close,
					Volume: ohlcv.Volume,
					HLC: ohlcv.HLC,
					Spread: ohlcv.Spread,
					VWAP: ohlcv.VWAP,
				}
				return reconstruct, err
			}
		}
	case "tiingo":
		ohlcv, err := api4tiingo.GetAggregates(symbol, marketType, "1", "min", from, to)
		if err != nil {
			log.Error("[tiingo] bars filler failure for: [%s] (%v)", symbol, err)
		} else {
			if len(ohlcv.HLC) > 0 {
				reconstruct := OHLCV{
					Open: ohlcv.Open,
					High: ohlcv.High,
					Low: ohlcv.Low,
					Close: ohlcv.Close,
					Volume: ohlcv.Volume,
					HLC: ohlcv.HLC,
					Spread: ohlcv.Spread,
					VWAP: ohlcv.VWAP,
				}
				return reconstruct, err
			}
		}
	// case "twelve":
	// 	ohlcv, err := api4twelve.GetAggregates(symbol, marketType, "1", "minute", from, to)
	// 	if err != nil {
	// 		log.Error("[twelve] bars filler failure for: [%s] (%v)", symbol, err)
	// 	} else {
	// 		if len(ohlcv.HLC) > 0 {
	// 			reconstruct := OHLCV{
	// 				Open: ohlcv.Open,
	// 				High: ohlcv.High,
	// 				Low: ohlcv.Low,
	// 				Close: ohlcv.Close,
	// 				Volume: ohlcv.Volume,
	// 				HLC: ohlcv.HLC,
	// 				Spread: ohlcv.Spread,
	// 				VWAP: ohlcv.VWAP,
	// 			}
	// 			return reconstruct, err
	// 		}
	// 	}
	}
	return &OHLCV{}, err
}