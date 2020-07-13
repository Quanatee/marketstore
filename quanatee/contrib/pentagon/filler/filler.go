package filler

import (
	"fmt"
	//"math"
	"sync"
	"time"
	"strings"
	
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api"
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4polygon"
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4tiingo"
	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4twelve"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
)

var (
	ErrRetry  = fmt.Errorf("retry error")
	BackfillFrom *sync.Map
	BackfillMarket *sync.Map
)

func Bars(wg *sync.WaitGroup, symbol, marketType string, from, to time.Time, timeframes []string) {
	defer wg.Done()
	if from.IsZero() {
		from = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	if to.IsZero() {
		to = time.Now()
	}
	
	ohlcvs := map[string]api.OHLCV{}
	
	// If crypto
	if strings.Compare(marketType, "crypto") == 0 && strings.HasSuffix(symbol, "USD") {

		// USDT
		ohlcv_pgt := GetDataFromProvider("polygon", symbol+"T", marketType, from, to)
		if len(ohlcv_pgt.HLC) > 0 {
			ohlcvs["polygon_usdt"] = ohlcv_pgt
		}
		ohlcv_tit := GetDataFromProvider("tiingo", symbol+"T", marketType, from, to)
		if len(ohlcv_tit.HLC) > 0 {
			ohlcvs["tiingo_usdt"] = ohlcv_tit
		}
		ohlcv_twt := GetDataFromProvider("twelve", symbol+"T", marketType, from, to)
		if len(ohlcv_twt.HLC) > 0 {
			ohlcvs["twelve_usdt"] = ohlcv_twt
		}
		
	} else {
		
		// Polygon
		ohlcv := GetDataFromProvider("polygon", symbol, marketType, from, to)
		if len(ohlcv.HLC) > 0 {
			ohlcvs["polygon"] = ohlcv
		}
		// Tiingo
		ohlcv_ti := GetDataFromProvider("tiingo", symbol, marketType, from, to)
		if len(ohlcv_ti.HLC) > 0 {
			ohlcvs["tiingo"] = ohlcv_ti
		}
		// Twelvedata
		ohlcv_tw := GetDataFromProvider("twelve", symbol, marketType, from, to)
		if len(ohlcv_tw.HLC) > 0 {
			ohlcvs["twelve"] = ohlcv_tw
		}
		
	}
	
	// Get the Epoch slice of the largest OHLCV set
	Epochs_ := make([]int64, 0)
	
    for _, ohlcv_ := range ohlcvs {
		for key := range ohlcv_.HLC {
			Epochs_ = append(Epochs_, key)
		}
	}
	
	Epochs_ = removeDuplicatesInt64(Epochs_)
	if len(Epochs_) == 0 {
		return
	}
	var Epochs []int64
	var Opens, Highs, Lows, Closes, Volumes, HLCs, TVALs, Spreads, Splits []float32
	
	symbolSplits_, _ := api.PolygonSplitEvents.Load(symbol)
	var symbolSplits map[time.Time]float32
	if symbolSplits_ != nil {
		symbolSplits = symbolSplits_.(map[time.Time]float32)
	}
	for _, Epoch := range Epochs_ {
		var open, high, low, close, volume, hlc, tval, spread float32
		divisor := float32(0)
		volume_divisor := float32(0)
		split := float32(1)
		// Calculate the total split ratio for the epoch
		if len(symbolSplits) > 0 {
			for expiryDate, ratio := range symbolSplits {
				if time.Unix(Epoch, 0).Before(expiryDate) {
					split = float32(split * ratio)
				}
			}
		}
		for _, ohlcv_ := range ohlcvs {
			if ( (ohlcv_.Open[Epoch] != 0 && ohlcv_.High[Epoch] != 0 && ohlcv_.Low[Epoch] != 0 && ohlcv_.Close[Epoch] != 0) &&
				(ohlcv_.Volume[Epoch] != 0) &&
				(ohlcv_.HLC[Epoch] != 0) &&
				(ohlcv_.TVAL[Epoch] != 0) &&
				(ohlcv_.Spread[Epoch] != 0) ) {
				
				open += float32(ohlcv_.Open[Epoch] * split)
				high += float32(ohlcv_.High[Epoch] * split)
				low += float32(ohlcv_.Low[Epoch] * split)
				close += float32(ohlcv_.Close[Epoch] * split)
				hlc += float32(ohlcv_.HLC[Epoch] * split)
				spread += float32(ohlcv_.Spread[Epoch] * split)
				divisor += float32(1)
				if ohlcv_.Volume[Epoch] > 1 {
					volume += float32(ohlcv_.Volume[Epoch] / split)
					tval += float32(ohlcv_.TVAL[Epoch])
					volume_divisor += float32(1)
				}
			}
		}
		if divisor > 0 {
			Epochs = append(Epochs, Epoch)
			Opens = append(Opens, float32(open / divisor))
			Highs = append(Highs, float32(high / divisor))
			Lows = append(Lows, float32(low / divisor))
			Closes = append(Closes, float32(close / divisor))
			HLCs = append(HLCs, float32(hlc / divisor))
			Spreads = append(Spreads, float32(spread / divisor))
			Splits = append(Splits, split)
			if volume_divisor > 0 {
				Volumes = append(Volumes, float32(volume / volume_divisor))
				TVALs = append(TVALs, float32(tval / volume_divisor))
			} else {
				Volumes = append(Volumes, float32(1))
				TVALs = append(TVALs, float32(hlc / divisor))
			}
		}
	}
	
    sources := make([]string, 0, len(ohlcvs))
    for k := range ohlcvs {
        sources = append(sources, k)
    }
	if (to.Add(5*time.Minute)).After(time.Now()) {
		log.Debug("filler.Bars(%s) livefill via %v [from %v to %v] | Length(%v)", symbol, sources, from, to, len(Epochs))
	} else {
		log.Info("filler.Bars(%s) backfill via %v [from %v to %v] | Length(%v)", symbol, sources, from, to, len(Epochs))
	}
	
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", Epochs)
	cs.AddColumn("Open", Opens)
	cs.AddColumn("High", Highs)
	cs.AddColumn("Low", Lows)
	cs.AddColumn("Close", Closes)
	cs.AddColumn("Volume", Volumes)
	cs.AddColumn("HLC", HLCs)
	cs.AddColumn("TVAL", TVALs)
	cs.AddColumn("Spread", Spreads)
	cs.AddColumn("Split", Splits)

	tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/Price")
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)
	
	executor.WriteCSM(csm, false)
	
	api.WriteAggregates(marketType, symbol, "Price", timeframes, *cs, from, to)
	
}

func GetDataFromProvider(
	provider, symbol, marketType string,
	from, to time.Time) (api.OHLCV) {
	
	filltype := "backfill"
	if (to.Add(5*time.Minute)).After(time.Now()) {
		filltype = "livefill"
	}
	
	switch provider {
	case "polygon":
		ohlcv, err := api4polygon.GetAggregates(symbol, marketType, "1", "minute", from, to)
		if err != nil {
			log.Error("[polygon] %s %s bars from: %v to %v failure: (%v)", symbol, filltype, from, to, err)
		} else {
			if len(ohlcv.HLC) > 0 {
				reconstruct := api.OHLCV{
					Open: ohlcv.Open,
					High: ohlcv.High,
					Low: ohlcv.Low,
					Close: ohlcv.Close,
					Volume: ohlcv.Volume,
					HLC: ohlcv.HLC,
					TVAL: ohlcv.TVAL,
					Spread: ohlcv.Spread,
				}
				return reconstruct
			}
		}
	case "tiingo":
		return api.OHLCV{}
		ohlcv, err := api4tiingo.GetAggregates(symbol, marketType, "1", "min", from, to)
		if err != nil {
			log.Error("[tiingo] %s %s bars from: %v to %v failure: (%v)", symbol, filltype, from, to, err)
		} else {
			if len(ohlcv.HLC) > 0 {
				reconstruct := api.OHLCV{
					Open: ohlcv.Open,
					High: ohlcv.High,
					Low: ohlcv.Low,
					Close: ohlcv.Close,
					Volume: ohlcv.Volume,
					HLC: ohlcv.HLC,
					TVAL: ohlcv.TVAL,
					Spread: ohlcv.Spread,
				}
				return reconstruct
			}
		}
	case "twelve":
		ohlcv, err := api4twelve.GetAggregates(symbol, marketType, "1", "min", from, to)
		if err != nil {
			log.Error("[twelve] %s %s bars from: %v to %v failure: (%v)", symbol, filltype, from, to, err)
		} else {
			if len(ohlcv.HLC) > 0 {
				reconstruct := api.OHLCV{
					Open: ohlcv.Open,
					High: ohlcv.High,
					Low: ohlcv.Low,
					Close: ohlcv.Close,
					Volume: ohlcv.Volume,
					HLC: ohlcv.HLC,
					TVAL: ohlcv.TVAL,
					Spread: ohlcv.Spread,
				}
				return reconstruct
			}
		}
	}
	return api.OHLCV{}
}

func removeDuplicatesInt64(s []int64) []int64 {
	seen := make(map[int64]struct{}, len(s))
	j := 0
	for _, v := range s {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		s[j] = v
		j++
	}
	return s[:j]
}

func removeDuplicatesString(elements []string) []string {
    encountered := map[string]bool{}

    // Create a map of all unique elements.
    for v:= range elements {
        encountered[elements[v]] = true
    }

    // Place all keys from the map into a slice.
    result := []string{}
    for key, _ := range encountered {
        result = append(result, key)
    }
    return result
}
