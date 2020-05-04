package filler

import (
	"fmt"
	//"math"
	"math/rand"
	"sync"
	"time"
	"strings"
	crypto_rand "crypto/rand"
    "encoding/binary"
	
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

type OHLCV struct {
	Open      map[int64]float32
	High      map[int64]float32
	Low       map[int64]float32
	Close     map[int64]float32
	Volume    map[int64]float32
	HLC       map[int64]float32
	TVAL      map[int64]float32
	Spread    map[int64]float32
	Split     map[int64]float32
}

func Bars(wg *sync.WaitGroup, symbol, marketType string, from, to time.Time) {
	defer wg.Done()
	if from.IsZero() {
		from = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	if to.IsZero() {
		to = time.Now()
	}
	
	var ohlcvs []OHLCV
	var sources []string

	ohlcv := GetDataFromProvider("polygon", symbol, marketType, from, to)
	if len(ohlcv.HLC) > 0 {
		ohlcvs = append(ohlcvs, ohlcv)
		sources = append(sources, "polygon")
		log.Debug("Adding Polygon %s A %v", symbol, len(ohlcvs))
	}
	if len(ohlcv.HLC) > 0 {
		// Randomly run alt providers to ease api usage
		rand.Seed(GetRandSeed())
		if rand.Intn(3) <= 1 {
			ohlcv_ti := GetDataFromProvider("tiingo", symbol, marketType, from, to)
			if len(ohlcv_ti.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_ti)
				sources = append(sources, "tiingo")
				log.Debug("Adding Tiingo %s R %v", symbol, len(ohlcvs))
			}
		} else {
			ohlcv_tw := GetDataFromProvider("twelve", symbol, marketType, from, to)
			if len(ohlcv_tw.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_tw)
				sources = append(sources, "twelve")
				log.Debug("Adding Twelve %s R %v", symbol, len(ohlcvs))
			}
		}
	} else {
		// Run all alt providers since main provider failed to pull data
		ohlcv_ti := GetDataFromProvider("tiingo", symbol, marketType, from, to)
		if len(ohlcv_ti.HLC) > 0 {
			ohlcvs = append(ohlcvs, ohlcv_ti)
			sources = append(sources, "tiingo")
			log.Debug("Adding Tiingo %s F %v", symbol, len(ohlcvs))
		}
		ohlcv_tw := GetDataFromProvider("twelve", symbol, marketType, from, to)
		if len(ohlcv_tw.HLC) > 0 {
			ohlcvs = append(ohlcvs, ohlcv_tw)
			sources = append(sources, "twelve")
			log.Debug("Adding Tiingo %s F %v", symbol, len(ohlcvs))
		}
	}
	
	// If crypto, we randomly mix fiat USD with stablecoins USDT and USDC to create a robust CRYPTO/USD
	if strings.Compare(marketType, "crypto") == 0 && strings.HasSuffix(symbol, "USD") {
		// BUSD
		ohlcv_pgb := GetDataFromProvider("polygon", symbol[:len(symbol)-3] + "B" + symbol[len(symbol)-3:], marketType, from, to)
		if len(ohlcv_pgb.HLC) > 0 {
			ohlcvs = append(ohlcvs, ohlcv_pgb)
			log.Debug("Adding Polygon BUSD %s to %v", symbol, len(ohlcvs))
		}
		rand.Seed(GetRandSeed())
		if rand.Intn(2) <= 1 {
			ohlcv_tib := GetDataFromProvider("tiingo", symbol[:len(symbol)-3] + "B" + symbol[len(symbol)-3:], marketType, from, to)
			if len(ohlcv_tib.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_tib)
				log.Debug("Adding Tiingo BUSD %s to %v", symbol, len(ohlcvs))
			}
		} else {
			ohlcv_twb := GetDataFromProvider("twelve", symbol[:len(symbol)-3] + "B" + symbol[len(symbol)-3:], marketType, from, to)
			if len(ohlcv_twb.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_twb)
				sources = append(sources, "twelve")
				log.Debug("Adding Twelve BUSD %s to %v", symbol, len(ohlcvs))
			}
		}
		// USDT
		ohlcv_pgt := GetDataFromProvider("polygon", symbol+"T", marketType, from, to)
		if len(ohlcv_pgt.HLC) > 0 {
			ohlcvs = append(ohlcvs, ohlcv_pgt)
			sources = append(sources, "polygon")
			log.Debug("Adding Polygon USDT %s to %v", symbol, len(ohlcvs))
		}
		rand.Seed(GetRandSeed())
		if rand.Intn(2) <= 1 {
			ohlcv_tit := GetDataFromProvider("tiingo", symbol+"T", marketType, from, to)
			if len(ohlcv_tit.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_tit)
				sources = append(sources, "tiingo")
				log.Debug("Adding Tiingo USDT %s to %v", symbol, len(ohlcvs))
			}
		} else {
			ohlcv_twt := GetDataFromProvider("twelve", symbol+"T", marketType, from, to)
			if len(ohlcv_twt.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_twt)
				sources = append(sources, "twelve")
				log.Debug("Adding Twelve USDT %s to %v", symbol, len(ohlcvs))
			}
		}
		// USDC
		ohlcv_pgc := GetDataFromProvider("polygon", symbol+"C", marketType, from, to)
		if len(ohlcv_pgc.HLC) > 0 {
			ohlcvs = append(ohlcvs, ohlcv_pgc)
			sources = append(sources, "polygon")
			log.Debug("Adding Polygon USDC %s to %v", symbol, len(ohlcvs))
		}
		rand.Seed(GetRandSeed())
		if rand.Intn(2) <= 1 {
			ohlcv_tic := GetDataFromProvider("tiingo", symbol+"C", marketType, from, to)
			if len(ohlcv_tic.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_tic)
				sources = append(sources, "tiingo")
				log.Debug("Adding Tiingo USDC %s to %v", symbol, len(ohlcvs))
			}
		} else {
			ohlcv_twc := GetDataFromProvider("twelve", symbol+"C", marketType, from, to)
			if len(ohlcv_twc.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_twc)
				sources = append(sources, "twelve")
				log.Debug("Adding Twelve USDC %s to %v", symbol, len(ohlcvs))
			}
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

	var Opens, Highs, Lows, Closes, Volumes, HLCs, TVALs, Spreads, Splits []float32
	
	symbolSplits_, _ := api4polygon.SplitEvents.Load(symbol)
	var symbolSplits map[time.Time]float32
	if symbolSplits_ != nil {
		symbolSplits = symbolSplits_.(map[time.Time]float32)
	}
	for _, Epoch := range Epochs {
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
				(ohlcv_.Open[Epoch] != ohlcv_.Close[Epoch]) && 
				(ohlcv_.High[Epoch] != ohlcv_.Low[Epoch]) &&
				(ohlcv_.Volume[Epoch] != 0) &&
				(ohlcv_.HLC[Epoch] != 0) &&
				(ohlcv_.TVAL[Epoch] != 0) &&
				(ohlcv_.Spread[Epoch] != 0) ) {
				open += float32(ohlcv_.Open[Epoch] / split)
				high += float32(ohlcv_.High[Epoch] / split)
				low += float32(ohlcv_.Low[Epoch] / split)
				close += float32(ohlcv_.Close[Epoch] / split)
				hlc += float32(ohlcv_.HLC[Epoch] / split)
				spread += float32(ohlcv_.Spread[Epoch] / split)
				divisor += float32(1)
				if ohlcv_.Volume[Epoch] != 1 {
					volume += float32(ohlcv_.Volume[Epoch] * split)
					tval += float32(ohlcv_.TVAL[Epoch])
					volume_divisor += float32(1)
				}
			}
		}
		if divisor > 0 {
			Opens = append(Opens, float32(open / divisor))
			Highs = append(Highs, float32(high / divisor))
			Lows = append(Lows, float32(low / divisor))
			Closes = append(Closes, float32(close / divisor))
			HLCs = append(HLCs, float32(hlc / divisor))
			Spreads = append(Spreads, float32(spread / divisor))
			Splits = append(Splits, split)
		}
		if volume_divisor > 0 {
			Volumes = append(Volumes, float32(volume / volume_divisor))
			TVALs = append(TVALs, float32(tval / volume_divisor))
		} else {
			Volumes = append(Volumes, float32(1))
			TVALs = append(TVALs, float32(hlc / divisor))
		}
	}
	
	if (to.Add(5*time.Minute)).After(time.Now()) {
		log.Info("filler.Bars(%s) livefill via %v [from %v to %v] | Length(%v)", symbol, removeDuplicatesUnordered(sources), from, to, len(Epochs))
	} else {
		log.Info("filler.Bars(%s) backfill via %v [from %v to %v] | Length(%v)", symbol, removeDuplicatesUnordered(sources), from, to, len(Epochs))
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
	
}


func GetDataFromProvider(
	provider, symbol, marketType string,
	from, to time.Time) (OHLCV) {
	
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
				reconstruct := OHLCV{
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
		// Disable temporarily
		return OHLCV{}
		ohlcv, err := api4tiingo.GetAggregates(symbol, marketType, "1", "min", from, to)
		if err != nil {
			log.Error("[tiingo] %s %s bars from: %v to %v failure: (%v)", symbol, filltype, from, to, err)
		} else {
			if len(ohlcv.HLC) > 0 {
				reconstruct := OHLCV{
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
		// Twelve is not stable
		return OHLCV{}
		ohlcv, err := api4twelve.GetAggregates(symbol, marketType, "1", "min", from, to)
		if err != nil {
			log.Error("[twelve] %s %s bars from: %v to %v failure: (%v)", symbol, filltype, from, to, err)
		} else {
			if len(ohlcv.HLC) > 0 {
				reconstruct := OHLCV{
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
	return OHLCV{}
}

func IsMarketOpen(
	marketType string,
	from time.Time) (bool) {

	switch marketType {
	case "crytpo":
		return true
	case "forex":
		if ( 
			( from.Weekday() == 0 && from.Hour() >= 22 ) ||
			( from.Weekday() >= 1 && from.Weekday() <= 4 ) ||
			( from.Weekday() == 5 && from.Hour() < 21 ) ) {
			return true
		} else {
			return false
		}
	case "equity":
		if ( 
			( from.Weekday() >= 1 && from.Weekday() <= 5 ) &&
			( from.Hour() >= 13 && from.Hour() < 21 ) ) {
			return true
		} else {
			return false
		}
	}

	return true
}


func GetRandSeed() (int64) {
	var b [8]byte
	_, err := crypto_rand.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand package with cryptographically secure random number generator")
	}
	return int64(binary.LittleEndian.Uint64(b[:]))
}

func removeDuplicatesUnordered(elements []string) []string {
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
