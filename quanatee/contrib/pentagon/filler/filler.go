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
	Spread    map[int64]float32
	TVAL      map[int64]float32
}

func Bars(symbol, marketType string, from, to time.Time) (err error) {
	if from.IsZero() {
		from = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	if to.IsZero() {
		to = time.Now()
	}
	
	var ohlcvs []OHLCV
	
	ohlcv := GetDataFromProvider("polygon", symbol, marketType, from, to)
	if len(ohlcv.HLC) > 0 {
		ohlcvs = append(ohlcvs, ohlcv)
		log.Debug("Adding Polygon %s A %v", symbol, len(ohlcvs))
	}
	
	if (to.Add(time.Minute)).After(time.Now()) {
		// Current task livefill
		if len(ohlcv.HLC) > 0 {
			// Randomly run alt providers at 50% chance per alt provider to ease api usage
			rand.Seed(GetRandSeed())
			if rand.Intn(2) == 1 {
				ohlcv_ti := GetDataFromProvider("tiingo", symbol, marketType, from, to)
				if len(ohlcv_ti.HLC) > 0 {
					ohlcvs = append(ohlcvs, ohlcv_ti)
					log.Debug("Adding Tiingo %s R %v", symbol, len(ohlcvs))
				}
			}
			rand.Seed(GetRandSeed())
			if rand.Intn(2) == 1 {
				ohlcv_tw := GetDataFromProvider("twelve", symbol, marketType, from, to)
				if len(ohlcv_tw.HLC) > 0 {
					ohlcvs = append(ohlcvs, ohlcv_tw)
					log.Debug("Adding Twelve %s R %v", symbol, len(ohlcvs))
				}
			}
		} else {
			// Run all alt providers since main provider failed to pull data
			ohlcv_ti := GetDataFromProvider("tiingo", symbol, marketType, from, to)
			if len(ohlcv_ti.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_ti)
				log.Debug("Adding Tiingo %s F %v", symbol, len(ohlcvs))
			}
			ohlcv_tw := GetDataFromProvider("twelve", symbol, marketType, from, to)
			if len(ohlcv_tw.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_tw)
				log.Debug("Adding Tiingo %s F %v", symbol, len(ohlcvs))
			}
		}
	} else {
		// Current task is backfill
		// Run all alt providers
		ohlcv_ti := GetDataFromProvider("tiingo", symbol, marketType, from, to)
		if len(ohlcv_ti.HLC) > 0 {
			ohlcvs = append(ohlcvs, ohlcv_ti)
			log.Debug("Adding Tiingo %s B %v", symbol, len(ohlcvs))
		}
		ohlcv_tw := GetDataFromProvider("twelve", symbol, marketType, from, to)
		if len(ohlcv_tw.HLC) > 0 {
			ohlcvs = append(ohlcvs, ohlcv_tw)
			log.Debug("Adding Twelve %s B %v", symbol, len(ohlcvs))
		}
	}

	// If crypto, we randomly mix fiat USD with stablecoins USDT and USDC to create a robust CRYPTO/USD
	if strings.Compare(marketType, "crypto") == 0 && strings.HasSuffix(symbol, "USD") {
		// BUSD
		rand.Seed(GetRandSeed())
		if rand.Intn(2) == 1 {
			ohlcv_pgb := GetDataFromProvider("polygon", symbol[:len(symbol)-3] + "B" + symbol[len(symbol)-3:], marketType, from, to)
			if len(ohlcv_pgb.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_pgb)
				log.Debug("Adding Polygon BUSD %s to %v", symbol, len(ohlcvs))
			}
		}
		rand.Seed(GetRandSeed())
		if rand.Intn(2) == 1 {
			ohlcv_tib := GetDataFromProvider("tiingo", symbol[:len(symbol)-3] + "B" + symbol[len(symbol)-3:], marketType, from, to)
			if len(ohlcv_tib.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_tib)
				log.Debug("Adding Tiingo BUSD %s to %v", symbol, len(ohlcvs))
			}
		}
		rand.Seed(GetRandSeed())
		if rand.Intn(2) == 1 {
			ohlcv_twb := GetDataFromProvider("twelve", symbol[:len(symbol)-3] + "B" + symbol[len(symbol)-3:], marketType, from, to)
			if len(ohlcv_twb.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_twb)
				log.Debug("Adding Twelve BUSD %s to %v", symbol, len(ohlcvs))
			}
		}
		// USDT
		rand.Seed(GetRandSeed())
		if rand.Intn(2) == 1 {
			ohlcv_pgt := GetDataFromProvider("polygon", symbol+"T", marketType, from, to)
			if len(ohlcv_pgt.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_pgt)
				log.Debug("Adding Polygon USDT %s to %v", symbol, len(ohlcvs))
			}
		}
		rand.Seed(GetRandSeed())
		if rand.Intn(2) == 1 {
			ohlcv_tit := GetDataFromProvider("tiingo", symbol+"T", marketType, from, to)
			if len(ohlcv_tit.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_tit)
				log.Debug("Adding Tiingo USDT %s to %v", symbol, len(ohlcvs))
			}
		}
		rand.Seed(GetRandSeed())
		if rand.Intn(2) == 1 {
			ohlcv_twt := GetDataFromProvider("twelve", symbol+"T", marketType, from, to)
			if len(ohlcv_twt.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_twt)
				log.Debug("Adding Twelve USDT %s to %v", symbol, len(ohlcvs))
			}
		}
		// USDC
		rand.Seed(GetRandSeed())
		if rand.Intn(2) == 1 {
			ohlcv_pgc := GetDataFromProvider("polygon", symbol+"C", marketType, from, to)
			if len(ohlcv_pgc.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_pgc)
				log.Debug("Adding Polygon USDC %s to %v", symbol, len(ohlcvs))
			}
		}
		rand.Seed(GetRandSeed())
		if rand.Intn(2) == 1 {
			ohlcv_tic := GetDataFromProvider("tiingo", symbol+"C", marketType, from, to)
			if len(ohlcv_tic.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_tic)
				log.Debug("Adding Tiingo USDC %s to %v", symbol, len(ohlcvs))
			}
		}
		rand.Seed(GetRandSeed())
		if rand.Intn(2) == 1 {
			ohlcv_twc := GetDataFromProvider("twelve", symbol+"C", marketType, from, to)
			if len(ohlcv_twc.HLC) > 0 {
				ohlcvs = append(ohlcvs, ohlcv_twc)
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

	var Opens, Highs, Lows, Closes, Volumes, HLCs, Spreads, Tvals []float32
	
	for _, Epoch := range Epochs {
		var open, high, low, close, volume, hlc, spread, tval float32
		for _, ohlcv_ := range ohlcvs {
			if _, ok := ohlcv_.HLC[Epoch]; ok {
				open += float32(ohlcv_.Open[Epoch])
				high += float32(ohlcv_.High[Epoch])
				low += float32(ohlcv_.Low[Epoch])
				close += float32(ohlcv_.Close[Epoch])
				volume += float32(ohlcv_.Volume[Epoch])
				hlc += float32(ohlcv_.HLC[Epoch])
				spread += float32(ohlcv_.Spread[Epoch])
				tval += float32(ohlcv_.TVAL[Epoch])
			}
		}
		Opens = append(Opens, open / float32(len(ohlcvs)))
		Highs = append(Highs, high / float32(len(ohlcvs)))
		Lows = append(Lows, low / float32(len(ohlcvs)))
		Closes = append(Closes, close / float32(len(ohlcvs)))
		Volumes = append(Volumes, volume)
		HLCs = append(HLCs, hlc / float32(len(ohlcvs)))
		Spreads = append(Spreads, spread / float32(len(ohlcvs)))
		Tvals = append(Tvals, tval)
	}
	
	if (to.Add(time.Minute)).After(time.Now()) {
		log.Info("filler.Bars(%s) livefill from %v to %v with %v sources | Length(%v)", symbol, from.Unix(), to.Unix(), len(ohlcvs), len(Epochs))
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
	cs.AddColumn("TVAL", Tvals)

	tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/Price")
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)

	return executor.WriteCSM(csm, false)
}


func GetDataFromProvider(
	provider, symbol, marketType string,
	from, to time.Time) (OHLCV) {
	
	filltype := "backfill"
	if (to.Add(time.Minute)).After(time.Now()) {
		filltype = "livefill"
	}
	
	switch provider {
	case "polygon":
		ohlcv, err := api4polygon.GetAggregates(symbol, marketType, "1", "minute", from, to)
		if err != nil {
			if !strings.Contains(err.Error(), "status code 400") {
				log.Error("[polygon] bars %s failure for: [%s] (%v)", filltype, symbol, err)
			}
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
					TVAL: ohlcv.TVAL,
				}
				return reconstruct
			}
		}
	case "tiingo":
		if strings.Compare(marketType, "crypto") == 0 && (to.Add(time.Minute)).After(time.Now()) == false {
			// Disable backfill
			return OHLCV{}
		}
		if strings.Compare(marketType, "forex") == 0 && (to.Add(time.Minute)).After(time.Now()) == false {
			// Disable backfill
			return OHLCV{}
		}
		ohlcv, err := api4tiingo.GetAggregates(symbol, marketType, "1", "min", from, to)
		if err != nil {
			if !strings.Contains(err.Error(), "status code 400") {
				log.Error("[tiingo] bars %s failure for: [%s] (%v)", filltype, symbol, err)
			}
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
					TVAL: ohlcv.TVAL,
				}
				return reconstruct
			}
		}
	case "twelve":
		ohlcv, err := api4twelve.GetAggregates(symbol, marketType, "1", "min", from, to)
		if err != nil {
			if !strings.Contains(err.Error(), "status code 400") {
				log.Error("[twelve] bars %s failure for: [%s] (%v)", filltype, symbol, err)
			}
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
					TVAL: ohlcv.TVAL,
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