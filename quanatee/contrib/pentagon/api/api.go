package api

import (
	"sync"
	"time"
	"sort"
	"math/rand"
	crypto_rand "crypto/rand"
    "encoding/binary"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/log"
)

var (
	// Served by Polygon
	PolygonSplitEvents *sync.Map
	PolygonUpcomingSplitEvents *sync.Map
	PolygonDailyVolumes *sync.Map
	// Served by Tiingo
	TiingoDailyVolumes *sync.Map

	// Candle Building
	LivefillAggCache *sync.Map
	BackfillAggCache *sync.Map
)

type Slice struct {
    sort.Interface
    idx []int
}

func (s Slice) Swap(i, j int) {
    s.Interface.Swap(i, j)
    s.idx[i], s.idx[j] = s.idx[j], s.idx[i]
}

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

type cachedAgg struct {
	cs         io.ColumnSeries
	from, to time.Time
}

func (c *cachedAgg) Valid(from, to time.Time) bool {
	return from.Unix() >= c.from.Unix() && to.Unix() <= c.to.Unix()
}

func InitalizeSharedMaps() {
	PolygonSplitEvents = &sync.Map{}
	PolygonUpcomingSplitEvents = &sync.Map{}
	PolygonDailyVolumes = &sync.Map{}
	TiingoDailyVolumes = &sync.Map{}
	LivefillAggCache = &sync.Map{}
	BackfillAggCache = &sync.Map{}
}

func GetRandIntn(n int) (int) {
	
	var b [8]byte
	_, err := crypto_rand.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand package with cryptographically secure random number generator")
	}
	seed := int64(binary.LittleEndian.Uint64(b[:]))
	rand.Seed(seed)
	return rand.Intn(n)
}

func IsMarketOpen(
	marketType string,
	from time.Time) (bool) {
	
	switch marketType {
	case "crytpo":
		return IsCryptoMarketOpen(from.Unix())
	case "forex":
		return IsForexMarketOpen(from.Unix())
	case "equity":
		return IsEquityMarketOpen(from.Unix())
	}
	return true
}

func IsCryptoMarketOpen(epoch int64) bool {
	t := time.Unix(epoch, 0)
	if t.IsZero() == false {
		return true
	} else {
		return false
	}
	return true
}

func IsForexMarketOpen(epoch int64) bool {
	t := time.Unix(epoch, 0)
	if ( 
		( t.Weekday() == 0 && t.Hour() >= 22 ) ||
		( t.Weekday() >= 1 && t.Weekday() <= 4 ) ||
		( t.Weekday() == 5 && t.Hour() <= 21 ) ) {
		return true
	} else {
		return false
	}
	return true
}

func IsEquityMarketOpen(epoch int64) bool {
	t := time.Unix(epoch, 0)
	if ( 
		( t.Weekday() >= 1 && t.Weekday() <= 5 ) &&
		( t.Hour() >= 13 && t.Hour() <= 21 ) ) {
		return true
	} else {
		return false
	}
	return true
}

func GetAlternateVolumePolygonFirst(symbol, marketType string, Epoch int64, to, from time.Time) (float32) {
	
	dt := time.Unix(Epoch, 0)
	// Try provider daily volume with options for livefill and backfill
	volume_alt := false
	symbolDailyVolume_, _ := PolygonDailyVolumes.Load(symbol)
	if symbolDailyVolume_ != nil {
		symbolDailyVolume := symbolDailyVolume_.(map[time.Time]float32)
		dailyVolume := float32(1)
		if (to.Add(5*time.Minute)).After(time.Now()) {
			// Livefill, get the last daily volume
			last_date := time.Time{}
			for date := range symbolDailyVolume {
				if date.After(last_date) {
					last_date = date
				}
			}
			dailyVolume, _ = symbolDailyVolume[time.Date(last_date.Year(), last_date.Month(), last_date.Day(), 0, 0, 0, 0, time.UTC)]
		} else {
			// Backfill, directly retrieve the daily volume
			dailyVolume, _ = symbolDailyVolume[time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC)]
		}
		if dailyVolume != 0 {
			switch marketType {
			case "crytpo":
				return float32(dailyVolume/1440)
			case "equity":
				return float32(dailyVolume/390)
			case "forex":
				return float32(dailyVolume/1440)
			default:
				volume_alt = true
			}
		} else {
			volume_alt = true
		}
	} else {
		volume_alt = true
	}
	if volume_alt == true {
		// Try alternative daily volume, or set to 1
		symbolDailyVolume_, _ := TiingoDailyVolumes.Load(symbol)
		if symbolDailyVolume_ != nil {
			symbolDailyVolume := symbolDailyVolume_.(map[time.Time]float32)
			dailyVolume := float32(1)
			if (to.Add(5*time.Minute)).After(time.Now()) {
				// Livefill, get the last daily volume
				last_date := time.Time{}
				for date := range symbolDailyVolume {
					if date.After(last_date) {
						last_date = date
					}
				}
				dailyVolume, _ = symbolDailyVolume[time.Date(last_date.Year(), last_date.Month(), last_date.Day(), 0, 0, 0, 0, time.UTC)]
			} else {
				// Backfill, directly retrieve the daily volume
				dailyVolume, _ = symbolDailyVolume[time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC)]
			}
			if dailyVolume != 0 {
				switch marketType {
				case "crytpo":
					return float32(dailyVolume/1440)
				case "forex":
					return float32(dailyVolume/1440)
				case "equity":
					return float32(dailyVolume/390)
				default:
					return float32(1)
				}
			} else {
				return float32(1)
			}
		} else {
			return float32(1)
		}
	}
	return float32(1)
}

func GetAlternateVolumeTiingoFirst(symbol, marketType string, Epoch int64, to, from time.Time) (float32) {
	
	dt := time.Unix(Epoch, 0)
	// Try provider daily volume with options for livefill and backfill
	volume_alt := false
	symbolDailyVolume_, _ := TiingoDailyVolumes.Load(symbol)
	if symbolDailyVolume_ != nil {
		symbolDailyVolume := symbolDailyVolume_.(map[time.Time]float32)
		dailyVolume := float32(1)
		if (to.Add(5*time.Minute)).After(time.Now()) {
			// Livefill, get the last daily volume
			last_date := time.Time{}
			for date := range symbolDailyVolume {
				if date.After(last_date) {
					last_date = date
				}
			}
			dailyVolume, _ = symbolDailyVolume[time.Date(last_date.Year(), last_date.Month(), last_date.Day(), 0, 0, 0, 0, time.UTC)]
		} else {
			// Backfill, directly retrieve the daily volume
			dailyVolume, _ = symbolDailyVolume[time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC)]
		}
		if dailyVolume != 0 {
			switch marketType {
			case "crytpo":
				return float32(dailyVolume/1440)
			case "equity":
				return float32(dailyVolume/390)
			case "forex":
				return float32(dailyVolume/1440)
			default:
				volume_alt = true
			}
		} else {
			volume_alt = true
		}
	} else {
		volume_alt = true
	}
	if volume_alt == true {
		// Try alternative daily volume, or set to 1
		symbolDailyVolume_, _ := PolygonDailyVolumes.Load(symbol)
		if symbolDailyVolume_ != nil {
			symbolDailyVolume := symbolDailyVolume_.(map[time.Time]float32)
			dailyVolume := float32(1)
			if (to.Add(5*time.Minute)).After(time.Now()) {
				// Livefill, get the last daily volume
				last_date := time.Time{}
				for date := range symbolDailyVolume {
					if date.After(last_date) {
						last_date = date
					}
				}
				dailyVolume, _ = symbolDailyVolume[time.Date(last_date.Year(), last_date.Month(), last_date.Day(), 0, 0, 0, 0, time.UTC)]
			} else {
				// Backfill, directly retrieve the daily volume
				dailyVolume, _ = symbolDailyVolume[time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC)]
			}
			if dailyVolume != 0 {
				switch marketType {
				case "crytpo":
					return float32(dailyVolume/1440)
				case "forex":
					return float32(dailyVolume/1440)
				case "equity":
					return float32(dailyVolume/390)
				default:
					return float32(1)
				}
			} else {
				return float32(1)
			}
		} else {
			return float32(1)
		}
	}
	return float32(1)
}

func WriteAggregates(
	marketType, symbol, bucket string,
	timeframes []string,
	min_cs io.ColumnSeries,
	from, to time.Time) {

	cs := io.NewColumnSeries()
	tbk := io.NewTimeBucketKeyFromString(symbol + "/" + "1Min" + "/" + bucket)
	
	if (to.Add(5*time.Minute)).After(time.Now()) {
		if v, ok := LivefillAggCache.Load(tbk.String()); ok {
			min_c := v.(*cachedAgg)
			// Trim cs to keep only one days worth and store it
			start := to.AddDate(0, 0, -1).Unix()
			end := to.Unix()
			trimmed_cs, _ := io.SliceColumnSeriesByEpoch(min_cs, &start, &end)
			LivefillAggCache.Store(tbk.String(), &cachedAgg{
				cs:   trimmed_cs,
				from: to.AddDate(0, 0, -1),
				to: to,
			})
			cs = io.ColumnSeriesUnion(&min_cs, &min_c.cs)
		} else {
			cs = &min_cs
		}
	} else {
		if v, ok := BackfillAggCache.Load(tbk.String()); ok {
			min_c := v.(*cachedAgg)
			// Trim cs to keep only one days worth and store it
			start := to.AddDate(0, 0, -1).Unix()
			end := to.Unix()
			trimmed_cs, _ := io.SliceColumnSeriesByEpoch(min_cs, &start, &end)
			BackfillAggCache.Store(tbk.String(), &cachedAgg{
				cs:   trimmed_cs,
				from: to.AddDate(0, 0, -1),
				to: to,
			})
			cs = io.ColumnSeriesUnion(&min_cs, &min_c.cs)
		} else {
			cs = &min_cs
		}
	}

	epochs_int64 := cs.GetColumn("Epoch").([]int64)
	epochs_int := make([]int, len(epochs_int64))
	for i := range epochs_int64 {
		epochs_int[i] = int(epochs_int64[i])
	}
	// Returns the indices that would sort cs
	indices := Sort(sort.IntSlice(epochs_int))
	
	for column_key, column_values_ := range cs.GetColumns() {
		switch column_key {
		case "Epoch":
			column_values := column_values_.([]int64)
			var sorted_values []int64
			for _, index := range indices {	
				sorted_values = append(sorted_values, column_values[index])
			}
			cs.Remove("Epoch")
			cs.AddColumn("Epoch", sorted_values)
		default:
			column_values := column_values_.([]float32)
			var sorted_values []float32
			for _, index := range indices {
				sorted_values = append(sorted_values, column_values[index])
			}
			cs.Remove(column_key)
			cs.AddColumn(column_key, sorted_values)
		}
	}
	
	for _, timeframe := range timeframes {

		aggTbk := io.NewTimeBucketKeyFromString(symbol + "/" + timeframe + "/" + bucket)
		timeframe_duration := utils.CandleDurationFromString(timeframe)
		
		window := utils.CandleDurationFromString(timeframe_duration.String)
		start := window.Truncate(from).Unix()
		end := window.Ceil(to).Add(-time.Second).Unix()
		
		slc, err := io.SliceColumnSeriesByEpoch(*cs, &start, &end)
		if err != nil {
			log.Error("%s/%s/%s: %v", symbol, timeframe, bucket, err)
			continue
		}
		if len(slc.GetEpoch()) == 0 {
			continue
		}
	
		var tqSlc io.ColumnSeries
		
		switch marketType {
		case "crytpo":
			tqSlc = *slc.ApplyTimeQual(IsCryptoMarketOpen)
		case "forex":
			tqSlc = *slc.ApplyTimeQual(IsForexMarketOpen)
		case "equity":
			tqSlc = *slc.ApplyTimeQual(IsEquityMarketOpen)
		}
		
		csm := io.NewColumnSeriesMap()
		if len(tqSlc.GetEpoch()) > 0 {
			csm.AddColumnSeries(*aggTbk, aggregate(&tqSlc, aggTbk))
		}
		
		executor.WriteCSM(csm, false)
	}
}

func aggregate(cs *io.ColumnSeries, tbk *io.TimeBucketKey) *io.ColumnSeries {

	params := []accumParam{
		accumParam{"Open", "avgl", "Open"},
		accumParam{"High", "avgr", "High"},
		accumParam{"Low", "avgr", "Low"},
		accumParam{"Close", "avgr", "Close"},
	}
	if cs.Exists("Volume") {
		params = append(params, accumParam{"Volume", "sum", "Volume"})
	}
	if cs.Exists("HLC") {
		params = append(params, accumParam{"HLC", "avgr", "HLC"})
		params = append(params, accumParam{"HLC", "roc", "ROC"}) // Original output
    }
	if cs.Exists("TVAL") {
		params = append(params, accumParam{"TVAL", "sum", "TVAL"})
		params = append(params, accumParam{"TVAL", "roc", "TROC"}) // Original output
    }
	if cs.Exists("Spread") {
		params = append(params, accumParam{"Spread", "avgl", "OSpread"}) // Original output
		params = append(params, accumParam{"Spread", "avgr", "CSpread"}) // Original output
    }
	if cs.Exists("Split") {
		params = append(params, accumParam{"Split", "last", "Split"})
	}
	
	accumGroup := newAccumGroup(cs, params)

	ts := cs.GetTime()
	outEpoch := make([]int64, 0)
	
	timeWindow := utils.CandleDurationFromString(tbk.GetItemInCategory("Timeframe"))
	if len(ts) > 2 {
		groupKey := timeWindow.Ceil(ts[0]) // Get the upper-bounds of the timeframe for the new Epoch
		groupStart := 0
		for i, t := range ts {
			// timestamp has iterated to the new Epoch, add it to accumGroup for aggregation
			// Timestamp on close
			// Example: New Epoch: 2020-05-01 03:15:00 +0000 UTC, Built From: 2020-05-01 03:01:00 +0000 UTC To: 2020-05-01 03:15:00 +0000 UTC
			if groupKey.Unix() <= t.Unix() {
				if i > groupStart+1 {
					outEpoch = append(outEpoch, groupKey.Unix())
					accumGroup.apply(groupStart+1, i)
				}
				// log.Info("%s: %v for %v-%v (%v-%v)", tbk.String(), groupKey, groupStart+1, i, ts[groupStart+1], ts[i])
				groupKey = timeWindow.Ceil(t)
				groupStart = i
			}
		}
	}
	
	// finalize output
	outCs := io.NewColumnSeries()
	outCs.AddColumn("Epoch", outEpoch)
	accumGroup.addColumns(outCs)
	return outCs
}
