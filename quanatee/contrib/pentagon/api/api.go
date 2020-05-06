package api

import (
	"sync"
	"time"
	"sort"
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
	case "futures":
		return IsFuturesMarketOpen(from.Unix())
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

func IsFuturesMarketOpen(epoch int64) bool {
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

	epochs := cs.GetColumn("Epoch").([]int)
	// Returns the indices that would sort cs
	indices := Sort(sort.IntSlice(epochs))
	
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
	
	log.Info("%s, %v", tbk.String, cs)

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
		case "futures":
			tqSlc = *slc.ApplyTimeQual(IsFuturesMarketOpen)
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
		params = append(params, accumParam{"Spread", "avgr", "Spread"})
    }
	if cs.Exists("Split") {
		params = append(params, accumParam{"Split", "last", "Split"})
    }
	accumGroup := newAccumGroup(cs, params)

	ts := cs.GetTime()
	outEpoch := make([]int64, 0)
	
	timeWindow := utils.CandleDurationFromString(tbk.GetItemInCategory("Timeframe"))
	
	groupKey := timeWindow.Truncate(ts[0])
	groupStart := 0
	// accumulate inputs.  Since the input is ordered by
	// time, it is just to slice by correct boundaries
	for i, t := range ts {
		if !timeWindow.IsWithin(t, groupKey) {
			// Emit new row and re-init aggState
			outEpoch = append(outEpoch, groupKey.Unix())
			accumGroup.apply(groupStart, i)
			groupKey = timeWindow.Truncate(t)
			groupStart = i
		}
	}
	// accumulate any remaining values if not yet
	outEpoch = append(outEpoch, groupKey.Unix())
	accumGroup.apply(groupStart, len(ts))
	
	// finalize output
	outCs := io.NewColumnSeries()
	outCs.AddColumn("Epoch", outEpoch)
	accumGroup.addColumns(outCs)
	return outCs
}
