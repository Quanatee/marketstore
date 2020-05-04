package api4polygon

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"

	"net/http"
	"net/url"
	//"strconv"
	"time"

	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api4tiingo"
	"github.com/alpacahq/marketstore/utils/log"
	"gopkg.in/matryer/try.v1"
)

const (
	aggURL     = "%v/v2/aggs/ticker/%v/range/%v/%v/%v/%v"
	splitsURL  = "%v/v2/reference/splits/%v"
	retryCount = 3
)
	
var (
	baseURL = "https://api.polygon.io"
	start time.Time
	apiKey  	string
	length = 0
	symbolPrefix = map[string]string{
		"crypto": "X:",
		"forex": "C:",
		"equity": "",
	}
	SplitEvents *sync.Map
	UpcomingSplitEvents *sync.Map
	DailyVolumes *sync.Map
)

func SetAPIKey(key string) {
	apiKey = key
}

func UpdateSplitEvents(symbol string, timeStarted time.Time) (bool) {
		
	u, err := url.Parse(fmt.Sprintf(splitsURL, baseURL, symbol))

	if err != nil {
		log.Error("%s %v", symbol, err)
	}
	
	q := u.Query()
	q.Set("apiKey", apiKey)

	u.RawQuery = q.Encode()

	var splitsItem SplitsItem

	err = downloadAndUnmarshal(u.String(), retryCount, &splitsItem)

	if err != nil {
		log.Error("%s %v", symbol, err)
	}
	
	rebackfill := false

	if len(splitsItem.SplitData) > 0 {
		
		symbolSplits, ok := SplitEvents.Load(symbol)
		
		if ok == false {
			// First time
			symbolSplits := map[time.Time]float32{}
			for _, splitData := range splitsItem.SplitData {
				expiryDate, _ := time.Parse("2006-01-02", splitData.Expiry)
				symbolSplits[expiryDate] = splitData.Ratio
			}
			if len(symbolSplits) > 0 {
				SplitEvents.Store(symbol, symbolSplits)
				log.Info("[polygon] %s: %v", symbol, symbolSplits)
			}
		} else {
			// Subsequence
			symbolSplits := symbolSplits.(map[time.Time]float32)
			for _, splitData := range splitsItem.SplitData {
				expiryDate, _ := time.Parse("2006-01-02", splitData.Expiry)
				if _, ok := symbolSplits[expiryDate]; ok {
					upcomingSplit, _ := UpcomingSplitEvents.Load(symbol)
					if upcomingSplit != nil {
						upcomingExpiryDate := upcomingSplit.(time.Time)
						// There are two ways to trigger a backfill
						// 1. ExpiryDate is after the time plugin was started and registered as an upcoming split event; or
						// 2. ExpiryDate is after the time plugin was started and ExpiryDate is the same date as current date.
						// We can do this because we only checkStockSplits() (in pentagon.go) once a day 02:00 New York time
						if ( ( expiryDate.After(timeStarted) || upcomingExpiryDate.IsZero() == false ) || 
							( expiryDate.After(timeStarted) || (expiryDate.Day() == time.Now().Day() && expiryDate.Month() == time.Now().Month() && expiryDate.Year() == time.Now().Year()) ) ) {
							rebackfill = true
							// Deregister as an upcoming split event
							UpcomingSplitEvents.Store(symbol, nil)
						}
					}
				} else {
					// New split event detected, we only store 1 upcoming split event per symbol at any given time
					symbolSplits[expiryDate] = splitData.Ratio
					UpcomingSplitEvents.Store(symbol, expiryDate)
				}
			}
			if len(symbolSplits) > 0 {
				SplitEvents.Store(symbol, symbolSplits)
			}
		}
	}
	return rebackfill
}

func UpdateDailyVolumes(symbol, marketType string, queryStart time.Time) {
	
	u, err := url.Parse(fmt.Sprintf(aggURL, baseURL, symbolPrefix[marketType]+symbol, "24", "hour", queryStart.Unix()*1000, time.Now().Unix()*1000))
	
	if err != nil {
		log.Error("[polygon] %s %v", symbol, err)
	}
	
	q := u.Query()
	q.Set("apiKey", apiKey)
	q.Set("unadjusted", "true") // false is buggy

	u.RawQuery = q.Encode()

	var agg Agg

	err = downloadAndUnmarshal(u.String(), retryCount, &agg)

	if err != nil {
		log.Error("[polygon] %s %v", symbol, err)
	}

	if len(agg.PriceData) > 0 {
		symbolDailyVolume := map[time.Time]float32{}
		for _, priceData := range agg.PriceData {
			if priceData.Volume != 0 {
				date := time.Unix(priceData.Timestamp / 1000, 0)
				date = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
				symbolDailyVolume[date] = priceData.Volume
			}
		}
		if len(symbolDailyVolume) > 0 {
			DailyVolumes.Store(symbol, symbolDailyVolume)
		}
	}
}

func GetAggregates(
	symbol, marketType, multiplier, resolution string,
	from, to time.Time) (*OHLCV, error) {
	
	u, err := url.Parse(fmt.Sprintf(aggURL, baseURL, symbolPrefix[marketType]+symbol, multiplier, resolution, from.Unix()*1000, to.Unix()*1000))

	if err != nil {
		log.Error("[polygon] %s %v", symbol, err)
	}
	
	q := u.Query()
	q.Set("apiKey", apiKey)
	q.Set("unadjusted", "true") // false is buggy

	u.RawQuery = q.Encode()
	
	var agg Agg

	err = downloadAndUnmarshal(u.String(), retryCount, &agg)
	
	if err != nil {
		return &OHLCV{}, err
	}
	
	length = len(agg.PriceData)
	
	if length == 0 {
		log.Info("%s [polygon] returned 0 results between %v and %v | Link: %s", symbol, from, to, u.String())
		return &OHLCV{}, nil
	}

	ohlcv := &OHLCV{
		Open: make(map[int64]float32),
		High: make(map[int64]float32),
		Low: make(map[int64]float32),
		Close: make(map[int64]float32),
		Volume: make(map[int64]float32),
		HLC: make(map[int64]float32),
		TVAL: make(map[int64]float32),
		Spread: make(map[int64]float32),
	}
	// Panic recovery
    defer func() {
        if err := recover(); err != nil {
            log.Error("Panic occurred:", err)
        }
	}()
	// Polygon candle formula (Timestamp on open)
	// Requested at 14:05:01
	// Candle built from 14:04 to 14:05
	// Timestamped at 14:04
	// We use Timestamp on close, so +60 to Timestamp
	for bar := 0; bar < length; bar++ {
		if len(agg.PriceData) <= bar {
			// Unknown issue that causes index out of range
			continue
		}
		if ( (agg.PriceData[bar].Open != 0 && agg.PriceData[bar].High != 0 && agg.PriceData[bar].Low != 0 && agg.PriceData[bar].Close != 0) &&
			(agg.PriceData[bar].Open != agg.PriceData[bar].Close) && 
			(agg.PriceData[bar].High != agg.PriceData[bar].Low) ) {
			Epoch := (agg.PriceData[bar].Timestamp / 1000) + 60
			if Epoch > from.Unix() && Epoch < to.Unix() {
				// OHLCV
				ohlcv.Open[Epoch] = agg.PriceData[bar].Open
				ohlcv.High[Epoch] = agg.PriceData[bar].High
				ohlcv.Low[Epoch] = agg.PriceData[bar].Low
				ohlcv.Close[Epoch] = agg.PriceData[bar].Close
				// If Polygon fails to provide intraday volume, we try to take from Tiingo historical daily volume (pro-rated)
				// Livefilling volume is dependent on Polygon (Which should be fine)
				if agg.PriceData[bar].Volume > float32(1) {
					ohlcv.Volume[Epoch] = float32(agg.PriceData[bar].Volume)
				} else {
					// Try provider daily volume, or set to 1
					volume_alt := false
					symbolDailyVolume_, _ := DailyVolumes.Load(symbol)
					if symbolDailyVolume_ != nil {
						symbolDailyVolume := symbolDailyVolume_.(map[time.Time]float32)
						dt := time.Unix(Epoch, 0)
						if dailyVolume, ok := symbolDailyVolume[time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC)]; ok {
							switch marketType {
							case "crytpo":
								ohlcv.Volume[Epoch] = float32(dailyVolume/1440)
							case "forex":
								ohlcv.Volume[Epoch] = float32(dailyVolume/1440)
							case "equity":
								ohlcv.Volume[Epoch] = float32(dailyVolume/390)
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
						symbolDailyVolume_, _ := api4tiingo.DailyVolumes.Load(symbol)
						if symbolDailyVolume_ != nil {
							symbolDailyVolume := symbolDailyVolume_.(map[time.Time]float32)
							dt := time.Unix(Epoch, 0)
							if dailyVolume, ok := symbolDailyVolume[time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC)]; ok {
								switch marketType {
								case "crytpo":
									ohlcv.Volume[Epoch] = float32(dailyVolume/1440)
								case "forex":
									ohlcv.Volume[Epoch] = float32(dailyVolume/1440)
								case "equity":
									ohlcv.Volume[Epoch] = float32(dailyVolume/390)
								default:
									ohlcv.Volume[Epoch] = float32(1)
								}
							} else {
								ohlcv.Volume[Epoch] = float32(1)
							}
						} else {
							ohlcv.Volume[Epoch] = float32(1)
						}
					}
					
				}
				// Extra
				ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
				ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
				ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
			}
		}
	}
	if len(ohlcv.HLC) == 0 {
		log.Info("%s [polygon] returned %v results and validated %v results between %v and %v | Link: %s", symbol, length, len(ohlcv.HLC), from, to, u.String())
		if length == 1 {
			log.Debug("%s [polygon] Data: %v", symbol, agg)
		}
	}

	return ohlcv, nil
	
}

func downloadAndUnmarshal(url string, retryCount int, data interface{}) error {
	// It is required to retry both the download() and unmarshal() calls
	// as network errors (e.g. Unexpected EOF) can come also from unmarshal()
	err := try.Do(func(attempt int) (bool, error) {
		resp, err := download(url, retryCount)
		if err == nil {
			err = unmarshal(resp, data)
		}

		if err != nil && strings.Contains(err.Error(), "GOAWAY") {
			// Polygon's way to tell that we are too fast
			time.Sleep(1 * time.Second)
		}

		return attempt < retryCount, err
	})

	return err
}

func download(url string, retryCount int) (*http.Response, error) {
	var (
		client = &http.Client{}
		resp   *http.Response
	)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	// The returned JSON's size can be greatly reduced by enabling compression
	req.Header.Add("Accept-Encoding", "gzip")
	resp, err = client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("status code %v", resp.StatusCode)
	}

	return resp, nil
}

func unmarshal(resp *http.Response, data interface{}) (err error) {
	defer resp.Body.Close()

	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return err
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, data)
}
