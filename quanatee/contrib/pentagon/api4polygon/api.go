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
	symbolPrefix = map[string]string{
		"crypto": "X:",
		"forex": "C:",
		"equity": "",
	}
	SplitEvents *sync.Map
	UpcomingSplitEvents *sync.Map
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

	var aggCrypto AggCrypto
	var aggForex AggForex
	var aggEquity AggEquity

	if strings.Compare(marketType, "crypto") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggCrypto)
	} else if strings.Compare(marketType, "forex") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggForex)
	} else if strings.Compare(marketType, "equity") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggEquity)
	}
	
	if err != nil {
		return &OHLCV{}, err
	}
	
	length := len(agg.PriceData)

	if length == 0 {
		log.Info("%s [polygon] returned 0 results between %v and %v | Link: %s", symbol, from, to, u.String())
		return &OHLCV{}, nil
	}
	
	if strings.Compare(marketType, "crypto") == 0 {
		length = len(aggCrypto.PriceData)
	} else if strings.Compare(marketType, "forex") == 0 {
		length = len(aggForex.PriceData)
	} else if strings.Compare(marketType, "equity") == 0 {
		length = len(aggEquity.PriceData)
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
		if strings.Compare(marketType, "crypto") == 0 {
			if ( (aggCrypto.PriceData[bar].Open != 0 && aggCrypto.PriceData[bar].High != 0 && aggCrypto.PriceData[bar].Low != 0 && aggCrypto.PriceData[bar].Close != 0) &&
				(aggCrypto.PriceData[bar].Open != aggCrypto.PriceData[bar].Close) && 
				(aggCrypto.PriceData[bar].High != aggCrypto.PriceData[bar].Low) ) {
				Epoch := (aggCrypto.PriceData[bar].Timestamp / 1000) + 60
				if Epoch > from.Unix() && Epoch < to.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggCrypto.PriceData[bar].Open
					ohlcv.High[Epoch] = aggCrypto.PriceData[bar].High
					ohlcv.Low[Epoch] = aggCrypto.PriceData[bar].Low
					ohlcv.Close[Epoch] = aggCrypto.PriceData[bar].Close
					// If Polygon fails to provide intraday volume, we try to take from Tiingo historical daily volume (pro-rated)
					// Livefilling volume is dependent on Polygon (Which should be fine)
					if aggCrypto.PriceData[bar].Volume > float32(1) {
						ohlcv.Volume[Epoch] = float32(aggCrypto.PriceData[bar].Volume)
					} else {
						symbolDailyVolume_, _ := api4tiingo.DailyVolumes.Load(symbol)
						if symbolDailyVolume_ != nil {
							symbolDailyVolume := symbolDailyVolume_.(map[time.Time]float32)
							dt := time.Unix(Epoch, 0)
							if dailyVolume, ok := symbolDailyVolume[time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC)]; ok {
								ohlcv.Volume[Epoch] = float32(dailyVolume/1440)
							} else {
								ohlcv.Volume[Epoch] = float32(1)
							}
						} else {
							ohlcv.Volume[Epoch] = float32(1)
						}
					}
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
				}
			}
		} else if strings.Compare(marketType, "forex") == 0 {
			if ( (aggForex.PriceData[bar].Open != 0 && aggForex.PriceData[bar].High != 0 && aggForex.PriceData[bar].Low != 0 && aggForex.PriceData[bar].Close != 0) &&
				(aggForex.PriceData[bar].Open != aggForex.PriceData[bar].Close) && 
				(aggForex.PriceData[bar].High != aggForex.PriceData[bar].Low) ) {
				Epoch := (aggForex.PriceData[bar].Timestamp / 1000) + 60
				if Epoch > from.Unix() && Epoch < to.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggForex.PriceData[bar].Open
					ohlcv.High[Epoch] = aggForex.PriceData[bar].High
					ohlcv.Low[Epoch] = aggForex.PriceData[bar].Low
					ohlcv.Close[Epoch] = aggForex.PriceData[bar].Close
					// If Polygon fails to provide intraday volume, we try to take from Tiingo historical daily volume (pro-rated)
					// Livefilling volume is dependent on Polygon (Which should be fine)
					if aggForex.PriceData[bar].Volume > float32(1) {
						ohlcv.Volume[Epoch] = float32(aggForex.PriceData[bar].Volume)
					} else {
						symbolDailyVolume_, _ := api4tiingo.DailyVolumes.Load(symbol)
						if symbolDailyVolume_ != nil {
							symbolDailyVolume := symbolDailyVolume_.(map[time.Time]float32)
							dt := time.Unix(Epoch, 0)
							if dailyVolume, ok := symbolDailyVolume[time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC)]; ok {
								ohlcv.Volume[Epoch] = float32(dailyVolume/1440)
							} else {
								ohlcv.Volume[Epoch] = float32(1)
							}
						} else {
							ohlcv.Volume[Epoch] = float32(1)
						}
					}
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
				}
			}
		} else if strings.Compare(marketType, "equity") == 0 {
			if ( (aggEquity.PriceData[bar].Open != 0 && aggEquity.PriceData[bar].High != 0 && aggEquity.PriceData[bar].Low != 0 && aggEquity.PriceData[bar].Close != 0) &&
				(aggEquity.PriceData[bar].Open != aggEquity.PriceData[bar].Close) && 
				(aggEquity.PriceData[bar].High != aggEquity.PriceData[bar].Low) ) {
				Epoch := (aggEquity.PriceData[bar].Timestamp / 1000) + 60
				if Epoch > from.Unix() && Epoch < to.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggEquity.PriceData[bar].Open
					ohlcv.High[Epoch] = aggEquity.PriceData[bar].High
					ohlcv.Low[Epoch] = aggEquity.PriceData[bar].Low
					ohlcv.Close[Epoch] = aggEquity.PriceData[bar].Close
					// If Polygon fails to provide intraday volume, we try to take from Tiingo historical daily volume (pro-rated)
					// Livefilling volume is dependent on Polygon (Which should be fine)
					if aggEquity.PriceData[bar].Volume > float32(1) {
						ohlcv.Volume[Epoch] = float32(aggEquity.PriceData[bar].Volume)
					} else {
						symbolDailyVolume_, _ := api4tiingo.DailyVolumes.Load(symbol)
						if symbolDailyVolume_ != nil {
							symbolDailyVolume := symbolDailyVolume_.(map[time.Time]float32)
							dt := time.Unix(Epoch, 0)
							if dailyVolume, ok := symbolDailyVolume[time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC)]; ok {
								ohlcv.Volume[Epoch] = float32(dailyVolume/390)
							} else {
								ohlcv.Volume[Epoch] = float32(1)
							}
						} else {
							ohlcv.Volume[Epoch] = float32(1)
						}
					}
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
				}
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
