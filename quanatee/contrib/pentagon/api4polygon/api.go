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

	"github.com/alpacahq/marketstore/utils/log"
	"gopkg.in/matryer/try.v1"
)

const (
	aggURL     = "%v/v2/aggs/ticker/%v/range/%v/%v/%v/%v"
	splitsURL  = "%v/v2/reference/splits/%v"
	retryCount = 10
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
	
	previousSplits *sync.Map
	upcomingSplits *sync.Map
	/*
	previousSplits = map[string]Splits{}
	upcomingSplits = map[string]time.Time{}
	*/
)

func SetAPIKey(key string) {
	apiKey = key
}

func GetPreviousSplits(symbol string) ([]Splits) {
	value, ok := previousSplits.Load(symbol)
	if ok == false {
		return []Splits{}
	}
	if value == nil {
		return []Splits{}
	}
	return value.([]Splits)
}
func SetPreviousSplits(symbol string, splits []Splits) {
	previousSplits.Store(symbol, splits)
}

func SetUpcomingSplits(symbol string, issueDate time.Time) {
	upcomingSplits.Store(symbol, issueDate)
}

func GetUpcomingSplits(symbol string) (time.Time) {
	value, ok := upcomingSplits.Load(symbol)
	if ok == false {
		return time.Time{}
	}
	if value == nil {
		return time.Time{}
	}
	return value.(time.Time)
}

func DeleteUpcomingSplits(symbol string) {
	upcomingSplits.Store(symbol, nil)
}

func UpdateSplits(symbol string) {
		
	u, err := url.Parse(fmt.Sprintf(splitsURL, baseURL, symbol))

	if err != nil {
		log.Error("%s %v", symbol, err)
	}
	
	q := u.Query()
	q.Set("apiKey", apiKey)

	u.RawQuery = q.Encode()

	var splitsItem []SplitsItem

	err = downloadAndUnmarshal(u.String(), retryCount, &splitsItem)

	if err != nil {
		log.Error("%s %v", symbol, err)
	}
	
	if splitsItem.Count > 0 {
		var splits []Splits
		for _, splitData := range splitsItem.SplitData {
			dt, _ := time.Parse("2006-01-02", splitData.Issue)
			append(splitsItem, Splits{Issue: dt, Ratio: splitData.Ratio,})
		}
		log.Info("%s %v", symbol, splits)
		SetPreviousSplits(symbol, splits)
	}
	
}
func GetAggregates(
	symbol, marketType, multiplier, resolution string,
	from, to time.Time) (*OHLCV, error) {
	
	u, err := url.Parse(fmt.Sprintf(aggURL, baseURL, symbolPrefix[marketType]+symbol, multiplier, resolution, from.Unix()*1000, to.Unix()*1000))

	if err != nil {
		log.Error("%s %v", symbol, err)
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

	length := len(agg.PriceData)

	if length == 0 {
		log.Debug("%s [polygon] returned 0 results between %v and %v", symbol, from, to)
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
		if ( (agg.PriceData[bar].Open != 0 && agg.PriceData[bar].High != 0 && agg.PriceData[bar].Low != 0 && agg.PriceData[bar].Close != 0) &&
			(agg.PriceData[bar].Open != agg.PriceData[bar].Close) && 
			(agg.PriceData[bar].High != agg.PriceData[bar].Low) ) {
			Epoch := (agg.PriceData[bar].Timestamp / 1000) + 60
			if Epoch > from.Unix() && Epoch < to.Unix() {
				//OHLCV
				ohlcv.Open[Epoch] = agg.PriceData[bar].Open
				ohlcv.High[Epoch] = agg.PriceData[bar].High
				ohlcv.Low[Epoch] = agg.PriceData[bar].Low
				ohlcv.Close[Epoch] = agg.PriceData[bar].Close
				if agg.PriceData[bar].Volume != float32(0) {
					ohlcv.Volume[Epoch] = agg.PriceData[bar].Volume
				} else {
					ohlcv.Volume[Epoch] = float32(1)
				}
				// Extra
				ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
				ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
				ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
				// Correct for Splits if required
				splits := GetPreviousSplits(symbol)
				if len(splits) > 0 {
					for _, split := range splits {
						if Epoch < split.Issue.Unix() {
							// data is before the split date
							//OHLCV Adjusted
							ohlcv.Open[Epoch] = ohlcv.Open[Epoch] / split.Ratio
							ohlcv.High[Epoch] = ohlcv.High[Epoch] / split.Ratio
							ohlcv.Low[Epoch] = ohlcv.Low[Epoch] / split.Ratio
							ohlcv.Close[Epoch] = ohlcv.Close[Epoch] / split.Ratio
							if ohlcv.Volume[Epoch] != float32(1) {
								ohlcv.Volume[Epoch] = ohlcv.Volume[Epoch] * split.Ratio
							}
							// Extra Adjusted
							ohlcv.HLC[Epoch] = ohlcv.HLC[Epoch] / split.Ratio
							ohlcv.TVAL[Epoch] = ohlcv.TVAL[Epoch] / split.Ratio
							ohlcv.Spread[Epoch] = ohlcv.Spread[Epoch] / split.Ratio
						}
					}
				}
			}
		}
	}
	
	if len(ohlcv.HLC) == 0 {
		log.Debug("%s [polygon] returned %v results and validated %v results between %v and %v", symbol, length, len(ohlcv.HLC), from, to)
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
