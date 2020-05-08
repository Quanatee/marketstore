package api4tiingo

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"net/http"
	"net/url"
	//"strconv"
	"time"

	"github.com/alpacahq/marketstore/quanatee/contrib/pentagon/api"
	"github.com/alpacahq/marketstore/utils/log"
	"gopkg.in/matryer/try.v1"
)

const (
	splitsURL = "%v/tiingo/daily/%v/prices"
	retryCount = 3
)

var (
	aggURL = map[string]string{
		"crypto": "%v/tiingo/crypto/prices",
		"forex": "%v/tiingo/fx/%v/prices",
		"equity": "%v/iex/%v/prices",
		"futures": "%v/iex/%v/prices",
	}
	baseURL = "https://api.tiingo.com"
	start time.Time
	apiKey 	 string
	length = 0
)

func SetAPIKey(key string) {
	apiKey = key
}

func UpdateDailyVolumes(symbol string, queryStart time.Time) {
	
	u, err := url.Parse(fmt.Sprintf(splitsURL, baseURL, symbol))

	if err != nil {
		log.Error("%s %v", symbol, err)
	}
	
	q := u.Query()
	q.Set("token", apiKey)
	q.Set("resampleFreq", "daily")
	q.Set("startDate", queryStart.Format("2006-01-02"))
	
	u.RawQuery = q.Encode()

	var splitsItem []SplitData

	err = downloadAndUnmarshal(u.String(), retryCount, &splitsItem)

	if err != nil {
		log.Error("[tiingo] %s %v", symbol, err)
	}

	if len(splitsItem) > 0 {
		
		symbolDailyVolume := map[time.Time]float32{}
		for _, splitData := range splitsItem {
			if splitData.Volume != 0 {
				date, _ := time.Parse(time.RFC3339, splitData.Date)
				symbolDailyVolume[date] = splitData.Volume
			}
		}
		if len(symbolDailyVolume) > 0 {
			api.TiingoDailyVolumes.Store(symbol, symbolDailyVolume)
		}
	}
}

func GetAggregates(
	symbol, marketType, multiplier, resolution string,
	from, to time.Time) (*OHLCV, error) {

	fullURL := ""

	if strings.Compare(marketType, "crypto") == 0 {
		fullURL = fmt.Sprintf(aggURL[marketType], baseURL)
	} else {
		fullURL = fmt.Sprintf(aggURL[marketType], baseURL, symbol)
	}

	u, err := url.Parse(fullURL)

	if err != nil {
		return &OHLCV{}, err
	}
	
	q := u.Query()
	q.Set("token", apiKey)
	q.Set("resampleFreq", multiplier+resolution)
	q.Set("startDate", from.Format("2006-01-02"))
	q.Set("endDate", to.AddDate(0, 0, 1).Format("2006-01-02"))
	if strings.Compare(marketType, "crypto") == 0 {
		q.Set("tickers", symbol)
	} else if strings.Compare(marketType, "equity") == 0 {
		q.Set("afterHours", "false")
		q.Set("forceFill", "false")
	}

	u.RawQuery = q.Encode()

	var aggCrypto []AggCrypto
	var aggEquity AggEquity
	var aggForex AggForex
	var aggFutures AggFutures

	if strings.Compare(marketType, "crypto") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggCrypto)
	} else if strings.Compare(marketType, "equity") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggEquity.PriceData)
	} else if strings.Compare(marketType, "forex") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggForex.PriceData)
	} else if strings.Compare(marketType, "futures") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggFutures.PriceData)
	}

	if err != nil {
		return &OHLCV{}, err
	}
	
	if strings.Compare(marketType, "crypto") == 0 {
		if len(aggCrypto) > 0 {
			length = len(aggCrypto[0].PriceData)
		} else {
			length = 0
		}
	} else if strings.Compare(marketType, "equity") == 0 {
			length = len(aggEquity.PriceData)
	} else if strings.Compare(marketType, "forex") == 0 {
		length = len(aggForex.PriceData)
	} else if strings.Compare(marketType, "futures") == 0 {
		length = len(aggFutures.PriceData)
	}

	if length == 0 {
		log.Debug("%s [tiingo] returned 0 results between %v and %v | Link: %s", symbol, from, to, u.String())
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
	/*
    defer func() {
        if err := recover(); err != nil {
            log.Error("Panic occurred:", err)
        }
	}()
	*/
	// Tiingo candle formula (Timestamp on close)
	// Requested at 14:05:01
	// Candle built from 14:04 to 14:05
	// Timestamped at 14:05
	// We use Timestamp on close, so no change
	if strings.Compare(marketType, "crypto") == 0 {
		for bar := 0; bar < len(aggCrypto[0].PriceData); bar++ {
			if len(aggCrypto[0].PriceData) <= bar {
				// Unknown issue that causes index out of range
				log.Info("[tiingo] %s bar went too far %v/%v", symbol, bar, len(aggCrypto[0].PriceData))
				break
			}
			dt, err_dt := time.Parse(time.RFC3339, aggCrypto[0].PriceData[bar].Date)
			if err_dt != nil {
				continue
			}
			if aggCrypto[0].PriceData[bar].Open != 0 && aggCrypto[0].PriceData[bar].High != 0 && aggCrypto[0].PriceData[bar].Low != 0 && aggCrypto[0].PriceData[bar].Close != 0 {
				Epoch := dt.Unix()
				if Epoch > from.Unix() && Epoch < to.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggCrypto[0].PriceData[bar].Open
					ohlcv.High[Epoch] = aggCrypto[0].PriceData[bar].High
					ohlcv.Low[Epoch] = aggCrypto[0].PriceData[bar].Low
					ohlcv.Close[Epoch] = aggCrypto[0].PriceData[bar].Close
					if aggCrypto[0].PriceData[bar].Volume > float32(1) {
						ohlcv.Volume[Epoch] = float32(aggCrypto[0].PriceData[bar].Volume)
					} else {
						ohlcv.Volume[Epoch] = api.GetAlternateVolumeTiingoFirst(symbol, marketType, Epoch, from, to)
					}
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
				}
			}
		}
	} else if strings.Compare(marketType, "equity") == 0 {
		for bar := 0; bar < len(aggEquity.PriceData); bar++ {
			if len(aggEquity.PriceData) <= bar {
				log.Info("[tiingo] %s bar went too far %v/%v", symbol, bar, len(aggEquity.PriceData))
				break
			}
			dt, err_dt := time.Parse(time.RFC3339, aggEquity.PriceData[bar].Date)
			if err_dt != nil {
				continue
			}
			if aggEquity.PriceData[bar].Open != 0 && aggEquity.PriceData[bar].High != 0 && aggEquity.PriceData[bar].Low != 0 && aggEquity.PriceData[bar].Close != 0 {
				Epoch := dt.Unix()
				if Epoch > from.Unix() && Epoch < to.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggEquity.PriceData[bar].Open
					ohlcv.High[Epoch] = aggEquity.PriceData[bar].High
					ohlcv.Low[Epoch] = aggEquity.PriceData[bar].Low
					ohlcv.Close[Epoch] = aggEquity.PriceData[bar].Close
					ohlcv.Volume[Epoch] = api.GetAlternateVolumeTiingoFirst(symbol, marketType, Epoch, from, to)
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
				}
			}
		}
	} else if strings.Compare(marketType, "forex") == 0 {
		for bar := 0; bar < len(aggForex.PriceData); bar++ {
			if len(aggForex.PriceData) <= bar {
				// Unknown issue that causes index out of range
				log.Info("[tiingo] %s bar went too far %v/%v", symbol, bar, len(aggForex.PriceData))
				break
			}
			dt, err_dt := time.Parse(time.RFC3339, aggForex.PriceData[bar].Date)
			if err_dt != nil {
				continue
			}
			if aggForex.PriceData[bar].Open != 0 && aggForex.PriceData[bar].High != 0 && aggForex.PriceData[bar].Low != 0 && aggForex.PriceData[bar].Close != 0 {
				Epoch := dt.Unix()
				if Epoch > from.Unix() && Epoch < to.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggForex.PriceData[bar].Open
					ohlcv.High[Epoch] = aggForex.PriceData[bar].High
					ohlcv.Low[Epoch] = aggForex.PriceData[bar].Low
					ohlcv.Close[Epoch] = aggForex.PriceData[bar].Close
					ohlcv.Volume[Epoch] = api.GetAlternateVolumeTiingoFirst(symbol, marketType, Epoch, from, to)
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
				}
			}
		}
	} else if strings.Compare(marketType, "futures") == 0 {
		for bar := 0; bar < len(aggFutures.PriceData); bar++ {
			if len(aggFutures.PriceData) <= bar {
				log.Info("[tiingo] %s bar went too far %v/%v", symbol, bar, len(aggFutures.PriceData))
				break
			}
			dt, err_dt := time.Parse(time.RFC3339, aggFutures.PriceData[bar].Date)
			if err_dt != nil {
				continue
			}
			if aggFutures.PriceData[bar].Open != 0 && aggFutures.PriceData[bar].High != 0 && aggFutures.PriceData[bar].Low != 0 && aggFutures.PriceData[bar].Close != 0 {
				Epoch := dt.Unix()
				if Epoch > from.Unix() && Epoch < to.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggFutures.PriceData[bar].Open
					ohlcv.High[Epoch] = aggFutures.PriceData[bar].High
					ohlcv.Low[Epoch] = aggFutures.PriceData[bar].Low
					ohlcv.Close[Epoch] = aggFutures.PriceData[bar].Close
					ohlcv.Volume[Epoch] = api.GetAlternateVolumeTiingoFirst(symbol, marketType, Epoch, from, to)
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
				}
			}
		}
	}

	if len(ohlcv.HLC) == 0 {
		log.Info("%s [tiingo] returned %v results and validated %v results between %v and %v | Link: %s", symbol, length, len(ohlcv.HLC), from, to, u.String())
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