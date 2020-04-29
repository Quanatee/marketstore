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

	"github.com/alpacahq/marketstore/utils/log"
	//"github.com/valyala/fasthttp"
	"gopkg.in/matryer/try.v1"
)

const (
	tickersURL = "%v/v2/reference/tickers"
	retryCount = 10
)

var (
	aggURL = map[string]string{
		"crypto": "%v/tiingo/crypto/prices",
		"forex": "%v/tiingo/fx/%v/prices",
		"equity": "%v/iex/%v/prices",
	}
	baseURL = "https://api.tiingo.com"
	apiKey 	 string
	length = 0
)

func SetAPIKey(key string) {
	apiKey = key
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
	q.Set("startDate", from.AddDate(0, 0, -1).Format("2006-01-02"))
	q.Set("endDate", to.AddDate(0, 0, 1).Format("2006-01-02"))
	if strings.Compare(marketType, "crypto") == 0 {
		q.Set("tickers", symbol)
	} else if strings.Compare(marketType, "equity") == 0 {
		q.Set("afterHours", "false")
		q.Set("forceFill", "false")
	}

	u.RawQuery = q.Encode()

	var aggCrypto []AggCrypto
	var aggForex AggForex
	var aggEquity AggEquity

	if strings.Compare(marketType, "crypto") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggCrypto)
	} else if strings.Compare(marketType, "forex") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggForex.PriceData)
	} else if strings.Compare(marketType, "equity") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggEquity.PriceData)
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
	} else if strings.Compare(marketType, "forex") == 0 {
		length = len(aggForex.PriceData)
	} else if strings.Compare(marketType, "equity") == 0 {
		length = len(aggEquity.PriceData)
	}

	if length == 0 {
		log.Debug("%s [tiingo] returned 0 results between %v and %v", symbol, from, to)
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
    for bar := 0; bar < length; bar++ {
		if strings.Compare(marketType, "crypto") == 0 {
			if len(aggCrypto[0].PriceData) <= bar {
				// Unknown issue unique to Tiingo that causes index out of range
				// (Probably malformed json)
				return &OHLCV{}, err
			}
			dt, err_dt := time.Parse(time.RFC3339, aggCrypto[0].PriceData[bar].Date)
			if err_dt != nil {
				return &OHLCV{}, err
			}
			if aggCrypto[0].PriceData[bar].Open != 0 && aggCrypto[0].PriceData[bar].High != 0 && aggCrypto[0].PriceData[bar].Low != 0 && aggCrypto[0].PriceData[bar].Close != 0 {
				Epoch := dt.Unix()
				if Epoch > from.Unix() && Epoch < to.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggCrypto[0].PriceData[bar].Open
					ohlcv.High[Epoch] = aggCrypto[0].PriceData[bar].High
					ohlcv.Low[Epoch] = aggCrypto[0].PriceData[bar].Low
					ohlcv.Close[Epoch] = aggCrypto[0].PriceData[bar].Close
					if aggCrypto[0].PriceData[bar].Volume != 0 {
						ohlcv.Volume[Epoch] = aggCrypto[0].PriceData[bar].Volume
					} else {
						ohlcv.Volume[Epoch] = 1.0
					}
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
				}
			}
		} else if strings.Compare(marketType, "forex") == 0 {
			if len(aggForex.PriceData) <= bar {
				// Unknown issue unique to Tiingo that causes index out of range
				// (Probably malformed json)
				return &OHLCV{}, err
			}
			dt, err_dt := time.Parse(time.RFC3339, aggForex.PriceData[bar].Date)
			if err_dt != nil {
				return &OHLCV{}, err
			}
			if aggForex.PriceData[bar].Open != 0 && aggForex.PriceData[bar].High != 0 && aggForex.PriceData[bar].Low != 0 && aggForex.PriceData[bar].Close != 0 {
				Epoch := dt.Unix()
				if Epoch > from.Unix() && Epoch < to.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggForex.PriceData[bar].Open
					ohlcv.High[Epoch] = aggForex.PriceData[bar].High
					ohlcv.Low[Epoch] = aggForex.PriceData[bar].Low
					ohlcv.Close[Epoch] = aggForex.PriceData[bar].Close
					ohlcv.Volume[Epoch] = 1.0
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
				}
			}
		} else if strings.Compare(marketType, "equity") == 0 {
			if len(aggEquity.PriceData) <= bar {
				return &OHLCV{}, err
			}
			dt, err_dt := time.Parse(time.RFC3339, aggEquity.PriceData[bar].Date)
			if err_dt != nil {
				// Unknown issue unique to Tiingo that causes index out of range
				// (Probably malformed json)
				return &OHLCV{}, err
			}
			if aggEquity.PriceData[bar].Open != 0 && aggEquity.PriceData[bar].High != 0 && aggEquity.PriceData[bar].Low != 0 && aggEquity.PriceData[bar].Close != 0 {
				Epoch := dt.Unix()
				if Epoch > from.Unix() && Epoch < to.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggEquity.PriceData[bar].Open
					ohlcv.High[Epoch] = aggEquity.PriceData[bar].High
					ohlcv.Low[Epoch] = aggEquity.PriceData[bar].Low
					ohlcv.Close[Epoch] = aggEquity.PriceData[bar].Close
					ohlcv.Volume[Epoch] = 1.0
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
				}
			}
		}
	}
	
	if len(ohlcv.HLC) == 0 {
		log.Debug("%s [tiingo] returned %v results and validated %v results between %v and %v", symbol, length, len(ohlcv.HLC), from, to)
		if length == 1 {
			if strings.Compare(marketType, "crypto") == 0 {
				log.Debug("%s [tiingo] Data: %v", symbol, aggCrypto[0])
			} else if strings.Compare(marketType, "forex") == 0 {
				log.Debug("%s [tiingo] Data: %v", symbol, aggForex)
			} else if strings.Compare(marketType, "equity") == 0 {
				log.Debug("%s [tiingo] Data: %v", symbol, aggEquity)
			}
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
			time.Sleep(3 * time.Second)
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