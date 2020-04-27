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
		"forex": "%v/fx/%v/prices",
		"stocks": "%v/iex/%v/prices",
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
	from, to time.Time) (*OHLCV_map, error) {

	fullURL := ""
	if strings.Compare(marketType, "crypto") == 0 {
		fullURL = fmt.Sprintf(aggURL[marketType], baseURL)
	} else {
		fullURL = fmt.Sprintf(aggURL[marketType], baseURL, symbol)
	}

	u, err := url.Parse(fullURL)
	if err != nil {
		return nil, err
	}
	
	q := u.Query()
	q.Set("token", apiKey)
	q.Set("resampleFreq", multiplier+resolution)
	q.Set("startDate", from.Format(time.RFC3339))
	q.Set("endDate", to.Format(time.RFC3339))
	if strings.Compare(marketType, "crypto") == 0 {
		q.Set("tickers", symbol)
	} else if strings.Compare(marketType, "stocks") == 0 {
		q.Set("afterHours", "false")
		q.Set("forceFill", "false")
	}

	u.RawQuery = q.Encode()

	// agg := &Agg{}
	// aggCrypto := &AggCrypto{}
	// aggCrypto := &[]AggCrypto{}
	var agg Agg
	var aggCrypto []AggCrypto

	if strings.Compare(marketType, "crypto") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggCrypto)
	} else {
		err = downloadAndUnmarshal(u.String(), retryCount, &agg)
	}

	if err != nil {
		return &OHLCV_map{}, err
	}

	if len(aggCrypto) == 0 {
		return &OHLCV_map{}, nil
	}

	if strings.Compare(marketType, "crypto") == 0 {
		length = len(aggCrypto[0].PriceData)
	} else {
		length = len(agg.PriceData)
	}

	if length == 0 {
		log.Info("%s: len %v", symbol, length)
		return &OHLCV_map{}, nil
	}
	
	ohlcv := &OHLCV_map{
		Open: make(map[int64]float32),
		High: make(map[int64]float32),
		Low: make(map[int64]float32),
		Close: make(map[int64]float32),
		Volume: make(map[int64]float32),
		HLC: make(map[int64]float32),
		Spread: make(map[int64]float32),
		VWAP: make(map[int64]float32),
	}
	// Tiingo candle formula (Timestamp on close)
	// Requested at 14:05:01
	// Candle built from 14:04 to 14:05
	// Timestamped at 14:05
	// We use Timestamp on open, so we substract 60s from the timetamp
    for bar := 0; bar < length; bar++ {
		
		if strings.Compare(marketType, "crypto") == 0 {
			if aggCrypto[0].PriceData[bar].Open != 0 && aggCrypto[0].PriceData[bar].High != 0 && aggCrypto[0].PriceData[bar].Low != 0 && aggCrypto[0].PriceData[bar].Close != 0 {
				dt, _ := time.Parse(time.RFC3339, aggCrypto[0].PriceData[bar].Date)	
				Epoch := dt.Unix() - 60
				if dt.Unix() - 60 >= from.Unix() {
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
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
					ohlcv.VWAP[Epoch] = (ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch])/ohlcv.Volume[Epoch]
				}
			}
		} else {
			if agg.PriceData[bar].Open != 0 && agg.PriceData[bar].High != 0 && agg.PriceData[bar].Low != 0 && agg.PriceData[bar].Close != 0 {
				dt, _ := time.Parse(time.RFC3339, aggCrypto[0].PriceData[bar].Date)	
				Epoch := dt.Unix() - 60
				if dt.Unix() - 60 >= from.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = agg.PriceData[bar].Open
					ohlcv.High[Epoch] = agg.PriceData[bar].High
					ohlcv.Low[Epoch] = agg.PriceData[bar].Low
					ohlcv.Close[Epoch] = agg.PriceData[bar].Close
					ohlcv.Volume[Epoch] = 1.0
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
					ohlcv.VWAP[Epoch] = (ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch])/ohlcv.Volume[Epoch]
				}
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
			time.Sleep(5 * time.Second)
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