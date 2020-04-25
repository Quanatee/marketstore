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
	"strconv"
	"time"

	"github.com/alpacahq/marketstore/utils/log"
	"github.com/valyala/fasthttp"
	"gopkg.in/matryer/try.v1"
)

const (
	aggURL     = "%v/v2/aggs/ticker/%v/range/%v/%v/%v/%v"
	tickersURL = "%v/v2/reference/tickers"
	retryCount = 10
)
	
var (
	baseURL = "https://api.polygon.io"
	apiKey  string
)

func SetAPIKey(key string) {
	apiKey = key
}

func GetAggregates(
	symbol, multiplier, resolution string,
	from, to time.Time) (*OHLCV, error) {
		u, err := url.Parse(fmt.Sprintf(aggURL, baseURL, symbol, multiplier, resolution, from.Unix()*1000, to.Unix()*1000))
	if err != nil {
		return nil, err
	}
	
	q := u.Query()
	q.Set("apiKey", apiKey)
	q.Set("unadjusted", "true")

	u.RawQuery = q.Encode()

	agg := &Aggv2{}
	err = downloadAndUnmarshal(u.String(), retryCount, agg)
	if err != nil {
		return &OHLCV{}, err
	}

	length := len(agg.PriceData)

	if length == 0 {
		return &OHLCV{}, nil
	}
	
    ohlcv := &OHLCV{
        Epoch: make([]int64, length),
        Open: make([]float32, length),
        High: make([]float32, length),
        Low: make([]float32, length),
        Close: make([]float32, length),
        HLC: make([]float32, length),
        Volume: make([]float32, length),
	}
	
    for bar := 0; bar < length; bar++ {
		
		if agg.PriceData[bar].Open != 0 && agg.PriceData[bar].High != 0 && agg.PriceData[bar].Low != 0 && agg.PriceData[bar].Close != 0 {

			ohlcv.Epoch[bar] = agg.PriceData[bar].Timestamp / 1000
			ohlcv.Open[bar] = agg.PriceData[bar].Open
			ohlcv.High[bar] = agg.PriceData[bar].High
			ohlcv.Low[bar] = agg.PriceData[bar].Low
			ohlcv.Close[bar] = agg.PriceData[bar].Close
			ohlcv.HLC[bar] = (agg.PriceData[bar].High + agg.PriceData[bar].Low + agg.PriceData[bar].Close)/3
			ohlcv.Volume[bar] = agg.PriceData[bar].Volume

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
