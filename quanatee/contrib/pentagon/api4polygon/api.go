package api4polygon

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
	apiKey  	string
	symbolPrefix = map[string]string{
		"crypto": "X:",
		"forex": "C:",
		"stocks": "",
	}
)

func SetAPIKey(key string) {
	apiKey = key
}

func SetMarketType(marketType string) {
	marketType = marketType
}

type ListTickersResponse struct {
	Page    int    `json:"page"`
	PerPage int    `json:"perPage"`
	Count   int    `json:"count"`
	Status  string `json:"status"`
	Tickers []struct {
		Ticker      string `json:"ticker"`
		Name        string `json:"name"`
		Market      string `json:"market"`
		Locale      string `json:"locale"`
		Type        string `json:"type"`
		Currency    string `json:"currency"`
		Active      bool   `json:"active"`
		PrimaryExch string `json:"primaryExch"`
		Updated     string `json:"updated"`
		Codes       struct {
			Cik     string `json:"cik"`
			Figiuid string `json:"figiuid"`
			Scfigi  string `json:"scfigi"`
			Cfigi   string `json:"cfigi"`
			Figi    string `json:"figi"`
		} `json:"codes"`
		URL string `json:"url"`
	} `json:"tickers"`
}

func includeExchange(exchange string) bool {
	// Polygon returns all tickers on all exchanges, which yields over 34k symbols
	// If we leave out OTC markets it will still have over 11k symbols
	if exchange == "CVEM" || exchange == "GREY" || exchange == "OTO" ||
		exchange == "OTC" || exchange == "OTCQB" || exchange == "OTCQ" {
		return false
	}
	return true
}

func ListTickers() (*ListTickersResponse, error) {
	resp := ListTickersResponse{}
	page := 0

	for {
		u, err := url.Parse(fmt.Sprintf(tickersURL, baseURL))
		if err != nil {
			return nil, err
		}

		q := u.Query()
		q.Set("apiKey", apiKey)
		q.Set("sort", "ticker")
		q.Set("perpage", "50")
		q.Set("market", "stocks")
		q.Set("locale", "us")
		q.Set("active", "true")
		q.Set("page", strconv.FormatInt(int64(page), 10))

		u.RawQuery = q.Encode()

		code, body, err := fasthttp.Get(nil, u.String())
		if err != nil {
			return nil, err
		}

		if code >= fasthttp.StatusMultipleChoices {
			return nil, fmt.Errorf("status code %v", code)
		}

		r := &ListTickersResponse{}

		err = json.Unmarshal(body, r)

		if err != nil {
			return nil, err
		}

		if len(r.Tickers) == 0 {
			break
		}

		for _, ticker := range r.Tickers {
			if includeExchange(ticker.PrimaryExch) {
				resp.Tickers = append(resp.Tickers, ticker)
			}
		}

		page++
	}

	log.Info("[polygon] Returning %v symbols\n", len(resp.Tickers))

	return &resp, nil
}

func GetAggregates(
	symbol, marketType, multiplier, resolution string,
	from, to time.Time) (*OHLCV_map, error) {
		
	u, err := url.Parse(fmt.Sprintf(aggURL, baseURL, symbolPrefix[marketType]+symbol, multiplier, resolution, from.Unix()*1000, to.Unix()*1000))
	if err != nil {
		return nil, err
	}
	
	q := u.Query()
	q.Set("apiKey", apiKey)
	q.Set("unadjusted", "true")

	u.RawQuery = q.Encode()

	//agg := &Aggv2{}
	var agg Aggv2
	err = downloadAndUnmarshal(u.String(), retryCount, &agg)
	if err != nil {
		return &OHLCV_map{}, err
	}

	length := len(agg.PriceData)

	if length == 0 {
		return &OHLCV_map{}, nil
	}
	
	ohlcv := &OHLCV_map{
		Open: make(map[int64]float32),
		High: make(map[int64]float32),
		Low: make(map[int64]float32),
		Close: make(map[int64]float32),
		HLC: make(map[int64]float32),
		Volume: make(map[int64]float32),
	}
	// Polygon candle formula (Timestamp on open)
	// Requested at 14:05:01
	// Candle built from 14:04 to 14:05
	// Timestamped at 14:04
    for bar := 0; bar < length; bar++ {
		
		if agg.PriceData[bar].Open != 0 && agg.PriceData[bar].High != 0 && agg.PriceData[bar].Low != 0 && agg.PriceData[bar].Close != 0 {

			Epoch := agg.PriceData[bar].Timestamp / 1000
			ohlcv.Open[Epoch] = agg.PriceData[bar].Open
			ohlcv.High[Epoch] = agg.PriceData[bar].High
			ohlcv.Low[Epoch] = agg.PriceData[bar].Low
			ohlcv.Close[Epoch] = agg.PriceData[bar].Close
			ohlcv.HLC[Epoch] = (agg.PriceData[bar].High + agg.PriceData[bar].Low + agg.PriceData[bar].Close)/3
			ohlcv.Volume[Epoch] = agg.PriceData[bar].Volume

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
