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
	//aggURL     = "%v/v1/historic/agg/%v/%v"
	aggURL     = "%v/v2/aggs/ticker/%v/range/%v/%v/%v/%v"
	tradesURL  = "%v/v2/ticks/stocks/trades/%v/%v"
	quotesURL  = "%v/v1/historic/quotes/%v/%v"
	tickersURL = "%v/v2/reference/tickers"
	retryCount = 10
)
	
var (
	baseURL = "https://api.polygon.io"
	servers = "ws://socket.polygon.io:30328" // default
	apiKey  string
	NY, _   = time.LoadLocation("America/New_York")
)

func SetAPIKey(key string) {
	apiKey = key
}

func SetBaseURL(url string) {
	baseURL = url
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

func GetLiveAggregates(
	symbol, multiplier, resolution string,
	from, to time.Time) (*Aggv2, error) {
	
		u, err := url.Parse(fmt.Sprintf(aggURL, baseURL, symbol, multiplier, resolution, from.AddDate(0, 0, -1).Format(time.RFC3339), to.AddDate(0, 0, 1).Format(time.RFC3339)))
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
		return nil, err
	}
	if agg["resultsCount"] == 0 {
		return nil, nil
	}

	return agg, nil
}

func GetPastAggregates(
	symbol, multiplier, resolution string,
	from, to time.Time) (*Aggv2, error) {
	
		u, err := url.Parse(fmt.Sprintf(aggURL, baseURL, symbol, multiplier, resolution, from.AddDate(0, 0, -1).Format(time.RFC3339), to.AddDate(0, 0, 1).Format(time.RFC3339)))
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
		return NewOHLCV(0), err
	}

	if agg["resultsCount"] == 0 {
		return NewOHLCV(0), nil
	}
	
	ohlcv := NewOHLCV(len(agg.PriceData))

    for bar := 0; bar < len(agg.PriceData); bar++ {

		if agg.PriceData[bar].Open != 0 && agg.PriceData[bar].High != 0 && agg.PriceData[bar].Low != 0 && agg.PriceData[bar].Close != 0 {

			ohlcv.Epoch[bar] = agg.PriceData[bar].Timestamp / 1000
			ohlcv.Open[bar] = agg.PriceData[bar].Open
			ohlcv.High[bar] = agg.PriceData[bar].High
			ohlcv.Low[bar] = agg.PriceData[bar].Low
			ohlcv.Close[bar] = agg.PriceData[bar].Close
			ohlcv.HLC[bar] = (agg.PriceData[bar].High + agg.PriceData[bar].Low + agg.PriceData[bar].Close)/3
			ohlcv.Volume[bar] = agg.PriceData[bar].Volume
			
	return ohlcv, nil
}

// GetHistoricAggregates requests polygon's REST API for historic aggregates
// for the provided resolution based on the provided query parameters.
func GetHistoricAggregates(
	symbol,
	resolution string,
	from, to time.Time,
	limit *int) (*HistoricAggregates, error) {
	// FIXME: Move this to Polygon API v2
	// FIXME: This function does not handle pagination

	u, err := url.Parse(fmt.Sprintf(aggURL, baseURL, resolution, symbol))
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("apiKey", apiKey)

	if !from.IsZero() {
		q.Set("from", from.Format(time.RFC3339))
	}

	if !to.IsZero() {
		q.Set("to", to.Format(time.RFC3339))
	}

	if limit != nil {
		q.Set("limit", strconv.FormatInt(int64(*limit), 10))
	}

	u.RawQuery = q.Encode()

	agg := &HistoricAggregates{}
	err = downloadAndUnmarshal(u.String(), retryCount, agg)
	if err != nil {
		return nil, err
	}

	return agg, nil
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
