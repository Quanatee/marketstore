package api4twelve

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
		"crypto": "%v/time_series",
		"forex": "%v/time_series",
		"equity": "%v/time_series",
	}
	baseURL = "https://api.twelvedata.com"
	apiKey 	 string
	length = 0
)

func SetAPIKey(key string) {
	apiKey = key
}

func GetAggregates(
	symbol, marketType, multiplier, resolution string,
	from, to time.Time) (*OHLCV, error) {

	fullURL := fmt.Sprintf(aggURL[marketType], baseURL)
	
	u, err := url.Parse(fullURL)
	
	if err != nil {
		return nil, err
	}
	
	q := u.Query()
	q.Set("apikey", apiKey)
	if strings.Compare(marketType, "equity") != 0 {
		// USD
		if strings.HasPrefix(symbol, "USD") {
			symbol = symbol[:3] + "/" + symbol[3:]
		} else if strings.HasSuffix(symbol, "USD") {
			symbol = symbol[:len(symbol)-3] + "/" + symbol[len(symbol)-3:]
		// BUSD
		} else if strings.HasPrefix(symbol, "BUSD") {
			symbol = symbol[:4] + "/" + symbol[4:]
		} else if strings.HasSuffix(symbol, "BUSD") {
			symbol = symbol[:len(symbol)-4] + "/" + symbol[len(symbol)-4:]
		// USDT
		} else if strings.HasPrefix(symbol, "USDT") {
			symbol = symbol[:4] + "/" + symbol[4:]
		} else if strings.HasSuffix(symbol, "USDT") {
			symbol = symbol[:len(symbol)-4] + "/" + symbol[len(symbol)-4:]
		// USDC
		} else if strings.HasPrefix(symbol, "USDC") {
			symbol = symbol[:4] + "/" + symbol[4:]
		} else if strings.HasSuffix(symbol, "USDC") {
			symbol = symbol[:len(symbol)-4] + "/" + symbol[len(symbol)-4:]
		}
	}
	q.Set("symbol", symbol)
	q.Set("interval", multiplier+resolution)
	q.Set("start_date", from.Format(time.RFC3339))
	q.Set("end_date", to.Format(time.RFC3339))

	u.RawQuery = q.Encode()

	var aggEquity AggEquity
	var aggCurrency AggForex
	var aggCrypto AggCrypto

	if strings.Compare(marketType, "equity") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggEquity)
	} else if strings.Compare(marketType, "currency") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggCurrency)
	} else if strings.Compare(marketType, "crypto") == 0 {
		err = downloadAndUnmarshal(u.String(), retryCount, &aggCrypto)
	}

	if err != nil {
		return &OHLCV{}, err
	}
	
	if strings.Compare(marketType, "equity") == 0 {
		length = len(aggEquity.PriceData)
	} else if strings.Compare(marketType, "currency") == 0 {
		length = len(aggCurrency.PriceData)
	} else if strings.Compare(marketType, "crypto") == 0 {
		length = len(aggCrypto.PriceData)
	}

	if length == 0 {
		log.Info("%s [twelve] returned 0 results between %v and %v", symbol, from, to)
		return &OHLCV{}, nil
	}
	
	ohlcv := &OHLCV{
		Open: make(map[int64]float32),
		High: make(map[int64]float32),
		Low: make(map[int64]float32),
		Close: make(map[int64]float32),
		Volume: make(map[int64]float32),
		HLC: make(map[int64]float32),
		Spread: make(map[int64]float32),
		TVAL: make(map[int64]float32),
	}
	
	// Twelve candle formula (Timestamp on open)
	// Requested at 14:05:01
	// Candle built from 14:04 to 14:05
	// Timestamped at 14:04
    for bar := 0; bar < length; bar++ {
		if strings.Compare(marketType, "equity") == 0 {
			loc, _ := time.LoadLocation(aggEquity.MetaData.ExchangeTZ)
			dt, _ := time.ParseInLocation("2006-01-02 15:04:05", aggEquity.PriceData[bar].Date, loc)
			dt = dt.UTC()
			log.Info("%s [twelve] Data: %v, From: %v, To: %v, Close: %v", symbol, dt, from, to, aggEquity.PriceData[bar].Date)
			if aggEquity.PriceData[bar].Open != 0 && aggEquity.PriceData[bar].High != 0 && aggEquity.PriceData[bar].Low != 0 && aggEquity.PriceData[bar].Close != 0 {
				Epoch := dt.Unix()
				if Epoch >= from.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggEquity.PriceData[bar].Open
					ohlcv.High[Epoch] = aggEquity.PriceData[bar].High
					ohlcv.Low[Epoch] = aggEquity.PriceData[bar].Low
					ohlcv.Close[Epoch] = aggEquity.PriceData[bar].Close
					if aggEquity.PriceData[bar].Volume != 0 {
						ohlcv.Volume[Epoch] = aggEquity.PriceData[bar].Volume
					} else {
						ohlcv.Volume[Epoch] = 1.0
					}
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
				}
			}
		} else if strings.Compare(marketType, "currency") == 0 {
			dt, _ := time.Parse("2006-01-02 15:04:05", aggCurrency.PriceData[bar].Date)
			log.Info("%s [twelve] Data: %v, From: %v, To: %v, Close: %v", symbol, dt, from, to, aggCurrency.PriceData[bar].Date)
			if aggCurrency.PriceData[bar].Open != 0 && aggCurrency.PriceData[bar].High != 0 && aggCurrency.PriceData[bar].Low != 0 && aggCurrency.PriceData[bar].Close != 0 {
				Epoch := dt.Unix()
				if Epoch >= from.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggCurrency.PriceData[bar].Open
					ohlcv.High[Epoch] = aggCurrency.PriceData[bar].High
					ohlcv.Low[Epoch] = aggCurrency.PriceData[bar].Low
					ohlcv.Close[Epoch] = aggCurrency.PriceData[bar].Close
					ohlcv.Volume[Epoch] = 1.0
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
				}
			}
		} else if strings.Compare(marketType, "crypto") == 0 {
			dt, _ := time.Parse("2006-01-02 15:04:05", aggCrypto.PriceData[bar].Date)
			log.Info("%s [twelve] Data: %v, From: %v, To: %v, Close: %v", symbol, dt, from, to, aggCrypto.PriceData[bar].Date)
			if aggCrypto.PriceData[bar].Open != 0 && aggCrypto.PriceData[bar].High != 0 && aggCrypto.PriceData[bar].Low != 0 && aggCrypto.PriceData[bar].Close != 0 {
				Epoch := dt.Unix()
				if Epoch >= from.Unix() {
					// OHLCV
					ohlcv.Open[Epoch] = aggCrypto.PriceData[bar].Open
					ohlcv.High[Epoch] = aggCrypto.PriceData[bar].High
					ohlcv.Low[Epoch] = aggCrypto.PriceData[bar].Low
					ohlcv.Close[Epoch] = aggCrypto.PriceData[bar].Close
					ohlcv.Volume[Epoch] = 1.0
					// Extra
					ohlcv.HLC[Epoch] = (ohlcv.High[Epoch] + ohlcv.Low[Epoch] + ohlcv.Close[Epoch])/3
					ohlcv.Spread[Epoch] = ohlcv.High[Epoch] - ohlcv.Low[Epoch]
					ohlcv.TVAL[Epoch] = ohlcv.HLC[Epoch] * ohlcv.Volume[Epoch]
				}
			}
		}
		
	}

	log.Info("%s [twelve] returned %v results and validated %v results between %v and %v", symbol, length, len(ohlcv.HLC), from, to)

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