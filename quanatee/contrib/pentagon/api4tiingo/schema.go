package api4tiingo

// Crypto
type AggCryptoData struct {
	TradesDone     float32 `json:"tradesDone"`
	Close          float32 `json:"close"`
	VolumeNotional float32 `json:"volumeNotional"`
	Low            float32 `json:"low"`
	Open           float32 `json:"open"`
	Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
	High           float32 `json:"high"`
	Volume         float32 `json:"volume"`
}

type AggCrypto struct {
	Ticker        string     	  `json:"ticker"`
	BaseCurrency  string    	  `json:"baseCurrency"`
	QuoteCurrency string  	      `json:"quoteCurrency"`
	PriceData     []AggCryptoData	  `json:"priceData"`
}

// Forex and Stocks
type AggData struct {
	Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
	Ticker         string  `json:"ticker"`
	Open           float32 `json:"open"`
	Low            float32 `json:"low"`
	High           float32 `json:"high"`
	Close          float32 `json:"close"`
}

type Agg struct {
	PriceData	[]AggData
}

type OHLCV struct {
	Epoch     []int64     `json:"epoch"`
	Open      []float32   `json:"open"`
	High      []float32   `json:"high"`
	Low       []float32   `json:"low"`
	Close     []float32   `json:"close"`
	HLC       []float32   `json:"HLC"`
	Volume    []float32   `json:"volume"`
}