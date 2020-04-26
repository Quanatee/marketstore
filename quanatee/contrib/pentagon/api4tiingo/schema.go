package api4tiingo

// Crypto
type cryptoData struct {
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
	Ticker        string      `json:"ticker"`
	BaseCurrency  string      `json:"baseCurrency"`
	QuoteCurrency string      `json:"quoteCurrency"`
	PriceData     []priceData `json:"priceData"`
}

// Forex and Stocks
type Agg struct {
	Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
	Ticker         string  `json:"ticker"`
	Open           float32 `json:"open"`
	Low            float32 `json:"low"`
	High           float32 `json:"high"`
	Close          float32 `json:"close"`
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