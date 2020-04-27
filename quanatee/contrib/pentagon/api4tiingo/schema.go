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
	Open      map[int64]float32
	High      map[int64]float32
	Low       map[int64]float32
	Close     map[int64]float32
	Volume    map[int64]float32
	HLC       map[int64]float32
	Spread    map[int64]float32
	VWAP      map[int64]float32
}