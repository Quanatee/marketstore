package api4tiingo

type AggCryptoData struct {
	High           float32 `json:"high"`
	Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
	Low            float32 `json:"low"`
	Close          float32 `json:"close"`
	TradesDone     float32 `json:"tradesDone"`
	Open           float32 `json:"open"`
	Volume         float32 `json:"volume"`
	VolumeNotional float32 `json:"volumeNotional"`
}

type AggCrypto struct {
	Ticker        string     	  `json:"ticker"`
	BaseCurrency  string    	  `json:"baseCurrency"`
	QuoteCurrency string  	      `json:"quoteCurrency"`
	PriceData     []AggCryptoData `json:"priceData"`
}

type AggForexData struct {
	Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
	Ticker         string  `json:"ticker"`
	Open           float32 `json:"open"`
	High           float32 `json:"high"`
	Low            float32 `json:"low"`
	Close          float32 `json:"close"`
}

type AggForex struct {
	PriceData	[]AggForexData
}

type AggEquityData struct {
	Date           string  `json:"date"` // "2017-12-19T00:00:00Z"
	Close          float32 `json:"close"`
	High           float32 `json:"high"`
	Low            float32 `json:"low"`
	Open           float32 `json:"open"`
}

type AggEquity struct {
	PriceData	[]AggEquityData
}

type OHLCV struct {
	Open      map[int64]float32
	High      map[int64]float32
	Low       map[int64]float32
	Close     map[int64]float32
	Volume    map[int64]float32
	HLC       map[int64]float32
	TVAL      map[int64]float32
	Spread    map[int64]float32
	Split     map[int64]float32// Unused in Tiingo
}