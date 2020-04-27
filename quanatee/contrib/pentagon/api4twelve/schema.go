package api4twelve

type Agg1 struct {
	Date           string  `json:"datetime"` // "2017-12-19T00:00:00"
	Open           float32 `json:"open"`
	Low            float32 `json:"low"`
	High           float32 `json:"high"`
	Close          float32 `json:"close"`
	Volume         float32 `json:"volume"`
}

type Agg2 struct {
	Symbol          string        `json:"symbol"`
	Timeframe       string        `json:"interval"`
	Currency        string        `json:"currency"`
	ExchangeTZ      string        `json:"exchange_timezone"`
	Exchange	    string        `json:"exchange"`
	AssetType	    string	      `json:"type"`
}

type Agg struct {
	PriceData       []Agg1	      `json:"values"`
	MetaData	    []Agg2        `json:"meta"`
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