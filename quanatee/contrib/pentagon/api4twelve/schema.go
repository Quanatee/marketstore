package api4twelve

type AggOHLC struct {
	Date           string  `json:"datetime"` // "2017-12-19T00:00:00"
	Open           float32 `json:"open,string"`
	High           float32 `json:"high,string"`
	Low            float32 `json:"low,string"`
	Close          float32 `json:"close,string"`
}

type AggOHLCV struct {
	Date           string  `json:"datetime"` // "2017-12-19T00:00:00"
	Open           float32 `json:"open,string"`
	High           float32 `json:"high,string"`
	Low            float32 `json:"low,string"`
	Close          float32 `json:"close,string"`
	Volume         float32 `json:"volume,string"`
}

// Equity
type AggEquityMeta struct {
	Symbol          string        `json:"symbol"`
	Timeframe       string        `json:"interval"`
	Currency        string        `json:"currency"`
	ExchangeTZ      string        `json:"exchange_timezone"`
	Exchange	    string        `json:"exchange"`
	AssetType	    string	      `json:"type"`
}

type AggForexMeta struct {
	Symbol          string        `json:"symbol"`
	Timeframe       string        `json:"interval"`
	CurrencyBase    string        `json:"currency_base"`
	CurrencyQuote   string        `json:"currency_quote"`
	AssetType	    string	      `json:"type"`
}

type AggCryptoMeta struct {
	Symbol          string        `json:"symbol"`
	Timeframe       string        `json:"interval"`
	CurrencyBase    string        `json:"currency_base"`
	CurrencyQuote   string        `json:"currency_quote"`
	Exchange	    string        `json:"exchange"`
	AssetType	    string	      `json:"type"`
}


type AggEquity struct {
	PriceData       []AggOHLCV    		`json:"values"`
	MetaData	    AggEquityMeta	 	`json:"meta"`
	Status			string				`json:"status"`
}

type AggForex struct {
	PriceData       []AggOHLC    		`json:"values"`
	MetaData	    AggForexMeta		`json:"meta"`
	Status			string				`json:"status"`
}

type AggCrypto struct {
	PriceData       []AggOHLC	  		`json:"values"`
	MetaData	    AggCryptoMeta   	`json:"meta"`
	Status			string				`json:"status"`
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
	Split     map[int64]float32
}