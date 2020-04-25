package api4polygon

/*
Streaming data
*/
type PolyTrade struct {
	eventType  string  `json:"-"` //ev
	Symbol     string  `json:"sym"`
	exchange   int     `json:"-"` //x
	Price      float64 `json:"p"`
	Size       int64   `json:"s"`
	Timestamp  int64   `json:"t"`
	Conditions []int   `json:"c"`
}

type PolyQuote struct {
	eventType   string  `json:"-"` //ev
	Symbol      string  `json:"sym"`
	bidExchange int     `json:"-"`
	BidPrice    float64 `json:"bp"`
	BidSize     int64   `json:"bs"`
	askExchange int     `json:"-"`
	AskPrice    float64 `json:"ap"`
	AskSize     int64   `json:"as"`
	condition   int     `json:"-"`
	Timestamp   int64   `json:"t"`
}

type PolyAggregate struct {
	eventType    string  `json:"-"` //ev
	Symbol       string  `json:"sym"`
	Volume       int     `json:"v"`
	accumVolume  int     `json:"-"`
	officialOpen float64 `json:"-"`
	vWAP         float64 `json:"-"`
	Open         float64 `json:"o"`
	Close        float64 `json:"c"`
	High         float64 `json:"h"`
	Low          float64 `json:"l"`
	EpochMillis  int64   `json:"s"`
	endTime      int64   `json:"-"`
}

/*
Historical data
*/

type AggData struct {
	Ticker         string  `json:"T"`
	Volume         float32 `json:"v"`
	VolumeWeighted float32 `json:"vw"`
	Open           float32 `json:"o"`
	High           float32 `json:"h"`
	Low            float32 `json:"l"`
	Close          float32 `json:"c"`
	Timestamp      int64   `json:"t"`
	Ticks          int64   `json:"n"`
}

type Aggv2 struct {
	Symbol          string        `json:"ticker"`
	Status          string        `json:"status"`
	Adjusted        bool          `json:"adjusted"`
	queryCount      int64         `json:"queryCount"`
	resultsCount    int64         `json:"resultsCount"`
	PriceData       []AggData     `json:"results"`
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