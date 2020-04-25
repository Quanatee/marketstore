package api4polygon

/*
Streaming Schema from Polygon

// Stocks QUOTE:
{
    "ev": "Q",              // Event Type
    "sym": "MSFT",          // Symbol Ticker
    "bx": "4",              // Bix Exchange ID
    "bp": 114.125,          // Bid Price
    "bs": 100,              // Bid Size
    "ax": "7",              // Ask Exchange ID
    "ap": 114.128,          // Ask Price
    "as": 160,              // Ask Size
    "c": 0,                 // Quote Condition
    "t": 1536036818784      // Quote Timestamp ( Unix MS )
}

// Stocks Aggregate:
{
    "ev": "AM",             // Event Type ( A = Second Agg, AM = Minute Agg )
    "sym": "MSFT",          // Symbol Ticker
    "v": 10204,             // Tick Volume
    "av": 200304,           // Accumlated Volume ( Today )
    "op": 114.04,           // Todays official opening price
    "vw": 114.4040,         // VWAP (Volume Weighted Average Price)
    "o": 114.11,            // Tick Open Price
    "c": 114.14,            // Tick Close Price
    "h": 114.19,            // Tick High Price
    "l": 114.09,            // Tick Low Price
    "a": 114.1314,          // Tick Average / VWAP Price
    "s": 1536036818784,     // Tick Start Timestamp ( Unix MS )
    "e": 1536036818784,     // Tick End Timestamp ( Unix MS )
}
*/
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

// HistoricAggregates is the structure that defines
// aggregate data served through polygon's REST API.
type HistoricAggregates struct {
	Symbol        string `json:"symbol"`
	AggregateType string `json:"aggType"`
	Map           struct {
		O string `json:"o"`
		C string `json:"c"`
		H string `json:"h"`
		L string `json:"l"`
		V string `json:"v"`
		D string `json:"d"`
	} `json:"map"`
	Ticks []AggTick `json:"ticks"`
}

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
func NewOHLCV(bars int) OHLCV {
	return OHLCV{
		Epoch:  make([]int64,   bars),
		Open:   make([]float32, bars),
		High:   make([]float32, bars),
		Low:    make([]float32, bars),
		Close:  make([]float32, bars),
		HLC:    make([]float32, bars),
		Volume: make([]float32, bars),
	}
}
