package api4polygon

type Agg1 struct {
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
	PriceData       []Agg1	      `json:"results"`
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
}