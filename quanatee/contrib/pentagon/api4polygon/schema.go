package api4polygon

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