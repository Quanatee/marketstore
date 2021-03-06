package api4polygon

type SplitData struct {
	Symbol 		string 		`json:"ticker"`
	Expiry 		string 		`json:"exDate"`
	Payment	 	string 		`json:"paymentDate"`
	Announce	string		`json:"declaredDate"` // optional
	Ratio		float32		`json:"ratio"`
	ToFactor    float32		`json:"tofactor"` // optional
	ForFactor   float32		`json:"forfactor"` // optional
}

type SplitsItem struct {
	Status 		string 		`json:"status"`
	Count 		int64 		`json:"count"`
	SplitData   []SplitData	`json:"results"`
}

type AggData struct {
	Volume         float32 `json:"v"`
	VolumeWeighted float32 `json:"vw"`
	Open           float32 `json:"o"`
	Close          float32 `json:"c"`
	High           float32 `json:"h"`
	Low            float32 `json:"l"`
	Timestamp      int64   `json:"t"`
	Ticks          int64   `json:"n"`
}

type Agg struct {
	Symbol          string          `json:"ticker"`
	Status          string          `json:"status"`
	Adjusted        bool            `json:"adjusted"`
	QueryCount      int64           `json:"queryCount"`
	ResultsCount    int64           `json:"resultsCount"`
	PriceData       []AggData	    `json:"results"`
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