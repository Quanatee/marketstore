package api

var (
	// Served by Polygon
	SplitEvents *sync.Map
	UpcomingSplitEvents *sync.Map
	PolygonDailyVolumes *sync.Map
	// Served by Tiingo
	TiingoDailyVolumes *sync.Map
)