package api

import "sync"

var (
	// Served by Polygon
	PolygonSplitEvents *sync.Map
	PolygonUpcomingSplitEvents *sync.Map
	PolygonDailyVolumes *sync.Map
	// Served by Tiingo
	TiingoDailyVolumes *sync.Map
)

func InitalizeSharedMaps() {
	PolygonSplitEvents = &sync.Map{}
	PolygonUpcomingSplitEvents = &sync.Map{}
	PolygonDailyVolumes = &sync.Map{}
	TiingoDailyVolumes = &sync.Map{}
}