package functions

func RocFloat32(values []float32) float32 {
	
	left := AvgLeftFloat32(values)
	right := AvgRightFloat32(values)

	roc := (right - left) / left
	
	return roc

}

func RocFloat64(values []float64) float64 {
	
	left := AvgLeftFloat64(values)
	right := AvgRightFloat64(values)

	roc := (right - left) / left
	
	return roc
	
}
