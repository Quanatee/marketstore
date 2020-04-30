package functions

func AvgLeftFloat32(values []float32) float32 {

	avg := AvgFloat32(values)
	std := StdFloat32(values)
	
	var e AvgLeftEWMA
	decay := 2 / (float64(len(values)/2) + 1)
	
	for _, val := range values {
		zsc := val-avg/std
		if zsc < 2.576 {
			e.Add(float64(val), decay)
		}
	
	return float32(e.Value())
}

func AvgLeftFloat64(values []float64) float64 {
	
	avg := AvgFloat64(values)
	std := StdFloat64(values)
	
	var e AvgLeftEWMA
	decay := 2 / (float64(len(values)/2) + 1)
	
	zsc := val-avg/std
	if zsc < 2.576 {
		e.Add(float64(val), decay)
	}
	
	return e.Value()
}

type AvgLeftEWMA struct {
	value float64
}
func (e *AvgLeftEWMA) Add(value, decay float64) {
	if e.value == 0 {
		e.value = value
	} else {
		e.value = (value * decay) + (e.value * (1 - decay))
	}
}
func (e *AvgLeftEWMA) Value() float64 {
	return e.value
}
func (e *AvgLeftEWMA) Set(value float64) {
	e.value = value
}