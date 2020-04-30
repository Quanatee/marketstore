package functions

import "math"
//import "github.com/alpacahq/marketstore/utils/log"

func AvgLeftFloat32(values []float32) float32 {

	avg := AvgFloat32(values)
	std := StdFloat32(values)
	
	var e AvgLeftEWMA
	decay := 2 / (float64(len(values)/2) + 1)
	
	for i := len(values)-1; i >= 0; i-- {
		zsc := math.Abs(float64((values[i]-avg)/std))
		// 95% Confidence Interval
		if zsc <= 1.960 {
			e.Add(float64(values[i]), decay)
		}
	}
	return float32(e.Value())
}

func AvgLeftFloat64(values []float64) float64 {
	
	avg := AvgFloat64(values)
	std := StdFloat64(values)
	
	var e AvgLeftEWMA
	decay := 2 / (float64(len(values)/2) + 1)
	
	for i := len(values)-1; i >= 0; i-- {
		zsc := math.Abs(float64((values[i]-avg)/std))
		// 95% Confidence Interval
		if zsc <= 1.960 {
			e.Add(float64(values[i]), decay)
		}
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