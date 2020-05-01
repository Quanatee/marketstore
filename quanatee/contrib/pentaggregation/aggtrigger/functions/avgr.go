package functions

import "math"

func AvgRightFloat32(values []float32) float32 {
	if len(values) > 2 {
		avg := AvgFloat32(values)
		std := StdFloat32(values)
		
		var e AvgRightEWMA
		decay := 2 / (float64(len(values)/2) + 1)
		
		for _, val := range values {
			zsc := math.Abs(float64((val-avg)/std))
			// 99.9% Confidence Interval
			if zsc <= 3.291 {
				e.Add(float64(val), decay)
			}
		}
		return float32(e.Value())
	} else {
		return values[len(values)-1]
	}
}

func AvgRightFloat64(values []float64) float64 {
		
	if len(values) > 2 {
		avg := AvgFloat64(values)
		std := StdFloat64(values)
		
		var e AvgRightEWMA
		decay := 2 / (float64(len(values)/2) + 1)
		
		for _, val := range values {
			zsc := math.Abs(float64((val-avg)/std))
			// 99.9% Confidence Interval
			if zsc <= 3.291 {
				e.Add(float64(val), decay)
			}
		}
		return e.Value()
	} else {
		return values[len(values)-1]
	}
}


type AvgRightEWMA struct {
	value float64
}
func (e *AvgRightEWMA) Add(value, decay float64) {
	if e.value == 0 {
		e.value = value
	} else {
		e.value = (value * decay) + (e.value * (1 - decay))
	}
}
func (e *AvgRightEWMA) Value() float64 {
	return e.value
}
func (e *AvgRightEWMA) Set(value float64) {
	e.value = value
}