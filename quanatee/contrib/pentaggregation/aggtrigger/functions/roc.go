package functions

import "math/big"

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

func ROCFloat64(values []float64) float64 {
	
	var e ROCEWMA
	decay := 2 / (float64(len(values)) + 1)
	
	for i := len(values)-1; i >= 0; i-- {
		e.Add(float64(values[i]), decay)
	}
	
	return e.Value()
}

type ROCEWMA struct {
	value float64
}
func (e *ROCEWMA) Add(value, decay float64) {
	if e.value == 0 {
		e.value = value
	} else {
		e.value = (value * decay) + (e.value * (1 - decay))
	}
}
func (e *ROCEWMA) Value() float64 {
	return e.value
}
func (e *ROCEWMA) Set(value float64) {
	e.value = value
}