package functions

func AvgRightFloat32(values []float32) float32 {

	var e SimpleEWMA
	decay := 2 / (float64(len(values)) + 1)
	
	for _, val := range values {
		e.Add(float64(val), decay)
	}
	
	return float32(e.Value())
}

func AvgRightFloat64(values []float64) float64 {
	
	var e SimpleEWMA
	decay := 2 / (float64(len(values)) + 1)
	
	for _, val := range values {
		e.Add(float64(val), decay)
	}
	
	return e.Value()
}

type SimpleEWMA struct {
	// The current value of the average. After adding with Add(), this is
	// updated to reflect the average of all values seen thus far.
	value float64
}

// Add adds a value to the series and updates the moving average.
func (e *SimpleEWMA) Add(value, decay float64) {
	if e.value == 0 { // this is a proxy for "uninitialized"
		e.value = value
	} else {
		e.value = (value * decay) + (e.value * (1 - decay))
	}
}

// Value returns the current value of the moving average.
func (e *SimpleEWMA) Value() float64 {
	return e.value
}

// Set sets the EWMA's value.
func (e *SimpleEWMA) Set(value float64) {
	e.value = value
}
