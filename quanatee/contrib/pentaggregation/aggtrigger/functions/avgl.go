package functions

import "math/big"

func AvgLeftFloat32(values []float32) float32 {
	avg := float32(0)
	for idx, val := range values {
		avg += (val/float32(idx+1))
	}
    
	return avg
}

func AvgLeftFloat64(values []float64) float64 {
	avg := big.NewFloat(float64(0.0))
	for idx, val := range values {
        avg = avg.Add(avg, big.NewFloat(float64(val/float64(idx+1))))
	}

    result, _ := avg.Float64()
    
	return result
}