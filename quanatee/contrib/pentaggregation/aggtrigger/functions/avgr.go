package functions

import "math/big"

func AvgRightFloat32(values []float32) float32 {
	avg := float32(0)
	length := len(values)
	for idx, val := range values {
		avg += (val/float32(length-(idx+1)))
	}
    
	return avg
}

func AvgRightFloat64(values []float64) float64 {
	avg := big.NewFloat(float64(0.0))
	length := len(values)
	for idx, val := range values {
        avg = avg.Add(avg, big.NewFloat(float64((val/float64(length-(idx+1))))))
	}
	
    result, _ := avg.Float64()
    
	return result
}