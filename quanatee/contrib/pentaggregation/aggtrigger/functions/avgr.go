package functions

import "math/big"

func AvgRightFloat32(values []float32) float32 {
	avg := float32(0)
	length := len(values)
	for idx, val := range values {
		avg += (val/(length-(idx+1)))
	}
    
    avg = avg/float32(length)
    
	return avg
}

func AvgRightFloat64(values []float64) float64 {
	avg := big.NewFloat(float64(0.0))
	length := len(values)
	for idx, val := range values {
        avg = avg.Add(avg, big.NewFloat(float64(val)))
        avg = avg.Add(avg, big.NewFloat(float64((val/(length-(idx+1))))))
	}

    avg = new(big.Float).Quo(avg, big.NewFloat(float64(length)))

    result, _ := avg.Float64()
    
	return result
}