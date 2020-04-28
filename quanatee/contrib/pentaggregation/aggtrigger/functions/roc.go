package functions

import "math/big"

func RocFloat32(values []float32) float32 {

	length := len(values)
	two_parts := int(length/2)

	avg_of_first_part := float32(0)
	for _, val := range values[:two_parts] {
		avg_of_first_part += val
	}
	avg_of_first_part = avg_of_first_part/float32(len(values[:two_parts]))
	
	avg_of_second_part := float32(0)
	for _, val := range values[length-two_parts:] {
		avg_of_second_part += val
	}
	avg_of_second_part = avg_of_second_part/float32(len(values[length-two_parts:]))
	
	roc := (avg_of_second_part - avg_of_first_part) / avg_of_first_part

	return roc

}

func RocFloat64(values []float64) float64 {

	length := len(values)
	two_parts := int(length/2)

	avg_of_first_part := big.NewFloat(float64(0.0))
	for _, val := range values[:two_parts] {
        avg_of_first_part = avg_of_first_part.Add(avg_of_first_part, big.NewFloat(float64(val)))
	}
	float_avg_of_first_part, _ := avg_of_first_part.Float64()
	float_avg_of_first_part = float_avg_of_first_part/float64(len(values[:two_parts]))
	
	avg_of_second_part := big.NewFloat(float64(0.0))
	for _, val := range values[length-two_parts:] {
        avg_of_second_part = avg_of_second_part.Add(avg_of_second_part, big.NewFloat(float64(val)))
	}
	float_avg_of_second_part, _ := avg_of_second_part.Float64()
	float_avg_of_second_part = float_avg_of_second_part/float64(len(values[length-two_parts:]))
	
	roc := (float_avg_of_second_part - float_avg_of_first_part) / float_avg_of_first_part
	
	return roc
	
}