package functions

import "math"

func StdFloat32(values []float32) float32 {

	upcasted_values := convertFloat32ToFloat64(values)
	std := StdDev(upcasted_values, nil)
	return float32(std)

}

func StdFloat64(values []float64) float64 {

	std := StdDev(values, nil)
	return std

}

func convertFloat32ToFloat64(ar []float32) []float64 {
	newar := make([]float64, len(ar))
	var v float32
	var i int
	for i, v = range ar {
	   newar[i] = float64(v)
	}
	return newar
 }
func convertFloat64ToFloat32(ar []float64) []float32 {
	newar := make([]float32, len(ar))
	var v float64
	var i int
	for i, v = range ar {
	   newar[i] = float32(v)
	}
	return newar
 }

// StdDev returns the sample standard deviation.
func StdDev(x, weights []float64) float64 {
	_, std := MeanStdDev(x, weights)
	return std
}

// MeanStdDev returns the sample mean and standard deviation
func MeanStdDev(x, weights []float64) (mean, std float64) {
	mean, variance := MeanVariance(x, weights)
	return mean, math.Sqrt(variance)
}

// Mean computes the weighted mean of the data set.
//  sum_i {w_i * x_i} / sum_i {w_i}
// If weights is nil then all of the weights are 1. If weights is not nil, then
// len(x) must equal len(weights).
func Mean(x, weights []float64) float64 {
	if weights == nil {
		return floats.Sum(x) / float64(len(x))
	}
	if len(x) != len(weights) {
		panic("stat: slice length mismatch")
	}
	var (
		sumValues  float64
		sumWeights float64
	)
	for i, w := range weights {
		sumValues += w * x[i]
		sumWeights += w
	}
	return sumValues / sumWeights
}

// MeanVariance computes the sample mean and variance, where the mean and variance are
//  \sum_i w_i * x_i / (sum_i w_i)
//  \sum_i w_i (x_i - mean)^2 / (sum_i w_i - 1)
// respectively.
// If weights is nil then all of the weights are 1. If weights is not nil, then
// len(x) must equal len(weights).
func MeanVariance(x, weights []float64) (mean, variance float64) {

	// This uses the corrected two-pass algorithm (1.7), from "Algorithms for computing
	// the sample variance: Analysis and recommendations" by Chan, Tony F., Gene H. Golub,
	// and Randall J. LeVeque.

	// note that this will panic if the slice lengths do not match
	mean = Mean(x, weights)
	var (
		ss           float64
		compensation float64
	)
	if weights == nil {
		for _, v := range x {
			d := v - mean
			ss += d * d
			compensation += d
		}
		variance = (ss - compensation*compensation/float64(len(x))) / float64(len(x)-1)
		return
	}

	var sumWeights float64
	for i, v := range x {
		w := weights[i]
		d := v - mean
		wd := w * d
		ss += wd * d
		compensation += wd
		sumWeights += w
	}
	variance = (ss - compensation*compensation/sumWeights) / (sumWeights - 1)
	return
}