package functions

import "math"

func StdFloat32(values []float32) float32 {

	upcasted_values := convertFloat32ToFloat64(values)
	std, _ := StandardDeviation(upcasted_values)
	
	return float32(std)
}

func StdFloat64(values []float64) float64 {

	std, _ := StandardDeviation(values)

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

// adopted from https://github.com/montanaflynn/stats

type statsError struct {
	err string
}

 var (
    EmptyInputErr = statsErr{"Input must not be empty."}
    NaNErr        = statsErr{"Not a number."}
    NegativeErr   = statsErr{"Must not contain negative values."}
    ZeroErr       = statsErr{"Must not contain zero values."}
    BoundsErr     = statsErr{"Input is outside of range."}
    SizeErr       = statsErr{"Must be the same length."}
    InfValue      = statsErr{"Value is infinite."}
    YCoordErr     = statsErr{"Y Value must be greater than zero."}
)

// StandardDeviation the amount of variation in the dataset
func StandardDeviation(input []float64) (sdev float64, err error) {
	return StandardDeviationPopulation(input)
}

// StandardDeviationPopulation finds the amount of variation from the population
func StandardDeviationPopulation(input []float64) (sdev float64, err error) {

	if len(input) == 0 {
		return math.NaN(), EmptyInputErr
	}

	// Get the population variance
	vp, _ := PopulationVariance(input)

	// Return the population standard deviation
	return math.Pow(vp, 0.5), nil
}


// PopulationVariance finds the amount of variance within a population
func PopulationVariance(input []float64) (pvar float64, err error) {

	v, err := _variance(input, 0)
	if err != nil {
		return math.NaN(), err
	}

	return v, nil
}


// _variance finds the variance for both population and sample data
func _variance(input []float64, sample int) (variance float64, err error) {

	if len(input) == 0 {
		return math.NaN(), EmptyInputErr
	}

	// Sum the square of the mean subtracted from each number
	m, _ := Mean(input)

	for _, n := range input {
		variance += (n - m) * (n - m)
	}

	// When getting the mean of the squared differences
	// "sample" will allow us to know if it's a sample
	// or population and wether to subtract by one or not
	return variance / float64((len(input) - (1 * sample))), nil
}


// Mean gets the average of a slice of numbers
func Mean(input []float64) (float64, error) {

	if len(input) == 0 {
		return math.NaN(), EmptyInputErr
	}

	sum := 0.0
	for _, value := range input {
		sum += value
	}
	
	return sum / float64(len(input)), nil
}