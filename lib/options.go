package lib

import "fmt"

// ValidatorFctType defines a validator function that returns a validated string and/or an error.
type ValidatorFctType func(val string) (string, error)

// ListOpts holds a list of values and a validation function.
type ListOpts struct {
	values    *[]string
	validator ValidatorFctType
}

// NewListOpts creates a new ListOpts with the specified validator.
func NewListOpts(validator ValidatorFctType) ListOpts {
	var values []string
	return *NewListOptsRef(&values, validator)
}

// NewListOptsRef creates a new ListOpts with the specified values and validator.
func NewListOptsRef(values *[]string, validator ValidatorFctType) *ListOpts {
	return &ListOpts{
		values:    values,
		validator: validator,
	}
}

func (opts *ListOpts) String() string {
	return fmt.Sprintf("%v", []string((*opts.values)))
}

// Set validates if needed the input value and adds it to the
// internal slice.
func (opts *ListOpts) Set(value string) error {
	if opts.validator != nil {
		v, err := opts.validator(value)
		if err != nil {
			return err
		}
		value = v
	}
	(*opts.values) = append((*opts.values), value)
	return nil
}

// Delete removes the specified element from the slice.
func (opts *ListOpts) Delete(key string) {
	for i, k := range *opts.values {
		if k == key {
			(*opts.values) = append((*opts.values)[:i], (*opts.values)[i+1:]...)
			return
		}
	}
}

// GetMap returns the content of values in a map in order to avoid
// duplicates.
func (opts *ListOpts) GetMap() map[string]struct{} {
	ret := make(map[string]struct{})
	for _, k := range *opts.values {
		ret[k] = struct{}{}
	}
	return ret
}

// GetAll returns the values of slice.
func (opts *ListOpts) GetAll() []string {
	return (*opts.values)
}

// GetAllOrEmpty returns the values of the slice
// or an empty slice when there are no values.
func (opts *ListOpts) GetAllOrEmpty() []string {
	v := *opts.values
	if v == nil {
		return make([]string, 0)
	}
	return v
}

// Get checks the existence of the specified key.
func (opts *ListOpts) Get(key string) bool {
	for _, k := range *opts.values {
		if k == key {
			return true
		}
	}
	return false
}

// Len returns the amount of element in the slice.
func (opts *ListOpts) Len() int {
	return len((*opts.values))
}

// Type returns a string name for this Option type
func (opts *ListOpts) Type() string {
	return "list"
}

// WithValidator returns the ListOpts with validator set.
func (opts *ListOpts) WithValidator(validator ValidatorFctType) *ListOpts {
	opts.validator = validator
	return opts
}
