/*
Copyright 2021 The KubeVela Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package api

import (
	"errors"
	"fmt"
	"sync"

	"github.com/go-playground/validator/v10"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

var validate = validator.New()
var (
	validateInitOnce sync.Once
	validateInitErr  error
)

func init() {
	registerValidators()
}

// InitValidator initializes API custom validators and returns initialization errors.
func InitValidator() error {
	registerValidators()
	return validateInitErr
}

func registerValidators() {
	validateInitOnce.Do(func() {
		register := func(tag string, fn validator.Func) {
			if err := validate.RegisterValidation(tag, fn); err != nil {
				validateInitErr = errors.Join(validateInitErr, fmt.Errorf("register validation %q: %w", tag, err))
			}
		}
		register("checkname", ValidateName)
	})
}

// ValidateName custom check name field
func ValidateName(fl validator.FieldLevel) bool {
	return spec.ValidAPIName(fl.Field().String(), datastore.PrimaryKeyMaxLength)
}
