package main

import (
	"fmt"
	"testing"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/creasty/defaults"
	"github.com/stretchr/testify/assert"
	validator "gopkg.in/validator.v2"
	yaml "gopkg.in/yaml.v2"
)

func TestDefaultsValidator(t *testing.T) {
	type mm struct {
		Min *int `yaml:"Min" default:"1" validate:"min=1"`
		Max int  `yaml:"Max" default:"1" validate:"min=1"`
	}
	type dummy struct {
		T string `yaml:"T" default:"1m" validate:"regexp=^[1-9][0-9]{0\\,5}[smh]?$"`
		M []mm   `yaml:"M"`
	}
	d := new(dummy)
	defaults.Set(d)
	assert.Equal(t, "1m", d.T)
	assert.Nil(t, d.M)
	err := validator.Validate(d)
	assert.NoError(t, err)
	d.T = "1"
	err = validator.Validate(d)
	assert.NoError(t, err)
	d.T = "abc"
	err = validator.Validate(d)
	assert.Error(t, err)

	err = yaml.Unmarshal([]byte(`
T: 60
M:
  - Min: 0
  - Max: 0
  - Min: 2
    Max: 2
`), d)
	assert.NoError(t, err)
	assert.Len(t, d.M, 3)
	for i := range d.M {
		defaults.Set(&d.M[i])
	}
	assert.Equal(t, 1, *d.M[0].Min)
	assert.Equal(t, 1, d.M[0].Max)
	assert.Equal(t, 1, *d.M[1].Min)
	assert.Equal(t, 1, d.M[1].Max)
	assert.Equal(t, 2, *d.M[2].Min)
	assert.Equal(t, 2, d.M[2].Max)

	v := 0
	d.M[0].Min = &v
	err = validator.Validate(d)
	assert.EqualError(t, err, "M[0].Min: less than min")
}

func TestPrincipalsValidate(t *testing.T) {
	c := &config{
		SysTopics: []string{"$baidu", "$link"},
	}
	// round 1: regular principals config validate
	principals := []auth.Principal{{
		Username: "test",
		Password: "hahaha",
		Permissions: []auth.Permission{
			auth.Permission{Action: "pub", Permits: []string{"test", "benchmark", "#", "+", "test/+", "test/#"}},
			auth.Permission{Action: "sub", Permits: []string{"test", "benchmark", "#", "+", "test/+", "test/#"}},
		}}, {
		Username: "temp",
		Password: "3f29e1b2b05f8371595dc761fed8e8b37544b38d56dfce81a551b46c82f2f56b",
		Permissions: []auth.Permission{
			auth.Permission{Action: "pub", Permits: []string{"test", "benchmark", "a/+/b", "+/a/+", "+/a/#"}},
			auth.Permission{Action: "sub", Permits: []string{"test", "benchmark", "a/+/b", "+/a/+", "+/a/#"}},
		}}}
	err := c.principalsValidate(principals, "")
	assert.NoError(t, err)

	// round 2: duplicate username validate
	principals = principals[:len(principals)-1]
	principals = append(principals, auth.Principal{
		Username: "test",
		Password: "3f29e1b2b05f8371595dc761fed8e8b37544b38d56dfce81a551b46c82f2f56b",
		Permissions: []auth.Permission{
			auth.Permission{Action: "pub", Permits: []string{"test", "benchmark"}},
		}})
	err = c.principalsValidate(principals, "")
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Sprintf("username (test) duplicate"), err.Error())

	// round 3: invalid publish topic validate
	principals = principals[:len(principals)-1]
	principals = append(principals, auth.Principal{
		Username: "hello",
		Password: "3f29e1b2b05f8371595dc761fed8e8b37544b38d56dfce81a551b46c82f2f56b",
		Permissions: []auth.Permission{
			auth.Permission{Action: "pub", Permits: []string{"test/a+/b", "benchmark"}},
		}})
	err = c.principalsValidate(principals, "")
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Sprintf("pub topic(test/a+/b) invalid"), err.Error())

	// round 4: invalid subscribe topic validate
	principals = principals[:len(principals)-1]
	principals = append(principals, auth.Principal{
		Username: "hello",
		Password: "3f29e1b2b05f8371595dc761fed8e8b37544b38d56dfce81a551b46c82f2f56b",
		Permissions: []auth.Permission{
			auth.Permission{Action: "pub", Permits: []string{"test", "benchmark"}},
			auth.Permission{Action: "sub", Permits: []string{"test", "test/#/temp"}},
		}})
	err = c.principalsValidate(principals, "")
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Sprintf("sub topic(test/#/temp) invalid"), err.Error())
}
