package session

import (
	"testing"

	"github.com/baetyl/baetyl-go/utils"
	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	// TODO: add more test cases
	testConf1 := ""
	testConf2 := `
principals:
  - username: u1
    password: p1
    permissions:
    - action: sub
      permit: ['$link']
`
	testConf3 := `
session:
  sysTopics:
  - $link
  - $baidu
  maxConnections: 3
  republishInterval: 200ms
  persistence:
  location: testdata
principals:
  - username: c1
  - username: u1
    password: p1
    permissions:
    - action: sub
      permit: [test, talks, talks1, talks2, '$link/a', '#']
    - action: pub
      permit: [test, talks]
`

	var cfg1, cfg2, cfg3 Config
	err := utils.UnmarshalYAML([]byte(testConf1), &cfg1)
	assert.NoError(t, err)
	assert.Len(t, cfg1.Principals, 0)

	err = utils.UnmarshalYAML([]byte(testConf2), &cfg2)
	assert.EqualError(t, err, "Principals: sub topic($link) invalid")

	err = utils.UnmarshalYAML([]byte(testConf3), &cfg3)
	assert.NoError(t, err)
	assert.Len(t, cfg3.Principals, 2)
}
